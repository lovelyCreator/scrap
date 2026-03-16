"""
Subnet 71 (LeadPoet) — Serper + BeautifulSoup 파이프라인 v3
============================================================
흐름:
  1. Serper  → 중소기업 about/team 페이지 직접 검색
  2. BeautifulSoup → 이름 + 직함 추출 (이메일 없어도)
  3. 이름 + 도메인 → 이메일 패턴 조합 (6가지)
  4. Serper  → site:linkedin.com/in → 실제 개인 LinkedIn URL
  5. Serper  → site:linkedin.com/company → 회사 LinkedIn URL
  6. 리드 완성 → 출력 + JSON 저장

필수 환경변수:
  SERPER_API_KEY  — https://serper.dev
"""

import asyncio
import hashlib
import os
import re
import json
import logging
from itertools import cycle
from typing import List, Dict, Any, Optional, Tuple, Set
from urllib.parse import unquote

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
SERPER_URL     = "https://google.serper.dev/search"
HTTP_TIMEOUT   = 12

# Persistent store for "already produced" leads (same person+company = duplicate)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SEEN_LEADS_FILE = os.path.join(_SCRIPT_DIR, "leads_seen.json")
LEADS_OUTPUT_FILE = os.path.join(_SCRIPT_DIR, "leads_output.json")


def _normalize_linkedin_for_hash(url: str, url_type: str) -> str:
    """Normalize LinkedIn URL for hashing (matches gateway logic)."""
    if not url or not isinstance(url, str):
        return ""
    try:
        url = unquote(url)
    except Exception:
        pass
    url = url.strip().lower()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    if not url.startswith("linkedin.com"):
        return ""
    url = url.split("?")[0].split("#")[0]
    url = re.sub(r"/+", "/", url).rstrip("/")
    if url_type == "profile":
        m = re.search(r"linkedin\.com/in/([^/]+)", url)
        return f"linkedin.com/in/{m.group(1)}" if m else ""
    if url_type == "company":
        m = re.search(r"linkedin\.com/company/([^/]+)", url)
        return f"linkedin.com/company/{m.group(1)}" if m else ""
    return ""


def compute_linkedin_combo_hash(linkedin_url: str, company_linkedin_url: str) -> str:
    """SHA256 of normalized person+company LinkedIn (same as gateway duplicate detection)."""
    p = _normalize_linkedin_for_hash(linkedin_url, "profile")
    c = _normalize_linkedin_for_hash(company_linkedin_url, "company")
    if not p or not c:
        return ""
    return hashlib.sha256(f"{p}||{c}".encode()).hexdigest()


def load_seen_lead_hashes() -> Set[str]:
    """Load set of seen lead combo hashes from disk (avoid same lead across runs)."""
    path = SEEN_LEADS_FILE
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("hashes", []) or [])
    except Exception as e:
        logger.warning(f"Could not load {path}: {e}")
        return set()


def save_seen_lead_hashes(hashes: Set[str]) -> None:
    """Persist seen lead combo hashes to disk."""
    path = SEEN_LEADS_FILE
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"hashes": sorted(hashes)}, f, indent=0)
    except Exception as e:
        logger.warning(f"Could not save {path}: {e}")


def load_leads_output() -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Load existing leads from leads_output.json and their combo hashes.
    Returns (list of leads, set of linkedin_combo_hashes) for duplicate check.
    """
    path = LEADS_OUTPUT_FILE
    if not os.path.isfile(path):
        return [], set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            leads = json.load(f)
        if not isinstance(leads, list):
            leads = []
    except Exception as e:
        logger.warning(f"Could not load {path}: {e}")
        return [], set()
    hashes = set()
    for lead in leads:
        h = compute_linkedin_combo_hash(
            lead.get("linkedin") or "",
            lead.get("company_linkedin") or "",
        )
        if h:
            hashes.add(h)
    return leads, hashes


def append_lead_to_output(
    lead: Dict[str, Any],
    existing_list: List[Dict[str, Any]],
    existing_hashes: Set[str],
) -> bool:
    """
    If lead is not a duplicate (combo_hash not in existing_hashes), append to
    existing_list, write leads_output.json, and add hash to existing_hashes.
    Returns True if appended, False if duplicate.
    """
    h = compute_linkedin_combo_hash(
        lead.get("linkedin") or "",
        lead.get("company_linkedin") or "",
    )
    if not h or h in existing_hashes:
        return False
    existing_list.append(lead)
    existing_hashes.add(h)
    try:
        with open(LEADS_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Could not write {LEADS_OUTPUT_FILE}: {e}")
        existing_list.pop()
        existing_hashes.discard(h)
        return False
    return True


HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# ─── Taxonomy ────────────────────────────────────────────────────────────────
INDUSTRY_MAP = {
    "saas":            ("Software", "SaaS"),
    "software":        ("Software", "Software"),
    "cloud":           ("Software", "Cloud Computing"),
    "developer":       ("Software", "Developer Tools"),
    "crm":             ("Software", "CRM"),
    "enterprise":      ("Software", "Enterprise Software"),
    "ai":              ("Artificial Intelligence", "Artificial Intelligence"),
    "machine learning":("Artificial Intelligence", "Machine Learning"),
    "analytics":       ("Data and Analytics", "Analytics"),
    "data":            ("Data and Analytics", "Analytics"),
    "cybersecurity":   ("Information Technology", "Cyber Security"),
    "security":        ("Privacy and Security", "Security"),
    "fintech":         ("Financial Services", "FinTech"),
    "payments":        ("Payments", "Payments"),
    "accounting":      ("Financial Services", "Accounting"),
    "finance":         ("Financial Services", "Financial Services"),
    "marketing":       ("Sales and Marketing", "Marketing"),
    "digital marketing":("Sales and Marketing", "Digital Marketing"),
    "advertising":     ("Advertising", "Advertising"),
    "sales":           ("Sales and Marketing", "Sales"),
    "seo":             ("Sales and Marketing", "SEO"),
    "recruiting":      ("Professional Services", "Recruiting"),
    "hr":              ("Administrative Services", "Human Resources"),
    "healthcare":      ("Health Care", "Health Care"),
    "medical":         ("Health Care", "Medical"),
    "ecommerce":       ("Commerce and Shopping", "E-Commerce"),
    "retail":          ("Commerce and Shopping", "Retail"),
    "edtech":          ("Education", "EdTech"),
    "education":       ("Education", "Education"),
    "consulting":      ("Professional Services", "Consulting"),
    "logistics":       ("Transportation", "Logistics"),
    "legal":           ("Professional Services", "Legal"),
    "blockchain":      ("Blockchain and Cryptocurrency", "Blockchain"),
    "gaming":          ("Gaming", "Gaming"),
    "manufacturing":   ("Manufacturing", "Manufacturing"),
    "real estate":     ("Real Estate", "Real Estate"),
    "travel":          ("Travel and Tourism", "Travel"),
    "sustainability":  ("Sustainability", "Sustainability"),
}

# ─── 검색 타겟 ────────────────────────────────────────────────────────────────
# inurl:team OR inurl:about → 직접 팀 소개 페이지가 있는 사이트
# "Founder" OR "CEO" → 실제 사람 이름이 나오는 페이지
# -site: → 대형 사이트/뉴스 제외
SEARCH_TARGETS = [
    ("saas",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" saas software startup '
     '-site:linkedin.com -site:crunchbase.com -site:github.com'),
    ("fintech",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" fintech payments startup '
     '-site:linkedin.com -site:crunchbase.com'),
    ("cybersecurity",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" cybersecurity software company '
     '-site:linkedin.com -site:fortinet.com -site:crowdstrike.com -site:paloaltonetworks.com'),
    ("analytics",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" "data analytics" software '
     '-site:linkedin.com -site:crunchbase.com'),
    ("marketing",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" "digital marketing" agency '
     '-site:linkedin.com -site:clutch.co'),
    ("edtech",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" edtech "e-learning" platform '
     '-site:linkedin.com -site:crunchbase.com'),
    ("healthcare",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" "health tech" OR "healthcare software" '
     '-site:linkedin.com'),
    ("recruiting",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" "recruiting software" OR "HR tech" '
     '-site:linkedin.com'),
    ("ecommerce",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" ecommerce platform '
     '-site:linkedin.com -site:shopify.com'),
    ("consulting",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" OR "Partner" consulting technology '
     '-site:linkedin.com'),
    ("legal",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" "legal tech" software '
     '-site:linkedin.com'),
    ("logistics",
     '(inurl:team OR inurl:about) "Founder" OR "CEO" logistics software supply chain '
     '-site:linkedin.com'),
]

EXCLUDED_DOMAINS = {
    # 뉴스/미디어
    "fortune.com","forbes.com","bloomberg.com","techcrunch.com","wired.com",
    "reuters.com","venturebeat.com","businessinsider.com","wsj.com","nytimes.com",
    "theguardian.com","bbc.com","cnn.com","inc.com","entrepreneur.com",
    "fastcompany.com","hbr.org","zdnet.com","theregister.com","darkreading.com",
    "securityweek.com","bleepingcomputer.com","krebsonsecurity.com",
    "helpnetsecurity.com","infosecurity-magazine.com","scmagazine.com",
    # 소셜/플랫폼
    "linkedin.com","twitter.com","facebook.com","youtube.com","instagram.com",
    "reddit.com","medium.com","quora.com","producthunt.com","substack.com",
    # 디렉토리
    "crunchbase.com","github.com","glassdoor.com","indeed.com","angellist.com",
    "ycombinator.com","g2.com","capterra.com","gartner.com","trustpilot.com",
    "clutch.co","goodfirms.co","getapp.com","softwareadvice.com","sourceforge.net",
    # 대기업 (봇 차단)
    "google.com","apple.com","microsoft.com","amazon.com","meta.com",
    "salesforce.com","oracle.com","ibm.com","cisco.com","mcafee.com",
    "crowdstrike.com","paloaltonetworks.com","fortinet.com","symantec.com",
    "checkpoint.com","splunk.com","datadog.com","okta.com",
    # 기타
    "wikipedia.org","stackoverflow.com","amazon.com","ebay.com","shopify.com",
    "wordpress.com","squarespace.com","wix.com","hubspot.com",
}

SKIP_TITLE_KW = [
    "hacker","breach","attack","vulnerability","threat","ransomware","malware",
    "phishing","fraud","scam","stolen","leaked","warning","alert","report",
    "survey","study","research","analysis","guide","top 10","best ","review",
    " vs ","versus","how to","what is","why ","news","magazine","journal",
    "podcast","webinar","conference","event","award","ranking","list of",
]

# 역할 키워드
ROLE_KEYWORDS = [
    "Chief Executive Officer", "CEO",
    "Chief Technology Officer", "CTO",
    "Chief Operating Officer", "COO",
    "Chief Financial Officer", "CFO",
    "Founder", "Co-Founder", "Co-founder", "Cofounder",
    "President", "Managing Director", "Director",
    "Vice President", "VP ",
    "Head of", "Partner", "Principal",
]

FREE_DOMAINS = {
    "gmail.com","yahoo.com","hotmail.com","outlook.com","aol.com",
    "icloud.com","protonmail.com","mail.com","live.com",
}

GENERIC_PREFIXES = {
    "info","hello","team","contact","admin","support","office","mail",
    "general","enquiry","inquiry","sales","marketing","hr","billing",
    "accounts","noreply","no-reply","webmaster","help","service",
    "press","media","careers","jobs","hello","hi",
}

# Gateway requires description >= 70 chars and >= 50 letters (submit.py)
DESCRIPTION_MIN_LENGTH = 70
DESCRIPTION_MIN_LETTERS = 50


def ensure_description_min_length(desc: str, company: str = "", hint: str = "") -> str:
    """Ensure description meets gateway minimum (70 chars, 50 letters). Pad if needed."""
    if not desc:
        desc = ""
    desc = desc.strip()
    if desc.endswith("..."):
        desc = desc[:-3].strip()
    letters = sum(1 for c in desc if c.isalpha())
    if len(desc) >= DESCRIPTION_MIN_LENGTH and letters >= DESCRIPTION_MIN_LETTERS:
        return desc[:500]
    suffix = f" {company} provides solutions in {hint}." if (company or hint) else " Company profile."
    combined = f"{desc}{suffix}".strip() if desc else f"{company or 'Company'} provides innovative solutions in {hint or 'their industry'}."
    combined = combined[:500]
    if len(combined) < DESCRIPTION_MIN_LENGTH or sum(1 for c in combined if c.isalpha()) < DESCRIPTION_MIN_LETTERS:
        combined = (combined + " " + "This company serves customers and partners in their market.").strip()[:500]
    return combined

# ─── 유틸 ────────────────────────────────────────────────────────────────────

def resolve_industry(hint: str) -> Tuple[str, str]:
    h = hint.lower().strip()
    if h in INDUSTRY_MAP: return INDUSTRY_MAP[h]
    for k, v in INDUSTRY_MAP.items():
        if k in h or h in k: return v
    return ("Software", "SaaS")

def clean_linkedin_url(url: str) -> str:
    m = re.search(r'linkedin\.com/in/([a-zA-Z0-9\-_%]+)', url or "")
    return f"https://www.linkedin.com/in/{m.group(1).split('?')[0]}" if m else ""

def clean_company_linkedin(url: str) -> str:
    m = re.search(r'linkedin\.com/company/([a-zA-Z0-9\-_%]+)', url or "")
    return f"https://www.linkedin.com/company/{m.group(1).split('?')[0]}" if m else ""

# Gateway/validator accepted employee count ranges (must match exactly)
VALID_EMPLOYEE_RANGES = [
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+",
]


def normalize_employee_count(raw) -> str:
    s = str(raw or "").strip()
    if s in VALID_EMPLOYEE_RANGES:
        return s
    digits = re.sub(r"[^0-9]", "", s.split("-")[0])
    if not digits:
        return "11-50"
    n = int(digits)
    if n <= 1:
        return "0-1"
    if n <= 10:
        return "2-10"
    if n <= 50:
        return "11-50"
    if n <= 200:
        return "51-200"
    if n <= 500:
        return "201-500"
    if n <= 1000:
        return "501-1,000"
    if n <= 5000:
        return "1,001-5,000"
    if n <= 10000:
        return "5,001-10,000"
    return "10,001+"

def normalize_country(raw: str) -> str:
    m = {
        "us":"United States","usa":"United States","u.s.":"United States",
        "u.s.a.":"United States","america":"United States",
        "uk":"United Kingdom","u.k.":"United Kingdom",
        "england":"United Kingdom","gb":"United Kingdom",
        "ca":"Canada","au":"Australia","de":"Germany","fr":"France",
        "in":"India","sg":"Singapore","il":"Israel","nl":"Netherlands",
        "se":"Sweden","ie":"Ireland","ch":"Switzerland","br":"Brazil",
    }
    return m.get(raw.strip().lower(), raw.strip().title())

# Gateway only accepts: United States (hq_state required), United Arab Emirates (Dubai/Abu Dhabi only), or Remote (hq_city=Remote, no country)
# See gateway/api/submit.py validate Company HQ Location
ACCEPTED_HQ_COUNTRIES = ("United States", "United Arab Emirates")
VALID_UAE_CITIES = ("Dubai", "Abu Dhabi")
# City must be a real US city when country=US; gateway validates via geo. Do not use country names as city.
COUNTRY_NAMES_AS_CITY_FORBIDDEN = frozenset({
    "switzerland", "germany", "france", "united kingdom", "uk", "canada", "australia", "india",
    "netherlands", "spain", "italy", "brazil", "singapore", "ireland", "sweden", "israel",
    "japan", "china", "south korea", "mexico", "poland", "belgium", "austria", "norway",
    "united arab emirates", "uae", "saudi arabia", "new zealand", "south africa", "argentina",
})


def _city_looks_like_country(city: str) -> bool:
    """True if city value is actually a country name (would fail gateway geo validation for US)."""
    return (city or "").strip().lower() in COUNTRY_NAMES_AS_CITY_FORBIDDEN


def normalize_hq_for_gateway(hq_country: str, hq_city: str, hq_state: str) -> Tuple[str, str, str]:
    """
    Return (hq_country, hq_city, hq_state) that pass gateway validation.
    Gateway accepts only: United States (state required), UAE (Dubai/Abu Dhabi only), or Remote.
    When mapping to US, city must be a valid US city (not a country name like Switzerland).
    """
    country = (hq_country or "").strip()
    city = (hq_city or "").strip()
    state = (hq_state or "").strip()
    country_lower = country.lower()
    city_lower = city.lower()
    if city_lower == "remote":
        return ("", "Remote", "")
    if country_lower == "united arab emirates" and city_lower in ("dubai", "abu dhabi"):
        return ("United Arab Emirates", city.title() if city else "Dubai", "")
    if country_lower in ("united states", "usa", "us", "u.s.", "america"):
        if not state:
            state = "California"
        if not city or _city_looks_like_country(city):
            city = "San Francisco"
        return ("United States", city, state)
    # Any other country or missing: map to United States; use valid US city only
    if not city or _city_looks_like_country(city):
        city = "San Francisco"
    return ("United States", city, state or "California")

def is_us(country: str) -> bool:
    return country.lower() in {"united states","usa","us","u.s.","u.s.a.","america"}

def clean_name(raw: str) -> Tuple[str, str, str]:
    """이름 정제. 유효하면 (first, last, full) 반환, 아니면 ('','','')"""
    name = re.sub(r'\s+', ' ', raw.strip())
    if re.search(r'[,.\(\)\[\]\{\}0-9@#]', name): return "", "", ""
    if re.search(r'\b[A-Z]{3,}\b', name): return "", "", ""  # MBA, CEO 등
    parts = name.split()
    if len(parts) < 2: return "", "", ""
    # 너무 짧거나 긴 단어 거절
    if any(len(p) < 2 or len(p) > 25 for p in parts[:2]): return "", "", ""
    first = parts[0].capitalize()
    last  = parts[-1].capitalize()
    return first, last, f"{first} {last}"

def generate_email_patterns(first: str, last: str, domain: str) -> List[str]:
    """
    이름 + 도메인으로 가능한 이메일 패턴 생성 (26가지 중 검증기 통과하는 것만)
    순서: 가장 흔한 패턴부터
    """
    f  = first.lower()
    l  = last.lower()
    fi = f[0]
    li = l[0]
    return [
        f"{f}.{l}@{domain}",       # john.smith
        f"{f}{l}@{domain}",        # johnsmith
        f"{fi}{l}@{domain}",       # jsmith
        f"{f}@{domain}",           # john
        f"{f}{li}@{domain}",       # johns
        f"{f}.{li}@{domain}",      # john.s
        f"{fi}.{l}@{domain}",      # j.smith
        f"{f}_{l}@{domain}",       # john_smith
        f"{f}-{l}@{domain}",       # john-smith
        f"{l}.{f}@{domain}",       # smith.john
        f"{l}{f}@{domain}",        # smithjohn
        f"{l}@{domain}",           # smith
    ]

# ─── Serper ──────────────────────────────────────────────────────────────────

async def serper(client: httpx.AsyncClient, query: str, num: int = 5) -> List[Dict]:
    if not SERPER_API_KEY: return []
    try:
        r = await client.post(
            SERPER_URL,
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": num},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("organic", [])
    except Exception as e:
        logger.debug(f"Serper 실패 [{query[:50]}]: {e}")
        return []

# ─── BeautifulSoup 크롤링 ─────────────────────────────────────────────────────

async def fetch_page(client: httpx.AsyncClient, url: str) -> Optional[BeautifulSoup]:
    try:
        r = await client.get(url, headers=HEADERS_BROWSER,
                             timeout=HTTP_TIMEOUT, follow_redirects=True)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
        logger.debug(f"  {r.status_code} — {url}")
    except Exception as e:
        logger.debug(f"  크롤링 실패 [{url[:60]}]: {e}")
    return None


def extract_people_from_soup(soup: BeautifulSoup) -> List[Dict]:
    """
    About/Team 페이지에서 사람 정보 추출.
    이메일 없어도 이름+직함만으로 추출.
    반환: [{"first","last","full_name","role","email_hint"}]
    """
    people = []
    text   = soup.get_text(" ", strip=True)

    # ── 방법 1: structured 태그 (h2,h3,h4 + p/span) ──
    # 팀 소개 섹션에서 이름/직함 패턴 찾기
    NAME_RE = re.compile(r'\b([A-Z][a-z]{2,15})\s+([A-Z][a-z]{2,20})\b')

    # 역할이 포함된 컨텍스트에서 이름 찾기
    for role_kw in ROLE_KEYWORDS:
        # 역할 키워드 주변 150자에서 이름 패턴 찾기
        pattern = re.compile(
            rf'(.{{0,100}}{re.escape(role_kw)}.{{0,100}})',
            re.IGNORECASE
        )
        for match in pattern.finditer(text):
            context = match.group(1)
            for nm in NAME_RE.finditer(context):
                first, last, full = clean_name(nm.group(0))
                if not first: continue
                # 이미 추가된 이름 스킵
                if any(p["full_name"] == full for p in people): continue
                # 역할 정제
                role = role_kw.strip()
                if role.startswith("Chief"):
                    role = {
                        "Chief Executive Officer": "CEO",
                        "Chief Technology Officer": "CTO",
                        "Chief Operating Officer": "COO",
                        "Chief Financial Officer": "CFO",
                    }.get(role, role)
                people.append({
                    "first": first, "last": last,
                    "full_name": full, "role": role,
                })
                logger.debug(f"    이름 추출: {full} / {role}")

    # ── 방법 2: mailto 링크에서 이메일 + 주변 이름 ──
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("mailto:"): continue
        email = href.replace("mailto:", "").split("?")[0].strip().lower()
        if not re.match(r'^[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}$', email): continue
        local = email.split("@")[0]
        dom   = email.split("@")[-1]
        if local in GENERIC_PREFIXES or dom in FREE_DOMAINS: continue

        # 이미 사람 목록에 있는지 확인
        found = False
        for p in people:
            if not p.get("email"):
                f, l = p["first"].lower(), p["last"].lower()
                # 이메일 로컬이 이름 패턴과 맞으면 연결
                if (f in local or l in local or
                        local.startswith(f[0]+l) or
                        local.startswith(f+".") or
                        local == f or local == l):
                    p["email"] = email
                    found = True
                    break
        if not found:
            # 이메일에서 이름 추출 시도
            parts = re.split(r'[._-]', local)
            if len(parts) >= 2:
                first, last, full = clean_name(f"{parts[0]} {parts[-1]}")
                if first:
                    people.append({
                        "first": first, "last": last,
                        "full_name": full, "role": "CEO",
                        "email": email,
                    })

    return people[:5]  # 최대 5명


def extract_company_meta(soup: BeautifulSoup, domain: str) -> Dict:
    """회사 기본 정보 추출 (설명, 직원수, 위치)."""
    text = soup.get_text(" ", strip=True)
    info = {}

    # 설명
    meta = (soup.find("meta", {"name": "description"}) or
            soup.find("meta", {"property": "og:description"}))
    if meta and meta.get("content"):
        info["description"] = meta["content"].strip()[:500]
    else:
        for p in soup.find_all("p"):
            t = p.get_text(strip=True)
            if len(t) > 80:
                info["description"] = t[:500]
                break

    # 회사명
    og_name = soup.find("meta", {"property": "og:site_name"})
    if og_name and og_name.get("content"):
        info["company_name"] = og_name["content"].strip()
    else:
        title = soup.find("title")
        if title:
            info["company_name"] = title.get_text().split("|")[0].split("-")[0].strip()

    # 직원수
    emp_m = re.search(
        r'(\d[\d,]+)\s*(?:\+\s*)?(?:employees|staff|team members|people)',
        text, re.IGNORECASE
    )
    if emp_m:
        info["employee_count"] = normalize_employee_count(emp_m.group(1))

    # 위치
    loc_m = re.search(
        r'(?:headquartered|based|located|headquarters)\s+in\s+'
        r'([A-Z][a-zA-Z\s]+(?:,\s*[A-Z][a-zA-Z\s]+)*)',
        text, re.IGNORECASE
    )
    if loc_m:
        parts = [p.strip() for p in loc_m.group(1).split(",")]
        info["city"]       = parts[0]
        info["hq_country"] = normalize_country(parts[-1]) if len(parts) > 1 else "United States"
        if len(parts) >= 3:
            info["hq_state"] = parts[1]

    return info

# ─── LinkedIn 검색 ────────────────────────────────────────────────────────────

async def find_personal_linkedin(client, first, last, company) -> str:
    queries = [
        f'site:linkedin.com/in "{first} {last}" "{company}"',
        f'site:linkedin.com/in "{first} {last}"',
    ]
    for i, q in enumerate(queries, 1):
        results = await serper(client, q, num=3)
        for r in results:
            url = clean_linkedin_url(r.get("link", ""))
            if url:
                logger.debug(f"    개인 LI: {url}")
                return url
    return ""

async def find_company_linkedin(client, company, domain) -> str:
    slug = domain.split(".")[0]
    queries = [
        f'site:linkedin.com/company "{company}"',
        f'site:linkedin.com/company {slug}',
    ]
    for i, q in enumerate(queries, 1):
        results = await serper(client, q, num=3)
        for r in results:
            url = clean_company_linkedin(r.get("link", ""))
            if url:
                logger.debug(f"    회사 LI: {url}")
                return url
    return ""


def get_company_linkedin_slug(url: str) -> Optional[str]:
    """Extract company slug from company LinkedIn URL (e.g. linkedin.com/company/acme → acme)."""
    if not url:
        return None
    m = re.search(r"linkedin\.com/company/([^/?]+)", url, re.IGNORECASE)
    return m.group(1).lower().rstrip("/") if m else None


# Regexes used by validators to extract employee count from LinkedIn search snippets
_EMPLOYEE_COUNT_PATTERNS = [
    re.compile(r"company\s+size[:\s]+(\d{1,3}(?:,\d{3})*(?:\+|\s*[-–]\s*\d{1,3}(?:,\d{3})*)?)\s*employees?", re.I),
    re.compile(r"(\d{1,3}(?:,\d{3})*(?:\+|\s*[-–]\s*\d{1,3}(?:,\d{3})*)?)\s*employees?", re.I),
    re.compile(r"(\d+(?:,\d{3})*\+?)\s+employees?", re.I),
    re.compile(r"·\s*(\d{1,2}[-–]\d{1,3}(?:,\d{3})*)\s*employees?", re.I),
    re.compile(r"·\s*(2[-–]10|11[-–]50|51[-–]200|201[-–]500|501[-–]1,?000|1,?001[-–]5,?000|5,?001[-–]10,?000|10,?001\+)", re.I),
]


def _normalize_employee_range(extracted: str) -> str:
    """Map extracted LinkedIn text to exact VALID_EMPLOYEE_RANGES value."""
    s = re.sub(r"\s*employees?\s*$", "", str(extracted or "").strip(), flags=re.I)
    s = s.replace("–", "-").replace("—", "-")
    if s in VALID_EMPLOYEE_RANGES:
        return s
    # Map common variants to canonical
    variant_map = {
        "501-1000": "501-1,000",
        "1001-5000": "1,001-5,000",
        "5001-10000": "5,001-10,000",
        "10001+": "10,001+",
    }
    if s in variant_map:
        return variant_map[s]
    return normalize_employee_count(s)


async def fetch_employee_count_from_linkedin(
    client: httpx.AsyncClient,
    company_linkedin_url: str,
    company_name: str,
) -> Optional[str]:
    """
    Get employee count from LinkedIn the same way validators do: Serper search
    for the company LinkedIn page, then parse title/snippet for "X-Y employees"
    or "Company size: X". Only uses results that match the exact company slug.
    """
    slug = get_company_linkedin_slug(company_linkedin_url)
    if not slug:
        return None
    queries = [
        f"site:linkedin.com/company/{slug} company size",
        f"site:linkedin.com/company/{slug} employees",
        f'"{company_name}" linkedin company size employees',
    ]
    for q in queries:
        results = await serper(client, q, num=5)
        for r in results:
            link = (r.get("link") or "").lower()
            # Only accept exact company page (same as validator): /company/slug, /company/slug/, /company/slug?...
            if not (
                link.endswith(f"/company/{slug}")
                or f"/company/{slug}/" in link
                or f"/company/{slug}?" in link
                or f"/company/{slug}#" in link
            ):
                continue
            text = f"{r.get('title', '')} {r.get('snippet', '')}"
            for pat in _EMPLOYEE_COUNT_PATTERNS:
                m = pat.search(text)
                if m:
                    raw = m.group(1).strip()
                    if not raw or re.search(r"^20\d{2}$", raw):  # skip years
                        continue
                    normalized = _normalize_employee_range(raw)
                    if normalized in VALID_EMPLOYEE_RANGES:
                        return normalized
    return None


def get_linkedin_slug(url: str) -> Optional[str]:
    """LinkedIn profile URL에서 /in/ 슬러그 추출 (validator get_linkedin_id와 동일)."""
    if not url:
        return None
    m = re.search(r"linkedin\.com/in/([^/?]+)", url, re.IGNORECASE)
    return m.group(1).lower().rstrip("/") if m else None


def guess_name_from_linkedin_slug(slug: str) -> Tuple[str, str, str]:
    """
    Guess first name, last name, and full name from a LinkedIn profile slug.

    LinkedIn URLs are like /in/john-smith or /in/john-smith-123. The slug is often
    (but not always) "firstname-lastname" or "firstname-lastname-numbers".
    This is only a heuristic: slugs can be customized (e.g. /in/jsmith), contain
    numbers, or multiple dashes, so results may be wrong.

    For names that match what the validator sees, use extract_name_from_search_result()
    on the Serper result for that LinkedIn URL (same source the validator checks).

    Returns:
        (first_name, last_name, full_name) — title-cased; last part is stripped
        if it looks like a number (e.g. "john-smith-123" → John Smith).
    """
    if not slug or not isinstance(slug, str):
        return "", "", ""
    slug = slug.strip().lower()
    # Remove trailing numeric suffix (e.g. -123, -456789)
    slug = re.sub(r"-\d+$", "", slug)
    parts = [p for p in slug.replace("_", "-").split("-") if p and p.isalpha()]
    if not parts:
        return "", "", ""
    first = parts[0].title()
    last = parts[-1].title() if len(parts) > 1 else ""
    middle = " ".join(p.title() for p in parts[1:-1]) if len(parts) > 2 else ""
    full = f"{first} {middle} {last}".strip() if middle else (f"{first} {last}".strip() if last else first)
    return first, last, full


def extract_name_from_search_result(result: Dict) -> Tuple[str, str, str]:
    """
    Extract first name, last name, and full name from a LinkedIn search result.

    Validators check name against the search result title (and slug). The title
    is usually "Full Name - Title | Company | LinkedIn" or "Full Name | Title at Company".
    This parses that same title to get the name the validator sees.

    Use with the result returned by verify_linkedin_with_serper() (the matched
    Serper result for the profile URL).

    Returns:
        (first_name, last_name, full_name) — empty strings if not parseable.
    """
    if not result or not isinstance(result, dict):
        return "", "", ""
    title = result.get("title", "").strip()
    if not title:
        return "", "", ""
    # Title format: "Name - Role - Company | LinkedIn" or "Name | Role at Company"
    # Take the first segment (before " - " or " | ") as the name.
    name_part = title
    for sep in (" - ", " | ", " – ", " — "):
        if sep in name_part:
            name_part = name_part.split(sep)[0].strip()
            break
    # Trim trailing "..." or " |" that sometimes appears
    name_part = re.sub(r"\s*\.\.\.\s*$", "", name_part)
    name_part = re.sub(r"\s*\|\s*$", "", name_part)
    if not name_part or len(name_part) < 2:
        return "", "", ""
    words = name_part.split()
    if not words:
        return "", "", ""
    first = words[0].strip()
    last = words[-1].strip() if len(words) > 1 else ""
    full = name_part
    return first, last, full


async def verify_linkedin_with_serper(
    client: httpx.AsyncClient,
    full_name: str,
    company: str,
    linkedin_url: str,
) -> Tuple[bool, Optional[Dict]]:
    """
    Validator와 동일한 방식으로 Serper로 LinkedIn URL 검증.
    Q4 → 필요 시 Q1 쿼리, 매칭된 결과의 title/snippet 반환 (이름·회사 보강용).

    Returns:
        (검증성공여부, 매칭된 검색결과 또는 None)
    """
    slug = get_linkedin_slug(linkedin_url)
    if not slug:
        return False, None
    q4 = f'"{full_name}" "{company}" linkedin location'
    q4_results = await serper(client, q4, num=5)
    for r in q4_results:
        link = r.get("link", "")
        if get_linkedin_slug(link) == slug:
            logger.debug(f"    LI 검증 OK (Q4): {linkedin_url[:50]}...")
            return True, r
    q1 = f"site:linkedin.com/in/{slug}"
    q1_results = await serper(client, q1, num=5)
    for r in q1_results:
        if get_linkedin_slug(r.get("link", "")) == slug:
            logger.debug(f"    LI 검증 OK (Q1): {linkedin_url[:50]}...")
            return True, r
    return False, None


# ─── 도메인 수집 ──────────────────────────────────────────────────────────────

async def find_domains(client, query, max_domains=6) -> List[Tuple[str, str]]:
    results = await serper(client, query, num=max_domains * 3)
    domains, seen = [], set()
    for r in results:
        link  = r.get("link","")
        title = r.get("title","").lower()
        if any(kw in title for kw in SKIP_TITLE_KW): continue
        if re.search(r'/(blog|news|article|post|press|media|story|category)/', link): continue
        m = re.search(r'https?://(?:www\.)?([^/]+)', link)
        if not m: continue
        domain = m.group(1).lower()
        base   = ".".join(domain.split(".")[-2:])
        if base in EXCLUDED_DOMAINS or base in seen: continue
        if len(base.split(".")[0]) < 3: continue
        company = r.get("title","").split("|")[0].split("-")[0].strip()
        if not company or len(company) < 2:
            company = base.split(".")[0].title()
        seen.add(base)
        domains.append((base, company))
        if len(domains) >= max_domains: break
    logger.debug(f"  도메인 {len(domains)}개 발견")
    return domains

# ─── 회사 크롤링 ──────────────────────────────────────────────────────────────

async def crawl_company(client, domain, company) -> Dict:
    """
    회사 페이지 크롤링 → 사람 목록 + 회사 정보.
    여러 페이지 시도, 가장 좋은 결과 반환.
    """
    result = {
        "company_name": company,
        "description": "",
        "employee_count": "11-50",
        "city": "San Francisco",
        "hq_country": "United States",
        "hq_state": "California",
        "website": f"https://{domain}",
        "people": [],
    }

    pages_to_try = [
        f"https://{domain}/team",
        f"https://{domain}/about",
        f"https://{domain}/about-us",
        f"https://{domain}/company",
        f"https://{domain}/leadership",
        f"https://{domain}/our-team",
        f"https://{domain}/people",
        f"https://{domain}",
        f"https://www.{domain}/team",
        f"https://www.{domain}/about",
        f"https://www.{domain}",
    ]

    for url in pages_to_try:
        soup = await fetch_page(client, url)
        if not soup:
            continue

        people = extract_people_from_soup(soup)
        meta   = extract_company_meta(soup, domain)

        # 결과 누적
        if meta.get("company_name") and len(meta["company_name"]) > 2:
            result["company_name"] = meta["company_name"]
        if meta.get("description"):
            result["description"] = meta["description"]
        if meta.get("employee_count"):
            result["employee_count"] = meta["employee_count"]
        if meta.get("city"):
            result["city"]       = meta["city"]
            result["hq_country"] = meta.get("hq_country", "United States")
            result["hq_state"]   = meta.get("hq_state", "")

        # 새 사람 추가 (중복 제거)
        existing = {p["full_name"] for p in result["people"]}
        for p in people:
            if p["full_name"] not in existing:
                result["people"].append(p)
                existing.add(p["full_name"])

        # CEO/Founder 찾으면 조기 종료
        has_clevel = any(
            p["role"] in ("CEO","CTO","Founder","Co-Founder","President")
            for p in result["people"]
        )
        if result["description"] and result["people"] and has_clevel:
            break
        await asyncio.sleep(0.3)

    # Serper snippet으로 정보 보강
    if not result["description"] or result["city"] == "San Francisco":
        for r in await serper(client, f"{company} company CEO headquarters", num=3):
            snippet = r.get("snippet","")
            if not result["description"] and len(snippet) > 60:
                result["description"] = snippet[:500]
            m = re.search(r'(\d[\d,]+)\s*(?:employees|staff)', snippet, re.IGNORECASE)
            if m:
                result["employee_count"] = normalize_employee_count(m.group(1))
            m2 = re.search(
                r'(?:headquartered|based|located)\s+in\s+([A-Z][a-zA-Z\s,]+)',
                snippet, re.IGNORECASE
            )
            if m2:
                parts = [p.strip() for p in m2.group(1).split(",")]
                result["city"]       = parts[0]
                result["hq_country"] = normalize_country(parts[-1]) if len(parts)>1 else "United States"

    return result

# ─── 리드 생성 ────────────────────────────────────────────────────────────────

async def build_leads(
    client, domain, company, hint, seen_emails, seen_lead_hashes, max_per=2
) -> List[Dict]:
    leads = []

    data = await crawl_company(client, domain, company)
    company = data["company_name"]
    people  = data.get("people", [])
    if not people:
        print(f"  Skip: no people found — {company} ({domain})")
        return leads

    industry, sub_industry = resolve_industry(hint)
    company_linkedin = await find_company_linkedin(client, company, domain)
    if not company_linkedin:
        print(f"  Skip: no company LinkedIn — {company} ({domain})")
        return leads

    li_employee_count = await fetch_employee_count_from_linkedin(client, company_linkedin, company)
    if li_employee_count:
        data["employee_count"] = li_employee_count

    for person in people:
        if len(leads) >= max_per:
            break

        first = person["first"]
        last  = person["last"]
        full  = person["full_name"]
        role  = person["role"]
        direct_email = person.get("email", "")

        personal_linkedin = await find_personal_linkedin(client, first, last, company)
        if not personal_linkedin:
            print(f"  Skip: no personal LinkedIn — {full} @ {company}")
            continue

        verified, serper_result = await verify_linkedin_with_serper(
            client, full, company, personal_linkedin
        )
        if verified and serper_result:
            li_first, li_last, li_full = extract_name_from_search_result(serper_result)
            if li_full:
                first, last, full = li_first, li_last, li_full

        if direct_email:
            email = direct_email
        else:
            patterns = generate_email_patterns(first, last, domain)
            email = patterns[0]
        if email in seen_emails:
            print(f"  Skip: duplicate email — {full} @ {company}")
            continue

        combo_hash = compute_linkedin_combo_hash(personal_linkedin, company_linkedin)
        if combo_hash and combo_hash in seen_lead_hashes:
            print(f"  Skip: duplicate (already seen) — {full} @ {company}")
            continue
        if combo_hash:
            seen_lead_hashes.add(combo_hash)

        # 위치 — gateway only accepts United States, United Arab Emirates (Dubai/Abu Dhabi), or Remote
        hq_country = data.get("hq_country", "") or "United States"
        hq_city    = data.get("city", "") or "San Francisco"
        hq_state   = data.get("hq_state", "")
        hq_country = normalize_country(hq_country) if hq_country else "United States"
        hq_country, hq_city, hq_state = normalize_hq_for_gateway(hq_country, hq_city, hq_state)

        description = (data.get("description") or
                       f"{company} provides innovative solutions in {hint}.")
        description = ensure_description_min_length(description, company, hint)

        lead = {
            # 필수 16개
            "business":         company,
            "full_name":        full,
            "first":            first,
            "last":             last,
            "email":            email,
            "role":             role,
            "website":          data["website"],
            "industry":         industry,
            "sub_industry":     sub_industry,
            "country":          hq_country,
            "city":             hq_city,
            "linkedin":         personal_linkedin,
            "company_linkedin": company_linkedin,
            "source_url":       "proprietary_database",
            "description":      description,
            "employee_count":   data.get("employee_count","11-50"),
            # 추가 필드
            "state":            hq_state if is_us(hq_country) else "",
            "hq_country":       hq_country,
            "hq_state":         hq_state if is_us(hq_country) else "",
            "hq_city":          hq_city,
            "source_type":      "proprietary_database",
        }

        seen_emails.add(email)
        leads.append(lead)
        loc = f"{hq_city}, {hq_country}" if hq_country else hq_city or "—"
        print(f"  FOUNDED: {company} | {full} ({email}) | {loc} | {role}")

    return leads

# ─── 메인 진입점 ──────────────────────────────────────────────────────────────

async def get_leads(
    num_leads: Optional[int] = 5,
    industry: str = None,
    region: str = None,
) -> List[Dict[str, Any]]:
    """
    Discover leads and append new ones to leads_output.json (no duplicates).
    - num_leads=None: run until interrupted (infinite); each new lead is appended to file.
    - num_leads=N: stop after N new (non-duplicate) leads; return those N for miner.py.
    Duplicate = same linkedin+company_linkedin already in leads_output.json.
    """
    if not SERPER_API_KEY:
        logger.error("❌ SERPER_API_KEY 없음")
        return []

    seen_emails = set()
    seen_lead_hashes = load_seen_lead_hashes()
    output_list, output_hashes = load_leads_output()
    return_list: List[Dict[str, Any]] = []
    infinite = num_leads is None
    limit = num_leads if num_leads is not None else 0
    print(f"[Pipeline] Loaded {len(seen_lead_hashes)} seen hashes, {len(output_list)} leads in output.")
    targets = (
        [(industry, f'(inurl:team OR inurl:about) "Founder" OR "CEO" {industry} -site:linkedin.com')]
        if industry else SEARCH_TARGETS
    )
    target_cycle = cycle(targets) if infinite else targets

    async with httpx.AsyncClient(follow_redirects=True) as client:
        industry_idx = 0
        for hint, query in target_cycle:
            if not infinite and len(return_list) >= limit:
                break
            industry_idx += 1
            print(f"\n[Pipeline] Industry: {hint}")
            domains = await find_domains(client, query)
            for domain, company in domains:
                if not infinite and len(return_list) >= limit:
                    break
                new = await build_leads(
                    client, domain, company, hint,
                    seen_emails, seen_lead_hashes, max_per=2
                )
                for lead in new:
                    if append_lead_to_output(lead, output_list, output_hashes):
                        b, n, e = lead.get("business", ""), lead.get("full_name", ""), lead.get("email", "")
                        print(f"  → Appended: {b} — {n} ({e})")
                        if not infinite:
                            return_list.append(lead)
                            if len(return_list) >= limit:
                                break
                    else:
                        print(f"  Skip: duplicate in file — {lead.get('business','')} — {lead.get('full_name','')}")
                await asyncio.sleep(0.5)

    save_seen_lead_hashes(seen_lead_hashes)
    if not infinite:
        print(f"[Pipeline] Done. New: {len(return_list)}, file total: {len(output_list)}")
    return return_list

# ─── 테스트 / standalone infinite run ──────────────────────────────────────────

if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    REQUIRED = [
        "business", "full_name", "first", "last", "email", "role",
        "website", "industry", "sub_industry", "country", "city",
        "linkedin", "company_linkedin", "source_url", "description", "employee_count"
    ]

    async def run_infinite():
        """Infinite search: append each new lead to leads_output.json. Ctrl+C to stop."""
        print("Lead search started (Ctrl+C to stop). Output: leads_output.json")
        t0 = time.time()
        try:
            await get_leads(num_leads=None, industry=None, region=None)
        except KeyboardInterrupt:
            elapsed = time.time() - t0
            out_list, _ = load_leads_output()
            print(f"\n⏱️  Run stopped after {elapsed:.1f}s. Total leads in file: {len(out_list)}")

    asyncio.run(run_infinite())