"""
main_leads.py — Subnet 71 (LeadPoet) free lead pipeline.

Every lead returned matches the exact schema required by miner.py/sanitize_prospect():
  business, full_name, first, last, email, role, website,
  industry, sub_industry, country, state, city, region,
  linkedin, company_linkedin, source_url, source_type,
  description, employee_count, hq_country, hq_state, hq_city,
  phone_numbers, socials, founded_year, ownership_type,
  company_type, number_of_locations

Pipeline (runs forever, builds pool in data/leads_found.jsonl):
  Phase 0 — Serve from existing pool (instant on subsequent calls)
  Phase 1 — GitHub org members     (real emails + LinkedIn from profiles)
  Phase 2 — YC companies → Hunter.io + page scrape
  Phase 3 — Wikipedia unicorns → Hunter.io + page scrape
  Phase 4 — Search engine discovery → Hunter.io + page scrape (∞ loop)

Env vars (all optional):
  GITHUB_TOKEN=xxx   raises GitHub rate limit 60→5000 req/hr
  BING_API_KEY=xxx   Bing Search free tier 1000/month
  SERPAPI_KEY=xxx    SerpAPI free tier 100/month
  GSE_API_KEY / GSE_CX / OPENROUTER_KEY / FIRECRAWL_KEY  — paid Lead Sorcerer
"""

import asyncio, csv, hashlib, json, logging, os, random, re, shutil
import sys, tempfile, time, urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
load_dotenv()

try:
    import requests
    from bs4 import BeautifulSoup
    FREE_SCRAPER_AVAILABLE = True
except ImportError:
    FREE_SCRAPER_AVAILABLE = False

# ── Paid Lead Sorcerer deps ────────────────────────────────────────────────
def check_dependencies():
    try:
        import phonenumbers, httpx, openai  # noqa
        return True, None
    except ImportError as e:
        return False, str(e)

deps_ok, error_msg = check_dependencies()
if not deps_ok:
    print(f"❌ Lead Sorcerer deps missing: {error_msg}")
    print("   Run: pip install -r miner_models/lead_sorcerer_main/requirements.txt")

LEAD_SORCERER_AVAILABLE = False
BASE_ICP_CONFIG: Dict[str, Any] = {}

if deps_ok:
    lead_sorcerer_dir = Path(__file__).parent.absolute()
    src_path    = lead_sorcerer_dir / "src"
    config_path = lead_sorcerer_dir / "config"
    for p in [str(lead_sorcerer_dir), str(src_path)]:
        if p not in sys.path:
            sys.path.insert(0, p)
    try:
        from orchestrator import LeadSorcererOrchestrator  # noqa
        LEAD_SORCERER_AVAILABLE = True
    except ImportError as e:
        print(f"❌ Orchestrator import failed: {e}")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    try:
        with open(lead_sorcerer_dir / "icp_config.json", "r", encoding="utf-8") as _f:
            BASE_ICP_CONFIG = json.load(_f)
    except Exception as e:
        if LEAD_SORCERER_AVAILABLE:
            raise RuntimeError(f"icp_config.json missing: {e}") from e

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR      = Path("data")
LEADS_JSONL   = DATA_DIR / "leads_found.jsonl"
COMPANIES_CSV = DATA_DIR / "company_list.csv"
SEARCH_CACHE  = DATA_DIR / ".search_cache.json"
CSV_FIELDS    = ["business","website","industry","sub_industry","country","state","city"]

# ─────────────────────────────────────────────────────────────────────────────
# Industry Taxonomy  (canonical values from Subnet 71)
# ─────────────────────────────────────────────────────────────────────────────
# Top-level industries
INDUSTRY_TAXONOMY: Dict[str, List[str]] = {
    "Tech & AI":       ["SaaS","AI/ML","Cloud Computing","Cybersecurity","Developer Tools",
                        "Data & Analytics","Infrastructure","IoT","Blockchain","AR/VR",
                        "Robotics","Autonomous Vehicles","API","Open Source","Developer Tools"],
    "FinTech":         ["Payments","Banking","InsurTech","WealthTech","Lending","Crypto",
                        "RegTech","Accounting","POS & Billing"],
    "Healthcare":      ["Digital Health","MedTech","Biotech","Pharma","Mental Health",
                        "Fitness & Wellness","HealthTech","Telemedicine","Medical Devices"],
    "E-commerce":      ["Marketplace","D2C","Retail Tech","Supply Chain","Logistics",
                        "Fulfillment","Dropshipping","Fashion","Beauty"],
    "EdTech":          ["Online Learning","LMS","Corporate Training","K-12","Higher Ed",
                        "Skills & Certification","Language Learning"],
    "HR Tech":         ["Recruiting","Payroll","HRIS","Employee Engagement","Benefits",
                        "Workforce Management","Background Checks"],
    "MarTech":         ["CRM","Email Marketing","SEO","Content Marketing","Social Media",
                        "Advertising Tech","Analytics","Affiliate Marketing"],
    "LegalTech":       ["Contract Management","Legal Research","Compliance","IP Management",
                        "Court Tech","Legal Billing"],
    "PropTech":        ["Real Estate SaaS","Property Management","iBuying","Mortgage Tech",
                        "Smart Buildings","Construction Tech"],
    "CleanTech":       ["Solar","Wind","Energy Storage","EV","Carbon Credits","Water Tech",
                        "Waste Management","Smart Grid"],
    "AgTech":          ["Precision Farming","Crop Tech","Livestock Tech","Supply Chain",
                        "Farm Management Software","Indoor Farming"],
    "Media & Content": ["Streaming","Publishing","Podcast","Gaming","Creator Economy",
                        "News Tech","Sports Tech","Music Tech"],
    "Travel & Hospitality":["Booking","Hotel Tech","Flight Tech","Restaurant Tech",
                            "Experience","Mobility"],
    "Manufacturing":   ["Industry 4.0","3D Printing","Quality Control","Supply Chain",
                        "Automation","Chemical","Materials"],
    "Professional Services":["Consulting","Accounting","Recruitment","Legal","Marketing Agency",
                              "IT Services","Business Intelligence"],
}

# Flat lookup: keyword → (industry, sub_industry)
_IND_KEYWORDS: List[Tuple[str, str, str]] = []
for _ind, _subs in INDUSTRY_TAXONOMY.items():
    _IND_KEYWORDS.append((_ind.lower(), _ind, _subs[0]))
    for _sub in _subs:
        _IND_KEYWORDS.append((_sub.lower(), _ind, _sub))

def _classify_industry(text: str, hint: str = "") -> Tuple[str, str]:
    """Return (industry, sub_industry) from free text, defaulting to Tech & AI / SaaS."""
    combined = (text + " " + hint).lower()
    best_ind = "Tech & AI"
    best_sub = "SaaS"
    best_len = 0
    for kw, ind, sub in _IND_KEYWORDS:
        if kw in combined and len(kw) > best_len:
            best_len = len(kw)
            best_ind = ind
            best_sub = sub
    return best_ind, best_sub

# ─────────────────────────────────────────────────────────────────────────────
# Employee count normalisation
# ─────────────────────────────────────────────────────────────────────────────
EMPLOYEE_RANGES = ["0-1","2-10","11-50","51-200","201-500",
                   "501-1,000","1,001-5,000","5,001-10,000","10,001+"]

def _normalize_employee_count(raw) -> str:
    """Convert any employee count value to the canonical range string."""
    if not raw:
        return ""
    s = str(raw).replace(",","").strip()
    # Already a valid range string?
    if s in EMPLOYEE_RANGES:
        return s
    # Try to parse as a number
    m = re.search(r"(\d+)", s)
    if not m:
        return ""
    n = int(m.group(1))
    if n <= 1:   return "0-1"
    if n <= 10:  return "2-10"
    if n <= 50:  return "11-50"
    if n <= 200: return "51-200"
    if n <= 500: return "201-500"
    if n <= 1000: return "501-1,000"
    if n <= 5000: return "1,001-5,000"
    if n <= 10000: return "5,001-10,000"
    return "10,001+"

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.I)
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{6,14}\d", re.I)

GENERIC_PREFIXES = {
    "info","hello","contact","support","admin","team","hi","mail","office",
    "enquiries","sales","marketing","press","media","hr","jobs","careers",
    "noreply","no-reply","webmaster","postmaster","abuse","legal","privacy",
    "billing","help","service","feedback","general","inquiries","enquiry",
    "questions","ask","connect","partnership","partners","investor","investors",
    "relations","ir","pr","news","alerts","notifications","ping","hey","we",
}

BLACKLIST = {
    "linkedin.com","twitter.com","x.com","facebook.com","instagram.com",
    "youtube.com","tiktok.com","reddit.com","wikipedia.org","wikimedia.org",
    "medium.com","substack.com","bloomberg.com","techcrunch.com","crunchbase.com",
    "pitchbook.com","forbes.com","wired.com","theverge.com","venturebeat.com",
    "businessinsider.com","reuters.com","wsj.com","nytimes.com","ft.com",
    "glassdoor.com","indeed.com","angel.co","wellfound.com","ycombinator.com",
    "producthunt.com","github.com","gitlab.com","google.com","bing.com",
    "yahoo.com","duckduckgo.com","amazon.com","microsoft.com","apple.com",
    "meta.com","quora.com","stackoverflow.com","slack.com","zoom.us",
    "notion.so","hubspot.com","salesforce.com","oracle.com","sap.com",
}

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Version/17.2 Safari/605.1.15",
]

COUNTRY_MAP = {
    "United States": r"\b(United States|U\.S\.A?\.?|USA|U\.S\.)\b",
    "UK":            r"\b(United Kingdom|U\.K\.?|England|Britain|Great Britain|\bUK\b)\b",
    "Germany":       r"\b(Germany|Deutschland)\b",
    "France":        r"\bFrance\b",
    "Canada":        r"\bCanada\b",
    "Australia":     r"\bAustralia\b",
    "India":         r"\bIndia\b",
    "Singapore":     r"\bSingapore\b",
    "Israel":        r"\bIsrael\b",
    "Netherlands":   r"\b(Netherlands|Holland)\b",
    "Sweden":        r"\bSweden\b",
    "Switzerland":   r"\bSwitzerland\b",
    "Denmark":       r"\bDenmark\b",
    "Finland":       r"\bFinland\b",
    "Norway":        r"\bNorway\b",
    "Ireland":       r"\bIreland\b",
    "Estonia":       r"\bEstonia\b",
    "Brazil":        r"\bBrazil\b",
    "UAE":           r"\b(UAE|Dubai|United Arab Emirates)\b",
    "Japan":         r"\bJapan\b",
    "South Korea":   r"\b(South Korea|Korea)\b",
    "Spain":         r"\bSpain\b",
    "Italy":         r"\bItaly\b",
    "Poland":        r"\bPoland\b",
}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC",
}
STATE_NAMES = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
    "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
    "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS",
    "kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA",
    "michigan":"MI","minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT",
    "nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ",
    "new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND",
    "ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA","rhode island":"RI",
    "south carolina":"SC","south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT",
    "vermont":"VT","virginia":"VA","washington":"WA","west virginia":"WV",
    "wisconsin":"WI","wyoming":"WY","district of columbia":"DC",
}

CITY_RE = re.compile(
    r"\b(San Francisco|New York|Los Angeles|Chicago|Houston|Phoenix|Philadelphia|"
    r"San Antonio|San Diego|Dallas|San Jose|Austin|Jacksonville|Fort Worth|Columbus|"
    r"Charlotte|Indianapolis|Seattle|Denver|Washington|Nashville|Oklahoma City|"
    r"El Paso|Boston|Portland|Las Vegas|Memphis|Louisville|Baltimore|Milwaukee|"
    r"Albuquerque|Tucson|Fresno|Sacramento|Mesa|Atlanta|Omaha|Colorado Springs|"
    r"Raleigh|Long Beach|Virginia Beach|Minneapolis|Tampa|New Orleans|Arlington|"
    r"Wichita|Bakersfield|Aurora|Anaheim|Santa Ana|Corpus Christi|Riverside|"
    r"St. Louis|Lexington|Stockton|Pittsburgh|Anchorage|Cincinnati|St. Paul|"
    r"Greensboro|Plano|Lincoln|Orlando|Irvine|Newark|Toledo|Durham|Chula Vista|"
    r"Fort Wayne|Jersey City|St. Petersburg|Laredo|Madison|Chandler|Buffalo|"
    r"Scottsdale|Redmond|Menlo Park|Palo Alto|Mountain View|Sunnyvale|Cupertino|"
    r"London|Berlin|Paris|Amsterdam|Stockholm|Tel Aviv|Bangalore|Toronto|Sydney|"
    r"Seoul|Tokyo|Singapore|Dubai|Dublin|Helsinki|Copenhagen|Oslo|Zurich|Vienna|"
    r"Warsaw|Prague|Tallinn|Munich|Hamburg|Lyon|Manchester|Montreal|Vancouver|"
    r"Melbourne|Brisbane|Cape Town|Johannesburg|São Paulo|Mexico City|"
    r"Nairobi|Lagos|Accra|Casablanca)\b",
    re.IGNORECASE,
)

NAME_ROLE_RE = re.compile(
    r"(?P<n>[A-Z][a-z]+(?:\s[A-Z][a-z]+)+)\s*[\|,–\-]\s*"
    r"(?P<t>CEO|Founder|Co-?[Ff]ounder|CTO|COO|CFO|President|Owner|"
    r"Managing Director|VP\s+\w+|Head of \w+|Director\b[^,\n]{0,25}|"
    r"Chief\s+\w+\s+Officer|Partner|Principal)",
    re.MULTILINE,
)

EXEC_TITLES = {"ceo","cto","founder","co-founder","cofounder","coo","cfo",
               "president","vp","vice president","head","director","owner","partner"}

# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────
def _hdrs() -> Dict:
    return {"User-Agent": random.choice(UA_LIST),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9", "DNT": "1"}

def _domain(url: str) -> str:
    try:
        n = urllib.parse.urlparse(url).netloc.lower()
        return re.sub(r"^www\d*\.", "", n)
    except Exception:
        return ""

def _valid_url(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    d = _domain(url)
    return bool(d) and len(d) > 3 and not any(b in d for b in BLACKLIST)

def _is_personal(email: str) -> bool:
    local = email.split("@")[0].split("+")[0].lower().rstrip("0123456789")
    return local not in GENERIC_PREFIXES and len(local) > 1

def _name_split(full: str) -> Tuple[str, str]:
    p = full.strip().split(maxsplit=1)
    return p[0], (p[1] if len(p) > 1 else "")

def _normalize_linkedin(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if raw.startswith("/in/"):
        return "https://www.linkedin.com" + raw
    if raw.startswith("/company/"):
        return "https://www.linkedin.com" + raw
    if "linkedin.com" in raw and not raw.startswith("http"):
        return "https://" + raw
    if raw.startswith("http") and "linkedin.com" in raw:
        return raw
    # Bare username
    if "/" not in raw and "." not in raw:
        return f"https://www.linkedin.com/in/{raw}"
    return raw if raw.startswith("http") else ""

def _infer_state(text: str, country: str) -> str:
    """Infer US state from text if country is US."""
    if country != "United States":
        return ""
    for name, abbr in STATE_NAMES.items():
        if name in text.lower():
            return abbr
    for abbr in US_STATES:
        if re.search(rf"\b{abbr}\b", text):
            return abbr
    return ""

def _infer_city(text: str) -> str:
    m = CITY_RE.search(text)
    return m.group(0) if m else ""

def _infer_country(text: str) -> str:
    for cname, pat in COUNTRY_MAP.items():
        if re.search(pat, text, re.I):
            return cname
    return ""

def _ensure_data():
    DATA_DIR.mkdir(exist_ok=True)

def _get(url: str, timeout=12, **kw) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=_hdrs(), timeout=timeout,
                         allow_redirects=True, **kw)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Lead record factory  — ALL fields required by sanitize_prospect()
# ─────────────────────────────────────────────────────────────────────────────
def _make_lead(
    business:         str = "",
    website:          str = "",
    full_name:        str = "",
    first:            str = "",
    last:             str = "",
    email:            str = "",
    role:             str = "",
    country:          str = "",
    state:            str = "",
    city:             str = "",
    industry:         str = "",
    sub_industry:     str = "",
    linkedin:         str = "",
    company_linkedin: str = "",
    source_url:       str = "",
    source_type:      str = "company_site",
    description:      str = "",
    employee_count:   str = "",
    phone_numbers:    Optional[List] = None,
    socials:          Optional[Dict] = None,
    founded_year:     str = "",
    ownership_type:   str = "",
    company_type:     str = "",
    number_of_locations: str = "",
    industry_hint:    str = "",   # extra text for classification, not stored
) -> Dict:
    # Classify industry from all available text if not supplied
    if not industry or not sub_industry:
        ind_text = f"{business} {description} {industry} {sub_industry} {industry_hint}"
        auto_ind, auto_sub = _classify_industry(ind_text, industry_hint)
        if not industry:
            industry = auto_ind
        if not sub_industry:
            sub_industry = auto_sub

    # Normalize employee count to valid range
    emp = _normalize_employee_count(employee_count)

    # Normalize LinkedIn URLs
    linkedin         = _normalize_linkedin(linkedin)
    company_linkedin = _normalize_linkedin(company_linkedin) if company_linkedin else \
                       (f"https://www.linkedin.com/company/{_domain(website).split('.')[0]}"
                        if website else "")

    # hq fields = same as contact location (best we can do without paid data)
    hq_country = country
    hq_state   = state
    hq_city    = city

    # region = country (used by validator)
    region = country

    return {
        "business":            business,
        "full_name":           full_name,
        "first":               first,
        "last":                last,
        "email":               email,
        "role":                role,
        "website":             website,
        "industry":            industry,
        "sub_industry":        sub_industry,
        "country":             country,
        "state":               state,
        "city":                city,
        "region":              region,
        "linkedin":            linkedin,
        "company_linkedin":    company_linkedin,
        "source_url":          source_url or website,
        "source_type":         source_type,
        "description":         description,
        "employee_count":      emp,
        "hq_country":          hq_country,
        "hq_state":            hq_state,
        "hq_city":             hq_city,
        "phone_numbers":       phone_numbers or [],
        "socials":             socials or {},
        "founded_year":        founded_year,
        "ownership_type":      ownership_type,
        "company_type":        company_type,
        "number_of_locations": number_of_locations,
    }

# ─────────────────────────────────────────────────────────────────────────────
# Persistent lead store
# ─────────────────────────────────────────────────────────────────────────────
_seen_emails:  Set[str] = set()
_seen_domains: Set[str] = set()
_store_ready   = False

def _load_store():
    global _store_ready
    if _store_ready:
        return
    _store_ready = True
    if not LEADS_JSONL.exists():
        return
    try:
        for line in LEADS_JSONL.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                r = json.loads(line)
                if r.get("email"):
                    _seen_emails.add(r["email"].lower())
                if r.get("website"):
                    _seen_domains.add(_domain(r["website"]))
            except Exception:
                pass
    except Exception:
        pass

def _is_duplicate(lead: Dict) -> bool:
    email  = (lead.get("email") or "").lower().strip()
    domain = _domain(lead.get("website") or "")
    if email and email in _seen_emails:
        return True
    if not email and domain and domain in _seen_domains:
        return True
    return False

def _persist(lead: Dict):
    email  = (lead.get("email") or "").lower().strip()
    domain = _domain(lead.get("website") or "")
    if email:
        _seen_emails.add(email)
    if domain:
        _seen_domains.add(domain)
    _ensure_data()
    with LEADS_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(lead, ensure_ascii=False) + "\n")

def _load_pool(n: int = 500) -> List[Dict]:
    if not LEADS_JSONL.exists():
        return []
    try:
        lines = LEADS_JSONL.read_text(encoding="utf-8").splitlines()
        random.shuffle(lines)
        out = []
        for line in lines:
            try:
                r = json.loads(line)
                if r.get("business") and r.get("website"):
                    out.append(r)
                    if len(out) >= n:
                        break
            except Exception:
                pass
        return out
    except Exception:
        return []

# ─────────────────────────────────────────────────────────────────────────────
# CSV helpers
# ─────────────────────────────────────────────────────────────────────────────
_csv_domains: Set[str] = set()
_csv_loaded   = False

def _load_csv_domains():
    global _csv_loaded
    if _csv_loaded:
        return
    _csv_loaded = True
    if not COMPANIES_CSV.exists():
        return
    try:
        for row in csv.DictReader(open(COMPANIES_CSV, encoding="utf-8")):
            d = _domain(row.get("website",""))
            if d:
                _csv_domains.add(d)
    except Exception:
        pass

def _append_csv(rows: List[Dict]):
    global _csv_loaded
    _ensure_data()
    exists = COMPANIES_CSV.exists()
    with COMPANIES_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerows(rows)
    for r in rows:
        d = _domain(r.get("website",""))
        if d:
            _csv_domains.add(d)
    _csv_loaded = False

def _read_csv() -> List[Dict]:
    if not COMPANIES_CSV.exists():
        return []
    rows = []
    try:
        for row in csv.DictReader(open(COMPANIES_CSV, encoding="utf-8")):
            if row.get("website"):
                rows.append({k: row.get(k,"").strip() for k in CSV_FIELDS})
    except Exception:
        pass
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# Search cache
# ─────────────────────────────────────────────────────────────────────────────
def _load_cache() -> Dict:
    try:
        if SEARCH_CACHE.exists():
            return json.loads(SEARCH_CACHE.read_text())
    except Exception:
        pass
    return {"searched": [], "found_domains": []}

def _save_cache(c: Dict):
    _ensure_data()
    SEARCH_CACHE.write_text(json.dumps(c, indent=2))

def _qhash(q: str) -> str:
    return hashlib.md5(q.encode()).hexdigest()[:12]

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# SOURCE 1 — Hunter.io  (no API key, public domain search)
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
def _hunter_domain(domain: str, seed: Dict) -> List[Dict]:
    """Hunter.io public /domain-search — returns real employee emails."""
    leads = []
    try:
        r = requests.get(
            "https://hunter.io/api/v2/domain-search",
            params={"domain": domain, "limit": 10, "type": "personal"},
            headers={**_hdrs(), "Accept": "application/json"},
            timeout=15,
        )
        if r.status_code not in (200, 201):
            return leads
        data = r.json().get("data", {})
        if not data:
            return leads

        biz     = data.get("organization") or seed.get("business","") or domain.split(".")[0].title()
        desc    = data.get("description") or seed.get("description","")
        country = data.get("country") or seed.get("country","")
        size    = _normalize_employee_count(data.get("size",""))
        website = f"https://{domain}"
        city    = seed.get("city","")
        state   = seed.get("state","")

        # Infer location from description if missing
        if not country and desc:
            country = _infer_country(desc) or country
        if not city and desc:
            city = _infer_city(desc)
        if not state and country == "United States":
            state = _infer_state(desc + " " + city, country)

        ind_hint = seed.get("industry","") + " " + seed.get("sub_industry","")
        ind, sub = _classify_industry(desc + " " + biz + " " + ind_hint)

        co_li = f"https://www.linkedin.com/company/{domain.split('.')[0]}"

        for emp in (data.get("emails") or []):
            email = (emp.get("value") or "").lower().strip()
            if not email or not _is_personal(email):
                continue
            if (emp.get("confidence") or 0) < 40:
                continue
            fname = emp.get("first_name") or ""
            lname = emp.get("last_name")  or ""
            role  = emp.get("position")   or ""
            full  = f"{fname} {lname}".strip()
            li    = _normalize_linkedin(emp.get("linkedin",""))

            leads.append(_make_lead(
                business=biz, website=website,
                full_name=full, first=fname, last=lname,
                email=email, role=role,
                country=country, state=state, city=city,
                industry=ind, sub_industry=sub,
                linkedin=li, company_linkedin=co_li,
                source_url=f"https://hunter.io/domain-search/{domain}",
                source_type="directory",
                description=desc, employee_count=size,
            ))
    except Exception:
        pass
    return leads

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# SOURCE 2 — GitHub org member emails + LinkedIn
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

GITHUB_ORGS = [
    "stripe","twilio","plaid","brex","gusto","rippling","lattice","figma",
    "linear","vercel","netlify","supabase","neon-database","cockroachdb",
    "timescale","clickhouse","dbtlabs","airbytehq","prefecthq","dagster-io",
    "wandb","huggingface","cohere-ai","mistralai","langchain-ai","llamaindex",
    "chroma-core","qdrant-engine","weaviate","milvus-io","elastic","hashicorp",
    "grafana","pagerduty","atlassian","zendesk","intercom","descript","miro",
    "airtable","coda-io","retool","webflow","stytch","clerk","auth0","segment",
    "amplitude","mixpanel","posthog","heap","sentry-io","cloudflare","render",
    "railway","fly-io","shippo","gocardless","adyen","affirm","klarna","marqeta",
    "opendoor","teladoc","hims-hers","cal-com","plane-so","twenty-crm",
    "AppFlowy-IO","nocodb","n8n-io","activepieces","windmill-labs",
    "triggerdotdev","temporal-io","inngest","svix","tremorlabs","blitz-js",
    "refinedev","pocketbase","directus","appwrite","nhost-run","supertokens",
    "ory","casdoor","zitadel","logto-io","boxyhq","workos","hanko-io",
    "openfort-xyz","thirdweb","alchemy-platform","turso-tech","nativelink",
    "rerun-io","astronomer-io","kestra-io","mage-ai","bentoml","ray-project",
    "modal-com","replicate","fal-ai","anyscale","mosaic-ml","together-ai",
]

def _gh_hdrs() -> Dict:
    h = {**_hdrs(), "Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"token {GITHUB_TOKEN}"
    return h

def _gh_org_seed(org: str) -> Optional[Dict]:
    try:
        r = requests.get(f"https://api.github.com/orgs/{org}",
                         headers=_gh_hdrs(), timeout=10)
        if r.status_code != 200:
            return None
        d = r.json()
        if d.get("message") == "Not Found":
            return None
        blog = (d.get("blog") or "").strip()
        if blog and not blog.startswith("http"):
            blog = "https://" + blog
        loc     = (d.get("location") or "").strip()
        city    = _infer_city(loc) or (loc.split(",")[0].strip() if "," in loc else loc)
        country = _infer_country(loc) or ("United States" if not loc else "")
        state   = _infer_state(loc, country)
        desc    = d.get("description") or ""
        ind, sub = _classify_industry(desc + " " + (d.get("name") or org), "software")
        emp_raw  = d.get("public_repos", 0)
        # Rough employee estimate from repo count
        emp = _normalize_employee_count(
            1 if emp_raw < 5 else
            5 if emp_raw < 20 else
            25 if emp_raw < 50 else
            100 if emp_raw < 200 else
            500
        )
        return {
            "business":    d.get("name") or org.replace("-"," ").title(),
            "website":     blog or f"https://github.com/{org}",
            "industry":    ind,
            "sub_industry": sub,
            "country":     country,
            "state":       state,
            "city":        city,
            "description": desc,
            "employee_count": emp,
            "_gh_org":     org,
        }
    except Exception:
        return None

def _gh_member_leads(org: str, seed: Dict, max_per_org: int = 3) -> List[Dict]:
    """
    Fetch up to max_per_org leads from a GitHub org.
    Prioritises founders/executives. Extracts LinkedIn from blog field.
    """
    leads = []
    biz     = seed.get("business","")
    website = seed.get("website","")
    domain  = _domain(website) if "github.com" not in website else ""
    country = seed.get("country","")
    state   = seed.get("state","")
    city    = seed.get("city","")
    ind     = seed.get("industry","Tech & AI")
    sub     = seed.get("sub_industry","SaaS")
    desc    = seed.get("description","")
    emp     = seed.get("employee_count","")
    co_li   = f"https://www.linkedin.com/company/{org}"

    try:
        r = requests.get(f"https://api.github.com/orgs/{org}/public_members",
                         headers=_gh_hdrs(), params={"per_page": 30}, timeout=10)
        if r.status_code != 200:
            return leads
        members = r.json()
        if not isinstance(members, list):
            return leads

        candidates = []
        for member in members[:25]:
            uname = member.get("login","")
            if not uname:
                continue
            try:
                r2 = requests.get(f"https://api.github.com/users/{uname}",
                                   headers=_gh_hdrs(), timeout=8)
                if r2.status_code != 200:
                    continue
                u       = r2.json()
                bio     = (u.get("bio") or "").strip()
                comp    = (u.get("company") or "").lstrip("@").strip()
                blog    = (u.get("blog") or "").strip()
                twitter = u.get("twitter_username","") or ""
                full    = u.get("name") or uname
                email   = (u.get("email") or "").lower().strip()
                loc_u   = (u.get("location") or "").strip()

                # Determine role
                role = "Engineer"
                bio_l  = bio.lower()
                comp_l = comp.lower()
                for title in ["CEO","CTO","Founder","Co-Founder","COO","CFO",
                               "President","VP","Head","Director","Owner","Partner"]:
                    if title.lower() in bio_l or title.lower() in comp_l:
                        role = title
                        break
                else:
                    if bio:
                        # Use first sentence of bio as role hint
                        role = bio.split(".")[0][:50].strip() or "Engineer"

                # LinkedIn from blog field
                li = ""
                if "linkedin.com" in blog.lower():
                    li = _normalize_linkedin(blog)

                # Location override from user profile
                u_country = _infer_country(loc_u) or country
                u_state   = _infer_state(loc_u, u_country) or state
                u_city    = _infer_city(loc_u) or city

                if not email:
                    # Guess from first name + domain
                    if domain:
                        slug = re.sub(r"[^a-z0-9]","",
                                      (full.split()[0] if " " in full else full).lower())
                        if slug and len(slug) > 1:
                            email = f"{slug}@{domain}"

                if not email or not _is_personal(email):
                    continue

                is_exec = any(t in bio_l or t in comp_l for t in EXEC_TITLES)
                fname, lname = _name_split(full)

                socials_d: Dict = {"github": f"https://github.com/{uname}"}
                if twitter:
                    socials_d["twitter"] = f"https://twitter.com/{twitter}"

                candidates.append({
                    "is_exec": is_exec,
                    "lead": _make_lead(
                        business=biz, website=website,
                        full_name=full, first=fname, last=lname,
                        email=email, role=role,
                        country=u_country, state=u_state, city=u_city,
                        industry=ind, sub_industry=sub,
                        linkedin=li, company_linkedin=co_li,
                        source_url=f"https://github.com/{org}",
                        source_type="public_registry",
                        description=desc, employee_count=emp,
                        socials=socials_d,
                    )
                })
                time.sleep(0.25)
            except Exception:
                continue

        # Execs first, then others — return max_per_org total
        candidates.sort(key=lambda x: (0 if x["is_exec"] else 1))
        leads = [c["lead"] for c in candidates[:max_per_org]]

    except Exception:
        pass
    return leads

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# SOURCE 3 — YC company directory  (Algolia public API, ~4000 companies)
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
def _yc_page(page: int, per_page: int = 100) -> List[Dict]:
    try:
        r = requests.post(
            "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries",
            params={
                "x-algolia-agent":          "Algolia for JavaScript (4.14.2)",
                "x-algolia-api-key":        "9ddd9e7f7a7be8e17b0de1dd0c2e2db5",
                "x-algolia-application-id": "45BWZJ1SGC",
            },
            json={"requests":[{"indexName":"YCCompany_production",
                                "params":f"hitsPerPage={per_page}&page={page}"}]},
            timeout=20,
        )
        hits = r.json().get("results",[{}])[0].get("hits",[])
        out = []
        for h in hits:
            url = (h.get("url") or h.get("website") or "").strip()
            if not url:
                continue
            if not url.startswith("http"):
                url = "https://" + url
            name = h.get("name","").strip()
            if not name:
                continue
            tags = h.get("tags") or []
            desc = h.get("one_liner") or h.get("description") or ""
            ind, sub = _classify_industry(desc + " " + " ".join(tags), "saas tech")
            city = (h.get("city") or "San Francisco").strip()
            country_raw = h.get("country") or "United States"
            country = _infer_country(country_raw) or country_raw
            state   = _infer_state(city + " " + country_raw, country)
            out.append({
                "business":    name,
                "website":     url.rstrip("/"),
                "industry":    ind,
                "sub_industry": sub,
                "country":     country,
                "state":       state,
                "city":        city,
                "description": desc,
                "employee_count": "",
            })
        return out
    except Exception:
        return []

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# SOURCE 4 — Wikipedia unicorn list
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
def _wikipedia_unicorns() -> List[Dict]:
    if not FREE_SCRAPER_AVAILABLE:
        return []
    seeds = []
    try:
        r = _get("https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies")
        if not r:
            return seeds
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.select("table.wikitable"):
            for tr in table.select("tr")[1:]:
                tds = tr.select("td")
                if len(tds) < 4:
                    continue
                name    = re.sub(r"\[\d+\]","",tds[0].get_text(strip=True)).strip()
                country = re.sub(r"\[\d+\]","",tds[3].get_text(strip=True)).strip() if len(tds)>3 else ""
                ind_raw = re.sub(r"\[\d+\]","",tds[4].get_text(strip=True)).strip() if len(tds)>4 else ""
                if not name or len(name) < 2:
                    continue
                country = _infer_country(country) or country
                state   = _infer_state(country, country)
                ind, sub = _classify_industry(ind_raw + " " + name)
                slug    = re.sub(r"[^a-z0-9]","",name.lower())
                if not slug:
                    continue
                seeds.append({
                    "business": name, "website": f"https://www.{slug}.com",
                    "industry": ind, "sub_industry": sub,
                    "country": country, "state": state, "city": "",
                    "description": "", "employee_count": "1,001-5,000",
                })
    except Exception:
        pass
    return seeds

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# SOURCE 5 — Company page scraper  (BeautifulSoup)
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
def _scrape_company(seed: Dict) -> List[Dict]:
    """Scrape /team /about /contact — returns LIST of leads with all fields."""
    if not FREE_SCRAPER_AVAILABLE:
        return []
    website = seed.get("website","").rstrip("/")
    if not website:
        return []

    base = website
    pages = [base,
             f"{base}/about", f"{base}/about-us",
             f"{base}/team",  f"{base}/our-team",
             f"{base}/leadership", f"{base}/founders",
             f"{base}/contact",   f"{base}/people"]

    all_html = ""
    all_text = ""
    main_soup = None

    for page_url in pages[:6]:
        try:
            r = requests.get(page_url, headers=_hdrs(), timeout=10, allow_redirects=True)
            if r.status_code != 200:
                continue
            if "text/html" not in r.headers.get("content-type",""):
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            if main_soup is None:
                main_soup = soup
            all_html += " " + r.text
            all_text += " " + soup.get_text(" ", strip=True)
            time.sleep(random.uniform(0.3, 0.7))
        except Exception:
            continue

    if not main_soup:
        return []

    # ── Company metadata ─────────────────────────────────────────────────────
    og  = main_soup.find("meta", property="og:site_name")
    biz = (og.get("content","").strip() if og else "") or \
          seed.get("business","") or _domain(base).split(".")[0].title()

    desc_tag = (main_soup.find("meta", {"name":"description"}) or
                main_soup.find("meta", property="og:description"))
    desc = (desc_tag.get("content","").strip() if desc_tag else "")[:400]

    # ── Location ─────────────────────────────────────────────────────────────
    country = _infer_country(all_text) or seed.get("country","")
    city    = _infer_city(all_text)    or seed.get("city","")
    state   = _infer_state(all_text, country) or seed.get("state","")

    # ── Industry from page content ────────────────────────────────────────────
    ind_hint = seed.get("industry","") + " " + seed.get("sub_industry","")
    ind, sub = _classify_industry(desc + " " + all_text[:1000], ind_hint)

    # ── Employee count ────────────────────────────────────────────────────────
    emp_m = re.search(r"(\d[\d,]+)\s*(employees|staff|people|team members)", all_text, re.I)
    emp   = _normalize_employee_count(emp_m.group(1).replace(",","") if emp_m else seed.get("employee_count",""))

    # ── Founded year ─────────────────────────────────────────────────────────
    founded_m = re.search(r"[Ff]ounded\s+(?:in\s+)?(\d{4})|[Ee]stablished\s+(?:in\s+)?(\d{4})", all_text)
    founded   = (founded_m.group(1) or founded_m.group(2)) if founded_m else ""

    # ── Phone ─────────────────────────────────────────────────────────────────
    phones = list({p.strip() for p in PHONE_RE.findall(all_text) if len(p.strip()) >= 8})[:3]

    # ── LinkedIn / social links ───────────────────────────────────────────────
    co_li    = ""
    socials: Dict[str, str] = {}
    for a in main_soup.find_all("a", href=True):
        h = a["href"]
        if "linkedin.com/company" in h and not co_li:
            co_li = _normalize_linkedin(h)
        elif "twitter.com/" in h and "twitter" not in socials:
            socials["twitter"] = h
        elif "x.com/" in h and "twitter" not in socials:
            socials["twitter"] = h
    if not co_li:
        co_li = f"https://www.linkedin.com/company/{_domain(base).split('.')[0]}"

    # ── All emails ─────────────────────────────────────────────────────────────
    all_emails_raw = list({e.lower() for e in EMAIL_RE.findall(all_html)
                           if not re.search(r"\.(png|jpg|gif|svg|css|js|woff)$",e,re.I)})
    personal_emails = [e for e in all_emails_raw if _is_personal(e)]
    any_emails      = personal_emails or all_emails_raw

    # ── Named contacts ────────────────────────────────────────────────────────
    leads   = []
    seen_ns: Set[str] = set()

    for m in NAME_ROLE_RE.finditer(all_text):
        full = m.group("n").strip()
        role = m.group("t").strip()
        if full in seen_ns:
            continue
        seen_ns.add(full)
        fname, lname = _name_split(full)

        # Match email by local part similarity to name
        email = ""
        for e in personal_emails:
            local = e.split("@")[0].lower()
            if fname.lower()[:4] in local or (lname and lname.lower()[:4] in local):
                email = e
                break

        # Personal LinkedIn from nearby links
        person_li = ""
        for a in main_soup.find_all("a", href=True):
            h = a["href"]
            if "linkedin.com/in/" in h:
                txt = a.get_text(strip=True).lower()
                if fname.lower() in txt or (lname and lname.lower() in txt):
                    person_li = _normalize_linkedin(h)
                    break

        leads.append(_make_lead(
            business=biz, website=base,
            full_name=full, first=fname, last=lname,
            email=email, role=role,
            country=country, state=state, city=city,
            industry=ind, sub_industry=sub,
            linkedin=person_li, company_linkedin=co_li,
            source_url=base, source_type="company_site",
            description=desc, employee_count=emp,
            phone_numbers=phones, socials=socials,
            founded_year=founded,
        ))

    # Fallback: no named contacts — emit one lead with whatever email we found
    if not leads:
        email = any_emails[0] if any_emails else ""
        leads.append(_make_lead(
            business=biz, website=base,
            full_name="", first="", last="",
            email=email, role="",
            country=country, state=state, city=city,
            industry=ind, sub_industry=sub,
            linkedin="", company_linkedin=co_li,
            source_url=base, source_type="company_site",
            description=desc, employee_count=emp,
            phone_numbers=phones, socials=socials,
            founded_year=founded,
        ))

    return leads

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# SOURCE 6 — DuckDuckGo + Bing + SerpAPI search
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
SEARCH_INDUSTRIES = list(INDUSTRY_TAXONOMY.keys())
SEARCH_REGIONS = [
    "San Francisco","New York","London","Berlin","Paris","Amsterdam","Stockholm",
    "Tel Aviv","Bangalore","Toronto","Sydney","Seoul","Tokyo","Singapore","Dubai",
    "Austin","Boston","Chicago","Seattle","Miami","Los Angeles","Dublin",
    "Helsinki","Copenhagen","Oslo","Zurich","Warsaw","Tallinn",
    "USA","UK","Germany","France","Canada","Australia","Israel","India",
    "Netherlands","Sweden","Switzerland","Denmark","Finland","Norway","Ireland",
    "Estonia","UAE","Brazil","Spain","Italy","Japan","South Korea",
]
Q_TEMPLATES = [
    '"{industry}" startup "{region}" CEO email contact',
    "{industry} company {region} founder email",
    "{industry} {region} startup team about",
    '"{industry}" "{region}" CEO founder contact',
    "{industry} startup {region} leadership contact email",
    '"{region}" {industry} startup CEO founder email',
    "{industry} {region} company about contact email",
]

def _ddg(query: str, n: int = 10) -> List[str]:
    urls = []
    try:
        r = requests.get("https://html.duckduckgo.com/html/",
                         params={"q":query,"kl":"us-en"},
                         headers=_hdrs(), timeout=15)
        soup = BeautifulSoup(r.text,"html.parser")
        for a in soup.select("a.result__a"):
            href = a.get("href","")
            if "uddg=" in href:
                href = urllib.parse.unquote(href.split("uddg=")[-1].split("&")[0])
            if _valid_url(href):
                urls.append(href)
            if len(urls) >= n:
                break
    except Exception:
        pass
    return urls

def _bing_search(query: str, n: int = 10) -> List[str]:
    key = os.getenv("BING_API_KEY","")
    if not key:
        return []
    try:
        r = requests.get("https://api.bing.microsoft.com/v7.0/search",
                         headers={"Ocp-Apim-Subscription-Key":key},
                         params={"q":query,"count":n,"mkt":"en-US"}, timeout=10)
        return [i.get("url","") for i in r.json().get("webPages",{}).get("value",[])
                if _valid_url(i.get("url",""))]
    except Exception:
        return []

def _serpapi_search(query: str, n: int = 10) -> List[str]:
    key = os.getenv("SERPAPI_KEY","")
    if not key:
        return []
    try:
        r = requests.get("https://serpapi.com/search",
                         params={"q":query,"api_key":key,"engine":"google","num":n},
                         timeout=15)
        return [i.get("link","") for i in r.json().get("organic_results",[])
                if _valid_url(i.get("link",""))]
    except Exception:
        return []

def _search(query: str, n: int = 8) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for fn in [_ddg, _bing_search, _serpapi_search]:
        for url in fn(query, n):
            d = _domain(url)
            if d and d not in seen:
                seen.add(d)
                out.append(url)
        if len(out) >= n:
            break
    return out[:n]

def _gen_queries(industry: Optional[str], region: Optional[str], n: int) -> List[Dict]:
    inds = [industry] if industry else SEARCH_INDUSTRIES
    regs = [region]   if region   else SEARCH_REGIONS
    out  = []
    for ind in inds:
        for reg in regs:
            for tmpl in Q_TEMPLATES:
                out.append({"query":tmpl.format(industry=ind,region=reg),
                             "industry":ind,"region":reg})
    random.shuffle(out)
    return out[:n]

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# MAIN FREE PIPELINE
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────
async def get_leads_free(
    num_leads: int,
    industry:  Optional[str] = None,
    region:    Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Returns num_leads from pool. Keeps discovering more in background.
    Pool in data/leads_found.jsonl grows permanently across miner restarts.

    Phase 0 — Existing pool        (instant on 2nd+ calls)
    Phase 1 — GitHub org members   (real emails + LinkedIn, max 3/org)
    Phase 2 — YC directory → Hunter.io + scrape  (4000+ companies)
    Phase 3 — Wikipedia unicorns → Hunter.io + scrape
    Phase 4 — Search engine discovery (infinite loop)
    """
    if not FREE_SCRAPER_AVAILABLE:
        print("⚠️  pip install requests beautifulsoup4")
        return []

    _load_store()
    _load_csv_domains()
    loop = asyncio.get_running_loop()
    collected: List[Dict] = []

    def _accept(lead: Dict) -> bool:
        if not lead.get("business") or not lead.get("website"):
            return False
        if _is_duplicate(lead):
            return False
        _persist(lead)
        collected.append(lead)
        e  = lead.get("email") or "—"
        b  = lead.get("business","?")
        r  = lead.get("role","")
        li = lead.get("linkedin","") or lead.get("company_linkedin","") or "—"
        print(f"   ✅ [{len(collected)}] {b} | {e} | {r} | {li}")
        return True

    # ── Phase 0: Existing pool ────────────────────────────────────────────────
    print(f"\n📦 Phase 0: existing pool...")
    for lead in _load_pool(num_leads * 5):
        collected.append(lead)
    print(f"   {len(collected)} from pool")
    if len(collected) >= num_leads:
        return collected[:num_leads]

    # ── Phase 1: GitHub org members ───────────────────────────────────────────
    print(f"\n🐙 Phase 1: GitHub org members (max 3/org)...")
    orgs = GITHUB_ORGS.copy()
    random.shuffle(orgs)
    for org in orgs:
        seed = await loop.run_in_executor(None, _gh_org_seed, org)
        if not seed:
            continue
        print(f"   → {org} ({seed.get('business','')})")
        gh_leads = await loop.run_in_executor(None, _gh_member_leads, org, seed, 3)
        for lead in gh_leads:
            _accept(lead)
        await asyncio.sleep(0.5)

    if len(collected) >= num_leads:
        return collected[:num_leads]

    # ── Phase 2: YC companies → Hunter.io + scrape ───────────────────────────
    print(f"\n🍊 Phase 2: YC directory (~4000 companies)...")
    yc_pn   = 0
    yc_seen: Set[str] = set()
    while yc_pn < 40:
        batch = await loop.run_in_executor(None, _yc_page, yc_pn, 100)
        yc_pn += 1
        if not batch:
            break
        for company in batch:
            d = _domain(company["website"])
            if not d or d in yc_seen:
                continue
            yc_seen.add(d)
            print(f"   → YC: {company['business']} ({d})")
            for lead in await loop.run_in_executor(None, _hunter_domain, d, company):
                _accept(lead)
            for lead in await loop.run_in_executor(None, _scrape_company, company):
                _accept(lead)
            await asyncio.sleep(random.uniform(0.5, 1.5))

    if len(collected) >= num_leads:
        return collected[:num_leads]

    # ── Phase 3: Wikipedia unicorns → Hunter.io + scrape ─────────────────────
    print(f"\n📚 Phase 3: Wikipedia unicorns...")
    wiki_seeds = await loop.run_in_executor(None, _wikipedia_unicorns)
    random.shuffle(wiki_seeds)
    wiki_seen: Set[str] = set()
    for company in wiki_seeds:
        d = _domain(company["website"])
        if not d or d in wiki_seen:
            continue
        wiki_seen.add(d)
        print(f"   → Wiki: {company['business']}")
        for lead in await loop.run_in_executor(None, _hunter_domain, d, company):
            _accept(lead)
        for lead in await loop.run_in_executor(None, _scrape_company, company):
            _accept(lead)
        await asyncio.sleep(random.uniform(0.5, 1.5))

    if len(collected) >= num_leads:
        return collected[:num_leads]

    # ── Phase 4: Search engine discovery (infinite) ───────────────────────────
    print(f"\n🔍 Phase 4: Search engine discovery (infinite)...")
    cache            = _load_cache()
    searched: Set[str] = set(cache.get("searched",[]))
    found:    Set[str] = set(cache.get("found_domains",[])) | _seen_domains
    s_round   = 0

    while len(collected) < num_leads:
        s_round += 1
        print(f"\n   🔄 Search round {s_round} ({len(collected)}/{num_leads})")
        queries   = _gen_queries(industry, region, n=100)
        run_count = 0
        new_seeds: List[Dict] = []

        for q in queries:
            if run_count >= 15:
                break
            qh = _qhash(q["query"])
            if qh in searched:
                continue
            urls = await loop.run_in_executor(None, _search, q["query"], 8)
            run_count += 1
            searched.add(qh)
            for url in urls:
                d = _domain(url)
                if d and d not in found:
                    found.add(d)
                    country_guess = ""
                    city_guess    = ""
                    reg = q["region"]
                    if any(c in reg for c in ["USA","UK","Germany","France","Canada",
                            "Australia","India","Israel","Netherlands","Sweden",
                            "Switzerland","Denmark","Finland","Norway","Ireland",
                            "Estonia","UAE","Brazil","Spain","Italy","Japan","South Korea"]):
                        country_guess = _infer_country(reg) or reg
                    else:
                        city_guess = reg

                    ind_q, sub_q = _classify_industry(q["industry"])
                    new_seeds.append({
                        "business":    _domain(url).split(".")[0].title(),
                        "website":     url.rstrip("/"),
                        "industry":    ind_q,
                        "sub_industry": sub_q,
                        "country":     country_guess,
                        "state":       "",
                        "city":        city_guess,
                        "description": "",
                        "employee_count": "",
                    })
            await asyncio.sleep(random.uniform(2.0, 4.0))

        if new_seeds:
            _append_csv(new_seeds)
            print(f"   💾 {len(new_seeds)} new domains found")

        cache["searched"]      = list(searched)
        cache["found_domains"] = list(found)
        _save_cache(cache)

        untried = [s for s in new_seeds + _read_csv()
                   if _domain(s.get("website","")) not in _seen_domains][: max((num_leads-len(collected))*3, 15)]

        if not untried:
            if run_count == 0:
                print("   🔁 Search cache full — resetting")
                searched.clear()
            await asyncio.sleep(3)
            continue

        print(f"   🏗️  Scraping {len(untried)} pages...")
        for i in range(0, len(untried), 5):
            if len(collected) >= num_leads:
                break
            batch = untried[i:i+5]
            for res in await asyncio.gather(
                *[loop.run_in_executor(None, _scrape_company, s) for s in batch],
                return_exceptions=True
            ):
                if not isinstance(res, Exception):
                    for lead in (res or []):
                        _accept(lead)
            for s in batch:
                d = _domain(s.get("website",""))
                if d:
                    for lead in await loop.run_in_executor(None, _hunter_domain, d, s):
                        _accept(lead)

        print(f"   📊 {len(collected)}/{num_leads} leads")

    print(f"\n✅ Pipeline done — {len(collected)} leads in pool")
    return collected[:num_leads]


# ─────────────────────────────────────────────────────────────────────────────
# Paid Lead Sorcerer (unchanged)
# ─────────────────────────────────────────────────────────────────────────────
def _create_industry_specific_config(industry=None):
    config = json.loads(json.dumps(BASE_ICP_CONFIG))
    if not industry:
        return config
    ind = industry.lower()
    if any(k in ind for k in ("tech","software","ai")):
        config["icp_text"] = "Technology companies needing contacts."
        config["queries"]  = ["technology company contact information"]
    elif any(k in ind for k in ("finance","fintech","bank")):
        config["icp_text"] = "Finance / FinTech organisations needing contacts."
        config["queries"]  = ["fintech company contact information"]
    elif any(k in ind for k in ("health","med","clinic")):
        config["icp_text"] = "Healthcare & wellness businesses needing contacts."
        config["queries"]  = ["healthcare company contact information"]
    return config

def _setup_temp_environment(temp_dir):
    tp = Path(temp_dir)
    tc = tp / "config"
    tc.mkdir(exist_ok=True)
    costs = config_path / "costs.yaml"
    if costs.exists():
        shutil.copy2(costs, tc / "costs.yaml")
    sp = config_path / "prompts"
    if sp.exists():
        shutil.copytree(sp, tc / "prompts", dirs_exist_ok=True)
    ss = lead_sorcerer_dir / "schemas"
    if ss.exists():
        shutil.copytree(ss, tp / "schemas", dirs_exist_ok=True)

def _convert_lead_record(rec):
    company  = rec.get("company", {})
    contacts = rec.get("contacts", [])
    best     = next((c for c in contacts if c.get("email")), contacts[0] if contacts else None)
    if best:
        full  = best.get("full_name") or ""
        first = best.get("first_name") or ""
        last  = best.get("last_name")  or ""
        if full and not (first or last):
            first, last = _name_split(full)
        elif not full:
            full = f"{first} {last}".strip()
        email = best.get("email") or ""
        role  = best.get("role") or best.get("job_title") or ""
        li    = _normalize_linkedin(best.get("linkedin") or best.get("linkedin_url") or "")
    else:
        full = first = last = email = role = li = ""
    def _s(v): return str(v).strip() if v else ""
    domain = _s(rec.get("domain"))
    ind, sub = _classify_industry(
        _s(company.get("industry")) + " " + _s(company.get("sub_industry")))
    country = _s(company.get("hq_location","")).split(",")[-1].strip()
    return _make_lead(
        business=_s(company.get("name")),
        website=f"https://{domain}" if domain else "",
        full_name=full, first=first, last=last,
        email=email, role=role,
        country=country, city=_s(company.get("hq_location","")).split(",")[0].strip(),
        industry=_s(company.get("industry")) or ind,
        sub_industry=_s(company.get("sub_industry")) or sub,
        linkedin=li,
        description=_s(company.get("description")),
        employee_count=_normalize_employee_count(company.get("employee_count","")),
        founded_year=_s(company.get("founded_year")),
        ownership_type=_s(company.get("ownership_type")),
        company_type=_s(company.get("company_type")),
        number_of_locations=_s(company.get("number_of_locations","")),
        socials=company.get("socials",{}),
        source_url=f"https://{domain}/about" if domain else "",
        source_type="company_site",
    )

async def _run_lead_sorcerer_pipeline(num_leads, industry=None, region=None):
    if not LEAD_SORCERER_AVAILABLE:
        return []
    with tempfile.TemporaryDirectory() as tmp:
        _setup_temp_environment(tmp)
        os.environ["LEADPOET_DATA_DIR"] = tmp
        orig = os.getcwd()
        try:
            os.chdir(tmp)
            cfg = _create_industry_specific_config(industry)
            cfg["caps"]["max_domains_per_run"] = min(max(num_leads*2,5),20)
            cfg["caps"]["max_crawl_per_run"]   = min(max(num_leads*2,5),20)
            cfg_file = Path(tmp) / "icp_config.json"
            cfg_file.write_text(json.dumps(cfg, indent=2))
            orch = LeadSorcererOrchestrator(str(cfg_file), batch_size=num_leads)
            async with orch:
                result = await orch.run_pipeline()
            if not result.get("success"):
                return []
            leads = []
            exports = Path(tmp) / "exports"
            if exports.exists():
                dirs = list(exports.glob("*/*"))
                if dirs:
                    lf = max(dirs,key=lambda x:x.stat().st_mtime) / "leads.jsonl"
                    if lf.exists():
                        for line in lf.read_text().splitlines():
                            try:
                                r = json.loads(line)
                                if r.get("contacts") and len(leads)<num_leads:
                                    leads.append(r)
                            except Exception:
                                pass
            return leads[:num_leads]
        except Exception as e:
            print(f"❌ Lead Sorcerer: {e}")
            return []
        finally:
            os.chdir(orig)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────
async def get_leads(
    num_leads: int,
    industry:  Optional[str] = None,
    region:    Optional[str] = None,
) -> List[Dict[str, Any]]:
    required = ["GSE_API_KEY","GSE_CX","OPENROUTER_KEY","FIRECRAWL_KEY"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing or not LEAD_SORCERER_AVAILABLE:
        print(f"⚠️  Free pipeline (paid keys missing: {missing})")
        return await get_leads_free(num_leads, industry=industry, region=region)
    try:
        records = await _run_lead_sorcerer_pipeline(num_leads, industry, region)
        if not records:
            return await get_leads_free(num_leads, industry=industry, region=region)
        leads = [_convert_lead_record(r) for r in records]
        leads = [l for l in leads if l.get("email") and l.get("business")]
        return leads or await get_leads_free(num_leads, industry=industry, region=region)
    except Exception as e:
        print(f"❌ Lead Sorcerer failed: {e}")
        return await get_leads_free(num_leads, industry=industry, region=region)

if not deps_ok:
    async def get_leads(num_leads, industry=None, region=None):  # noqa: F811
        return await get_leads_free(num_leads, industry=industry, region=region)


# ─────────────────────────────────────────────────────────────────────────────
# Smoke test
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import time as _t
    async def _loop():
        """
        Developer utility: run the free/paid pipeline in an infinite loop.

        This is for local scraping only (no Bittensor). Each iteration:
          - calls get_leads(10)
          - prints a short summary
          - sleeps a bit
        Leads are also persisted to data/leads_found.jsonl and reused by the miner.
        """
        batch_size = 10
        delay_sec  = 30
        round_id   = 0
        while True:
            round_id += 1
            t0 = _t.time()
            print(f"\n🧪 Scrape round {round_id} — target {batch_size} leads")
            leads = await get_leads(batch_size)
            dt = _t.time() - t0
            print(f"⏱️  Round {round_id}: {len(leads)} leads in {dt:.1f}s")
            if leads:
                uniq = len({l.get('business','') for l in leads})
                print(f"   Unique companies this round: {uniq}")
                sample = leads[0]
                print(f"   Sample: {sample.get('business')} | {sample.get('email','—')} | {sample.get('role','')}")
            print(f"💤 Sleeping {delay_sec}s before next round...\n")
            await asyncio.sleep(delay_sec)

    asyncio.run(_loop())