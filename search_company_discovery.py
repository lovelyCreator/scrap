"""
search_company_discovery.py
============================
Discovers companies using search engines — NO paid API keys required.

Strategy:
  1. Generate hundreds of targeted queries (industry × role × region)
  2. Search via DuckDuckGo HTML (free, no key) with rotating user-agents
  3. Filter & clean URLs → real company domains
  4. Scrape each domain for contact info
  5. Append results to data/company_list.csv

Usage:
    pip install requests beautifulsoup4 tqdm
    python search_company_discovery.py

Optional env vars (for higher volume):
    SERPAPI_KEY=xxx        # SerpAPI free tier: 100 searches/month
    BING_API_KEY=xxx       # Bing Search API free tier: 1000 searches/month
"""

import csv
import hashlib
import json
import os
import random
import re
import time
import urllib.parse
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

OUTPUT_PATH   = Path("data/company_list.csv")
CACHE_PATH    = Path("data/.search_cache.json")   # avoids re-running same queries
FIELDNAMES    = ["business", "website", "industry", "sub_industry",
                 "country", "state", "city"]

# How many search result URLs to collect per query
RESULTS_PER_QUERY = 8

# Delay range between requests (seconds) — vary to avoid detection
DELAY_MIN = 2.0
DELAY_MAX = 5.0

# Domains to always exclude from results
BLACKLIST_DOMAINS = {
    "linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
    "youtube.com", "tiktok.com", "reddit.com", "wikipedia.org", "wikimedia.org",
    "medium.com", "substack.com", "bloomberg.com", "techcrunch.com",
    "crunchbase.com", "pitchbook.com", "forbes.com", "wired.com",
    "theverge.com", "venturebeat.com", "businessinsider.com", "reuters.com",
    "wsj.com", "nytimes.com", "ft.com", "economist.com",
    "glassdoor.com", "indeed.com", "angel.co", "wellfound.com",
    "ycombinator.com", "producthunt.com", "github.com", "gitlab.com",
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "amazon.com", "microsoft.com", "apple.com", "meta.com",
    "quora.com", "stackoverflow.com", "docs.google.com",
    "play.google.com", "apps.apple.com",
}

# ─────────────────────────────────────────────────────────────────────────────
# QUERY GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

INDUSTRIES = [
    "AI startup", "Fintech startup", "SaaS company", "Cybersecurity startup",
    "Biotech startup", "EdTech startup", "HealthTech startup", "ClimateTech startup",
    "Blockchain startup", "Robotics company", "IoT startup", "Data Analytics company",
    "DevOps startup", "Cloud Computing startup", "MarTech startup", "HR Tech startup",
    "LegalTech startup", "Gaming startup", "Mobile App company", "E-commerce startup",
    "Logistics Tech startup", "PropTech startup", "AgTech startup", "FoodTech startup",
    "InsurTech startup", "DigitalHealth startup", "Autonomous Vehicle startup",
    "AR VR startup", "Digital Payments startup", "Supply Chain startup",
    "CyberSecurity company", "Developer Tools startup", "API company",
    "Infrastructure startup", "B2B SaaS", "Vertical SaaS", "Open Source startup",
]

ROLES = [
    "CEO", "Founder", "Co-founder", "CTO", "Head of Sales",
    "VP Sales", "Chief Revenue Officer", "Managing Director",
    "Head of Marketing", "Business Development",
]

REGIONS = [
    "USA", "UK", "Germany", "France", "Canada", "Australia",
    "Singapore", "Netherlands", "Sweden", "Israel", "India",
    "Japan", "South Korea", "Brazil", "Spain", "Switzerland",
    "Denmark", "Norway", "Finland", "Ireland", "Estonia",
    "UAE", "South Africa", "Poland", "Czech Republic",
    # Cities work better for DuckDuckGo
    "San Francisco", "New York", "London", "Berlin", "Paris",
    "Amsterdam", "Stockholm", "Tel Aviv", "Bangalore", "Toronto",
    "Sydney", "Singapore", "Seoul", "Tokyo", "Dubai",
    "Austin", "Boston", "Chicago", "Seattle", "Miami",
]

# Query templates — varied to avoid duplicate SERP results
QUERY_TEMPLATES = [
    "{industry} {region} contact email",
    "{industry} {region} {role}",
    "{industry} company {region} founder email",
    "{industry} startup {region} team",
    "{industry} {region} about us contact",
    '"{industry}" "{region}" site:linkedin.com/company',  # finds company LinkedIn
    "{industry} company {region} leadership",
    "{industry} startup {role} {region} email",
    "{industry} {region} official website contact",
]


def generate_queries(
    industries: List[str] = None,
    regions: List[str] = None,
    roles: List[str] = None,
    max_queries: int = 5000,
) -> List[Dict]:
    """
    Generate (query_string, metadata) pairs.
    Shuffled so each run explores a different space.
    """
    industries = industries or INDUSTRIES
    regions    = regions    or REGIONS
    roles      = roles      or ROLES

    queries = []
    for ind in industries:
        for reg in regions:
            for tmpl in QUERY_TEMPLATES:
                role = random.choice(roles)
                q = tmpl.format(industry=ind, region=reg, role=role)
                queries.append({
                    "query":    q,
                    "industry": ind.replace(" startup", "").replace(" company", "").strip(),
                    "region":   reg,
                })

    random.shuffle(queries)
    return queries[:max_queries]


# ─────────────────────────────────────────────────────────────────────────────
# USER AGENT ROTATION
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

def get_headers() -> Dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH BACKENDS
# ─────────────────────────────────────────────────────────────────────────────

def _extract_domain(url: str) -> str:
    """Extract base domain from URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        domain = re.sub(r"^www\d*\.", "", domain)
        return domain
    except Exception:
        return ""


def _is_valid_company_url(url: str) -> bool:
    """Return True if URL looks like a real company website."""
    if not url or not url.startswith("http"):
        return False
    domain = _extract_domain(url)
    if not domain:
        return False
    # Reject blacklisted domains
    for bad in BLACKLIST_DOMAINS:
        if bad in domain:
            return False
    # Reject very short or very long domains
    if len(domain) < 4 or len(domain) > 60:
        return False
    # Reject domains with too many subdomains
    if domain.count(".") > 2:
        return False
    return True


# ── Backend 1: DuckDuckGo HTML ────────────────────────────────────────────

def search_duckduckgo(query: str, max_results: int = 8) -> List[str]:
    """
    Scrape DuckDuckGo HTML results page.
    No API key. Rate limit: ~20 req/min safely.
    """
    urls = []
    try:
        params = {"q": query, "kl": "us-en", "kp": "-1"}
        r = requests.get(
            "https://html.duckduckgo.com/html/",
            params=params,
            headers=get_headers(),
            timeout=15,
        )
        if r.status_code != 200:
            return urls

        soup = BeautifulSoup(r.text, "html.parser")

        # DDG result links are in <a class="result__a">
        for a in soup.select("a.result__a")[:max_results * 2]:
            href = a.get("href", "")
            # DDG wraps URLs in redirect — decode them
            if "uddg=" in href:
                href = urllib.parse.unquote(
                    href.split("uddg=")[-1].split("&")[0]
                )
            if _is_valid_company_url(href):
                urls.append(href)
            if len(urls) >= max_results:
                break

    except Exception as e:
        pass  # Silent fail — search is best-effort

    return urls


# ── Backend 2: Bing Web Search API (free tier: 1000/month) ───────────────

def search_bing(query: str, max_results: int = 8) -> List[str]:
    """
    Uses Bing Web Search API free tier.
    Set BING_API_KEY env var to enable (1000 free searches/month).
    """
    api_key = os.getenv("BING_API_KEY")
    if not api_key:
        return []

    urls = []
    try:
        r = requests.get(
            "https://api.bing.microsoft.com/v7.0/search",
            headers={"Ocp-Apim-Subscription-Key": api_key},
            params={"q": query, "count": max_results, "mkt": "en-US"},
            timeout=10,
        )
        data = r.json()
        for item in data.get("webPages", {}).get("value", []):
            url = item.get("url", "")
            if _is_valid_company_url(url):
                urls.append(url)
    except Exception:
        pass

    return urls


# ── Backend 3: SerpAPI (free tier: 100/month) ─────────────────────────────

def search_serpapi(query: str, max_results: int = 8) -> List[str]:
    """
    Uses SerpAPI free tier (100 searches/month).
    Set SERPAPI_KEY env var to enable.
    """
    api_key = os.getenv("SERPAPI_KEY")
    if not api_key:
        return []

    urls = []
    try:
        r = requests.get(
            "https://serpapi.com/search",
            params={
                "q": query,
                "api_key": api_key,
                "engine": "google",
                "num": max_results,
            },
            timeout=15,
        )
        data = r.json()
        for item in data.get("organic_results", []):
            url = item.get("link", "")
            if _is_valid_company_url(url):
                urls.append(url)
    except Exception:
        pass

    return urls


def search_all_backends(query: str, max_results: int = 8) -> List[str]:
    """
    Try all available search backends.
    DDG always runs; Bing/Serpapi only if API keys set.
    Deduplicates results.
    """
    seen: Set[str] = set()
    results: List[str] = []

    for backend in [search_duckduckgo, search_bing, search_serpapi]:
        for url in backend(query, max_results):
            domain = _extract_domain(url)
            if domain and domain not in seen:
                seen.add(domain)
                results.append(url)
        if len(results) >= max_results:
            break

    return results[:max_results]


# ─────────────────────────────────────────────────────────────────────────────
# COMPANY INFO EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def extract_company_name(soup: BeautifulSoup, url: str) -> str:
    """Try several heuristics to get the company name from a webpage."""
    # 1. og:site_name
    og = soup.find("meta", property="og:site_name")
    if og and og.get("content"):
        return og["content"].strip()

    # 2. <title> tag — strip common suffixes
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        for sep in [" | ", " - ", " – ", " — ", " :: ", " · "]:
            if sep in title:
                title = title.split(sep)[0].strip()
        if 2 < len(title) < 60:
            return title

    # 3. Domain name as fallback
    domain = _extract_domain(url)
    name = domain.split(".")[0].replace("-", " ").replace("_", " ").title()
    return name


def extract_location(soup: BeautifulSoup, text: str) -> Tuple[str, str, str]:
    """
    Very simple location extraction from page text.
    Returns (city, state, country).
    """
    country_patterns = {
        "USA":            r"\b(United States|U\.S\.A?|USA)\b",
        "UK":             r"\b(United Kingdom|U\.K\.|England|London|Manchester)\b",
        "Germany":        r"\b(Germany|Deutschland|Berlin|Munich|Hamburg|Frankfurt)\b",
        "France":         r"\b(France|Paris|Lyon|Marseille)\b",
        "Canada":         r"\b(Canada|Toronto|Vancouver|Montreal)\b",
        "Australia":      r"\b(Australia|Sydney|Melbourne|Brisbane)\b",
        "India":          r"\b(India|Bangalore|Mumbai|Delhi|Hyderabad)\b",
        "Singapore":      r"\bSingapore\b",
        "Israel":         r"\b(Israel|Tel Aviv)\b",
        "Netherlands":    r"\b(Netherlands|Amsterdam|Rotterdam)\b",
        "Sweden":         r"\b(Sweden|Stockholm|Gothenburg)\b",
    }
    city_pattern = re.compile(
        r"\b(San Francisco|New York|London|Berlin|Paris|Amsterdam|Stockholm|"
        r"Tel Aviv|Bangalore|Toronto|Sydney|Seoul|Tokyo|Dubai|Austin|Boston|"
        r"Seattle|Chicago|Miami|Los Angeles|Denver|Atlanta|Portland|Nashville)\b",
        re.IGNORECASE,
    )

    country = ""
    city    = ""
    for cname, pat in country_patterns.items():
        if re.search(pat, text, re.IGNORECASE):
            country = cname
            break

    m = city_pattern.search(text)
    if m:
        city = m.group(0)

    return city, "", country


def scrape_company_page(url: str, industry: str = "", region: str = "") -> Optional[Dict]:
    """
    Fetch a company URL and extract structured data.
    Tries homepage + /about + /team + /contact pages.
    """
    base_url = url.rstrip("/")
    pages_to_try = [base_url, f"{base_url}/about", f"{base_url}/about-us",
                    f"{base_url}/team", f"{base_url}/contact"]

    all_text = ""
    soup_main = None

    for page_url in pages_to_try[:3]:   # limit to 3 pages per company
        try:
            r = requests.get(
                page_url,
                headers=get_headers(),
                timeout=12,
                allow_redirects=True,
            )
            if r.status_code != 200:
                continue
            # Only process text/html
            if "text/html" not in r.headers.get("content-type", ""):
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            if soup_main is None:
                soup_main = soup
            # Accumulate text for pattern matching
            page_text = soup.get_text(" ", strip=True)
            all_text += " " + page_text
            time.sleep(random.uniform(0.5, 1.2))
        except Exception:
            continue

    if not soup_main:
        return None

    # ── Company name ──────────────────────────────────────────────────────
    business = extract_company_name(soup_main, url)

    # ── Location ──────────────────────────────────────────────────────────
    city, state, country = extract_location(soup_main, all_text)
    if not country and region:
        country = region  # fall back to search query region

    # ── Sub-industry from description ─────────────────────────────────────
    desc_meta = (
        soup_main.find("meta", {"name": "description"}) or
        soup_main.find("meta", property="og:description")
    )
    description = desc_meta["content"].strip() if desc_meta and desc_meta.get("content") else ""

    return {
        "business":    business,
        "website":     base_url,
        "industry":    industry or "Technology",
        "sub_industry": "",
        "country":     country,
        "state":       state,
        "city":        city,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CACHE  (avoid re-running same queries)
# ─────────────────────────────────────────────────────────────────────────────

def load_cache() -> Dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text())
        except Exception:
            pass
    return {"searched_queries": [], "found_domains": []}


def save_cache(cache: Dict):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=2))


def query_hash(q: str) -> str:
    return hashlib.md5(q.encode()).hexdigest()[:12]


# ─────────────────────────────────────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_existing_domains() -> Set[str]:
    if not OUTPUT_PATH.exists():
        return set()
    domains = set()
    with open(OUTPUT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = _extract_domain(row.get("website", ""))
            if domain:
                domains.add(domain)
    return domains


def append_to_csv(rows: List[Dict]):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = OUTPUT_PATH.exists()
    with open(OUTPUT_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN DISCOVERY LOOP
# ─────────────────────────────────────────────────────────────────────────────

def run_discovery(
    max_queries: int   = 200,    # how many queries to run this session
    max_companies: int = 2000,   # stop after finding this many new companies
    industries: List[str] = None,
    regions: List[str]    = None,
    scrape_pages: bool    = True, # set False for faster domain-only collection
):
    """
    Main discovery loop.

    1. Generate queries
    2. Search each query
    3. Filter to valid company domains
    4. (Optionally) scrape each company page for enriched data
    5. Append to company_list.csv
    """
    print("\n🔍 Search Engine Company Discovery")
    print(f"   Max queries:    {max_queries}")
    print(f"   Max companies:  {max_companies}")
    print(f"   Scrape pages:   {scrape_pages}")
    print(f"   Output:         {OUTPUT_PATH}\n")

    cache = load_cache()
    already_searched: Set[str] = set(cache.get("searched_queries", []))
    already_found:    Set[str] = set(load_existing_domains())
    already_found.update(cache.get("found_domains", []))

    queries = generate_queries(
        industries=industries,
        regions=regions,
        max_queries=max_queries * 3,   # generate extra, we'll skip cached ones
    )

    new_companies: List[Dict] = []
    queries_run = 0

    for q_item in queries:
        if queries_run >= max_queries:
            break
        if len(new_companies) >= max_companies:
            break

        q      = q_item["query"]
        qh     = query_hash(q)
        if qh in already_searched:
            continue   # skip already-run queries

        print(f"🔎 [{queries_run+1}/{max_queries}] {q[:70]}")

        # Search
        urls = search_all_backends(q, max_results=RESULTS_PER_QUERY)
        queries_run += 1
        already_searched.add(qh)

        # Filter to new domains only
        new_urls = []
        for url in urls:
            domain = _extract_domain(url)
            if domain and domain not in already_found:
                already_found.add(domain)
                new_urls.append((url, domain))

        if not new_urls:
            print(f"   ↳ 0 new domains")
        else:
            print(f"   ↳ {len(new_urls)} new domain(s): {', '.join(d for _,d in new_urls[:4])}")

        # Scrape or create minimal records
        batch = []
        for url, domain in new_urls:
            if scrape_pages:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                company = scrape_company_page(
                    url,
                    industry=q_item["industry"],
                    region=q_item["region"],
                )
                if company and company.get("business"):
                    batch.append(company)
                    print(f"   ✅ {company['business']} ({domain})")
                else:
                    # Minimal fallback record
                    batch.append({
                        "business":    domain.split(".")[0].title(),
                        "website":     url,
                        "industry":    q_item["industry"],
                        "sub_industry": "",
                        "country":     q_item["region"],
                        "state":       "",
                        "city":        "",
                    })
            else:
                # Fast mode — no page scraping
                batch.append({
                    "business":    domain.split(".")[0].title(),
                    "website":     f"https://{domain}",
                    "industry":    q_item["industry"],
                    "sub_industry": "",
                    "country":     q_item["region"],
                    "state":       "",
                    "city":        "",
                })

        # Write batch to CSV immediately (survive crashes)
        if batch:
            append_to_csv(batch)
            new_companies.extend(batch)

        # Save cache every 10 queries
        if queries_run % 10 == 0:
            cache["searched_queries"] = list(already_searched)
            cache["found_domains"]    = list(already_found)
            save_cache(cache)
            print(f"\n💾 Progress saved — {len(new_companies)} new companies so far\n")

        # Polite delay between searches
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Final cache save
    cache["searched_queries"] = list(already_searched)
    cache["found_domains"]    = list(already_found)
    save_cache(cache)

    total_in_file = sum(1 for _ in open(OUTPUT_PATH)) - 1 if OUTPUT_PATH.exists() else 0
    print(f"\n✅ Discovery complete!")
    print(f"   New companies found this run: {len(new_companies)}")
    print(f"   Total in company_list.csv:    {total_in_file}")
    print(f"   Queries run:                  {queries_run}")
    print(f"   Cache saved to:               {CACHE_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Search-engine company discovery")
    parser.add_argument("--queries",    type=int,  default=200,
                        help="Number of search queries to run (default: 200)")
    parser.add_argument("--companies",  type=int,  default=2000,
                        help="Stop after finding N new companies (default: 2000)")
    parser.add_argument("--no-scrape",  action="store_true",
                        help="Skip page scraping — faster but less data")
    parser.add_argument("--industry",   type=str,  default=None,
                        help="Focus on a specific industry e.g. 'Fintech'")
    parser.add_argument("--region",     type=str,  default=None,
                        help="Focus on a specific region e.g. 'Germany'")
    args = parser.parse_args()

    industries = [args.industry] if args.industry else None
    regions    = [args.region]   if args.region   else None

    run_discovery(
        max_queries   = args.queries,
        max_companies = args.companies,
        scrape_pages  = not args.no_scrape,
        industries    = industries,
        regions       = regions,
    )