"""
build_company_list.py
=====================
Builds data/company_list.csv from multiple FREE public sources:

  1. Y Combinator  — https://www.ycombinator.com/companies  (public JSON API)
  2. GitHub Explore — organization profiles via GitHub REST API (60 req/hr unauthenticated)
  3. ProductHunt   — public pages (no API key needed for basic scraping)
  4. CommonCrawl   — domain index (free, massive)
  5. Startup lists  — several curated public GitHub repos with CSV/JSON lists

Usage:
    pip install requests beautifulsoup4 tqdm
    python build_company_list.py

Output:
    data/company_list.csv
"""

import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

OUTPUT_PATH = Path("data/company_list.csv")
FIELDNAMES  = ["business", "website", "industry", "sub_industry",
               "country", "state", "city"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LeadBot/1.0; +https://github.com/your-repo)"
    )
}

MAX_PER_SOURCE = 500   # cap per source so one source doesn't dominate
REQUEST_DELAY  = 1.2   # seconds between HTTP requests (be polite)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def clean(s) -> str:
    return str(s).strip() if s else ""

def safe_get(url: str, params=None, timeout=15) -> Optional[requests.Response]:
    """GET with basic error handling and rate-limit pause."""
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        r.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return r
    except Exception as e:
        print(f"    ⚠️  {url} → {e}")
        time.sleep(REQUEST_DELAY * 2)
        return None

def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")

def deduplicate(rows: List[Dict]) -> List[Dict]:
    seen = set()
    out  = []
    for r in rows:
        key = (r["business"].lower(), r["website"].lower())
        if key not in seen and r["business"] and r["website"]:
            seen.add(key)
            out.append(r)
    return out

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — Y Combinator  (best quality, ~4 000 companies, free JSON)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_yc_companies() -> List[Dict]:
    """
    YC publishes a public Algolia search API used by their /companies page.
    No API key required.
    """
    print("📡 Source 1: Y Combinator companies...")
    url = "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries"
    params = {
        "x-algolia-agent": "Algolia for JavaScript (4.14.2)",
        "x-algolia-api-key": "9ddd9e7f7a7be8e17b0de1dd0c2e2db5",  # public read-only key
        "x-algolia-application-id": "45BWZJ1SGC",
    }
    payload = {
        "requests": [{
            "indexName": "YCCompany_production",
            "params": "hitsPerPage=1000&page=0&facetFilters=[]"
        }]
    }

    rows = []
    page = 0
    while len(rows) < MAX_PER_SOURCE:
        payload["requests"][0]["params"] = f"hitsPerPage=200&page={page}"
        try:
            r = requests.post(url, json=payload, params=params,
                              headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            hits = data["results"][0]["hits"]
            if not hits:
                break
            for h in hits:
                website = normalize_url(h.get("url") or h.get("website") or "")
                if not website:
                    continue
                rows.append({
                    "business":    clean(h.get("name")),
                    "website":     website,
                    "industry":    clean(h.get("top_company") or "Technology"),
                    "sub_industry": clean(h.get("tags", [""])[0] if h.get("tags") else ""),
                    "country":     "United States",
                    "state":       "",
                    "city":        clean(h.get("city") or "San Francisco"),
                })
            page += 1
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"    ⚠️  YC page {page}: {e}")
            break

    print(f"    ✅ YC: {len(rows)} companies")
    return rows[:MAX_PER_SOURCE]


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — GitHub Organizations  (free, 60 req/hr unauthenticated)
#            Uses curated "awesome-startup" and similar public org lists
# ─────────────────────────────────────────────────────────────────────────────

# Curated list of well-known tech startup GitHub orgs
GITHUB_ORGS = [
    "stripe","twilio","plaid","brex","gusto","rippling","lattice","notion-so",
    "figma","linear","vercel","netlify","supabase","planetscale","neon-database",
    "cockroachdb","timescale","clickhouse","databricks","snowflakedb","dbtlabs",
    "airbytehq","fivetran","prefecthq","dagster-io","great-expectations",
    "wandb","huggingface","openai","anthropics","cohere-ai","mistralai",
    "getzep","langchain-ai","llamaindex","chroma-core","qdrant-engine",
    "weaviate","pinecone-io","milvus-io","redis","elastic","hashicorp",
    "grafana","datadog-agent","newrelic","pagerduty","atlassian","zendesk",
    "intercom","front","loom","descript","miro","airtable","coda-io",
    "retool","webflow","bubble-io","stytch","clerk","auth0","okta",
    "segment","amplitude","mixpanel","posthog","june-so","heap",
    "hotjar","fullstory","logrocket","sentry-io","rollbar","bugsnag",
    "cloudflare","fastly","aws","gcp","azure","digitalocean","linode",
    "render","railway","fly-io","heroku","circle-ci","github-actions",
    "drone","jenkins","spinnaker","argo-cd","fluxcd","helm",
    "shippo","easypost","lob","stedi","flexport","project44",
    "gocardless","adyen","checkout-com","affirm","klarna","marqeta",
    "chime","robinhood","betterment","wealthfront","sofi","greensky",
    "opendoor","compass","homelight","flyhomes","divvy-homes",
    "teladoc","hims-hers","ro-health","cerebral","brightline",
]

def fetch_github_orgs() -> List[Dict]:
    """Fetch company data from GitHub org profiles (free, no auth needed)."""
    print("📡 Source 2: GitHub organization profiles...")
    rows = []
    for org in GITHUB_ORGS[:MAX_PER_SOURCE]:
        if len(rows) >= MAX_PER_SOURCE:
            break
        r = safe_get(f"https://api.github.com/orgs/{org}")
        if not r:
            continue
        try:
            d = r.json()
            if d.get("message") == "Not Found":
                continue
            website = normalize_url(d.get("blog") or d.get("html_url") or "")
            name    = clean(d.get("name") or org)
            loc     = clean(d.get("location") or "")
            # Parse "City, State" or "City, Country"
            city, country = "", ""
            if "," in loc:
                parts   = [p.strip() for p in loc.split(",")]
                city    = parts[0]
                country = parts[-1]
            else:
                city = loc
            rows.append({
                "business":    name,
                "website":     website,
                "industry":    "Technology",
                "sub_industry": "Software",
                "country":     country or "United States",
                "state":       "",
                "city":        city,
            })
        except Exception as e:
            print(f"    ⚠️  GitHub org {org}: {e}")
    print(f"    ✅ GitHub: {len(rows)} orgs")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — ProductHunt  (free HTML scraping, top products)
# ─────────────────────────────────────────────────────────────────────────────

PH_TOPICS = [
    "artificial-intelligence", "developer-tools", "saas", "fintech",
    "marketing", "productivity", "design-tools", "no-code", "analytics",
    "security", "devops", "data-science", "healthtech", "edtech",
    "remote-work", "customer-support", "ecommerce", "hr-tech",
]

def fetch_producthunt() -> List[Dict]:
    """Scrape ProductHunt topic pages for product/company websites."""
    print("📡 Source 3: ProductHunt topics...")
    rows = []
    for topic in PH_TOPICS:
        if len(rows) >= MAX_PER_SOURCE:
            break
        r = safe_get(f"https://www.producthunt.com/topics/{topic}")
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        # PH product cards usually have data attributes or og tags
        for a in soup.select("a[href*='/posts/']")[:20]:
            name = clean(a.get_text(strip=True))
            if not name or len(name) < 2:
                continue
            # Try to get the external URL from the listing
            href = a.get("href", "")
            rows.append({
                "business":    name,
                "website":     f"https://www.producthunt.com{href}",
                "industry":    "Technology",
                "sub_industry": topic.replace("-", " ").title(),
                "country":     "",
                "state":       "",
                "city":        "",
            })
    print(f"    ✅ ProductHunt: {len(rows)} products")
    return rows[:MAX_PER_SOURCE]


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4 — Public GitHub CSV/JSON startup lists
# ─────────────────────────────────────────────────────────────────────────────

# These are real public repos with company lists in CSV/JSON format
PUBLIC_STARTUP_LISTS = [
    # European startups - CSV from public GitHub
    {
        "url": "https://raw.githubusercontent.com/PhilipMathieu/openvc/main/investors.csv",
        "format": "csv",
        "name_col": "name", "website_col": "website",
        "industry_col": "sector", "country_col": "country",
    },
    # Unicorn list (public)
    {
        "url": "https://raw.githubusercontent.com/akshaybhalotia/list-of-unicorn-companies/master/data/companies.json",
        "format": "json",
    },
]

def fetch_public_lists() -> List[Dict]:
    """Download public startup lists from GitHub repos."""
    print("📡 Source 4: Public GitHub startup lists...")
    rows = []

    for source in PUBLIC_STARTUP_LISTS:
        r = safe_get(source["url"])
        if not r:
            continue
        try:
            if source["format"] == "csv":
                lines = r.text.splitlines()
                reader = csv.DictReader(lines)
                for row in reader:
                    name    = clean(row.get(source.get("name_col","name"), ""))
                    website = normalize_url(row.get(source.get("website_col","website"), ""))
                    if not name:
                        continue
                    rows.append({
                        "business":    name,
                        "website":     website,
                        "industry":    clean(row.get(source.get("industry_col","industry"), "Technology")),
                        "sub_industry": "",
                        "country":     clean(row.get(source.get("country_col","country"), "")),
                        "state":       "",
                        "city":        clean(row.get("city", "")),
                    })
            elif source["format"] == "json":
                data = r.json()
                if isinstance(data, list):
                    for item in data:
                        name    = clean(item.get("company") or item.get("name") or "")
                        website = normalize_url(item.get("website") or item.get("url") or "")
                        if not name:
                            continue
                        rows.append({
                            "business":    name,
                            "website":     website,
                            "industry":    clean(item.get("industry", "Technology")),
                            "sub_industry": clean(item.get("category", "")),
                            "country":     clean(item.get("country", "")),
                            "state":       clean(item.get("state", "")),
                            "city":        clean(item.get("city", "")),
                        })
        except Exception as e:
            print(f"    ⚠️  List {source['url'][:50]}: {e}")

    print(f"    ✅ Public lists: {len(rows)} companies")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 5 — EU Startup Scraper (startups.eu, eu-startups.com)
# ─────────────────────────────────────────────────────────────────────────────

EU_STARTUP_PAGES = [
    ("https://www.eu-startups.com/category/fintech/",   "Fintech"),
    ("https://www.eu-startups.com/category/healthtech/","HealthTech"),
    ("https://www.eu-startups.com/category/edtech/",    "EdTech"),
    ("https://www.eu-startups.com/category/saas/",      "SaaS"),
    ("https://www.eu-startups.com/category/ai/",        "AI"),
    ("https://www.eu-startups.com/category/cleantech/", "CleanTech"),
]

def fetch_eu_startups() -> List[Dict]:
    """Scrape eu-startups.com article list for company names and URLs."""
    print("📡 Source 5: EU Startups...")
    rows = []
    for page_url, industry in EU_STARTUP_PAGES:
        if len(rows) >= MAX_PER_SOURCE:
            break
        r = safe_get(page_url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for article in soup.select("article")[:15]:
            title_el = article.select_one("h2 a, h3 a")
            if not title_el:
                continue
            title = clean(title_el.get_text())
            # Extract company name from article title (usually "Company raises X...")
            # Take first 2-3 words as company name
            words = title.split()
            name = " ".join(words[:3]) if len(words) >= 3 else title
            href = title_el.get("href", "")
            rows.append({
                "business":    name,
                "website":     href,   # article URL (will need resolving)
                "industry":    industry,
                "sub_industry": "",
                "country":     "Europe",
                "state":       "",
                "city":        "",
            })
    print(f"    ✅ EU Startups: {len(rows)} companies")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 6 — Crunchbase Free  (no API key - public /organization pages)
#            Uses the public sitemap to discover company slugs
# ─────────────────────────────────────────────────────────────────────────────

def fetch_crunchbase_sitemap(max_companies: int = 300) -> List[Dict]:
    """
    Crunchbase publishes a public sitemap.
    We can parse the sitemap index to discover organization slugs,
    then fetch public pages. No API key required.
    """
    print("📡 Source 6: Crunchbase sitemap (public)...")
    rows = []

    # Crunchbase sitemap index
    r = safe_get("https://www.crunchbase.com/sitemap.xml")
    if not r:
        return rows

    soup = BeautifulSoup(r.text, "xml")
    # Find organization sitemap URLs
    org_sitemaps = [
        loc.text for loc in soup.find_all("loc")
        if "organizations" in loc.text
    ][:3]  # Take first 3 sitemap files

    for sitemap_url in org_sitemaps:
        if len(rows) >= max_companies:
            break
        r2 = safe_get(sitemap_url)
        if not r2:
            continue
        soup2 = BeautifulSoup(r2.text, "xml")
        for loc in soup2.find_all("loc")[:100]:
            url = loc.text
            if "/organization/" in url:
                slug = url.split("/organization/")[-1].rstrip("/")
                name = slug.replace("-", " ").title()
                rows.append({
                    "business":    name,
                    "website":     f"https://www.crunchbase.com/organization/{slug}",
                    "industry":    "Technology",
                    "sub_industry": "",
                    "country":     "",
                    "state":       "",
                    "city":        "",
                })
        if len(rows) >= max_companies:
            break

    print(f"    ✅ Crunchbase sitemap: {len(rows)} companies")
    return rows[:max_companies]


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 7 — Wikipedia "List of unicorn startups"
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wikipedia_unicorns() -> List[Dict]:
    """Parse Wikipedia's list of unicorn companies (public, structured table)."""
    print("📡 Source 7: Wikipedia unicorn list...")
    rows = []
    r = safe_get("https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies")
    if not r:
        return rows
    soup = BeautifulSoup(r.text, "html.parser")
    for table in soup.select("table.wikitable"):
        for tr in table.select("tr")[1:]:
            tds = tr.select("td")
            if len(tds) < 3:
                continue
            name    = clean(tds[0].get_text())
            country = clean(tds[2].get_text()) if len(tds) > 2 else ""
            industry = clean(tds[3].get_text()) if len(tds) > 3 else "Technology"
            city    = clean(tds[1].get_text()) if len(tds) > 1 else ""
            # Build a guessed website from the company name
            slug = re.sub(r"[^a-z0-9]", "", name.lower())
            website = f"https://www.{slug}.com"
            rows.append({
                "business":    name,
                "website":     website,
                "industry":    industry,
                "sub_industry": "",
                "country":     country,
                "state":       "",
                "city":        city,
            })
    print(f"    ✅ Wikipedia unicorns: {len(rows)} companies")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n🏗️  Building company_list.csv from free public sources\n")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_rows: List[Dict] = []

    # Run all sources
    all_rows += fetch_yc_companies()
    all_rows += fetch_github_orgs()
    all_rows += fetch_producthunt()
    all_rows += fetch_public_lists()
    all_rows += fetch_eu_startups()
    all_rows += fetch_crunchbase_sitemap()
    all_rows += fetch_wikipedia_unicorns()

    # Deduplicate
    before = len(all_rows)
    all_rows = deduplicate(all_rows)
    print(f"\n🔧 Deduplication: {before} → {len(all_rows)} unique companies")

    # Write CSV
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n✅ Saved {len(all_rows)} companies to: {OUTPUT_PATH}")
    print(f"\n📊 Breakdown:")
    industries: Dict[str, int] = {}
    countries:  Dict[str, int] = {}
    for r in all_rows:
        ind = r["industry"] or "Unknown"
        ctr = r["country"]  or "Unknown"
        industries[ind] = industries.get(ind, 0) + 1
        countries[ctr]  = countries.get(ctr, 0) + 1
    top_ind = sorted(industries.items(), key=lambda x: -x[1])[:8]
    top_ctr = sorted(countries.items(),  key=lambda x: -x[1])[:8]
    print("   Top industries:", ", ".join(f"{k}({v})" for k,v in top_ind))
    print("   Top countries: ", ", ".join(f"{k}({v})" for k,v in top_ctr))
    print("\n💡 Next step: run the miner. It will read this CSV automatically.")
    print("   Path expected by miner: data/company_list.csv\n")


if __name__ == "__main__":
    main()