"""
Local, on-demand refresh of this site's Nashville/remote job listings.

Run it whenever you want fresh data:

    python scripts/refresh_jobs.py

No scheduling is wired up on purpose - run it by hand as often as you like.
That's a deliberate choice, not a placeholder for "later": periodic manual
runs are enough for now, and adding a scheduler is a separate decision.

Design, and why it looks the way it does:

- COMPANIES below is a small, hand-verified list. Every entry was checked
  live against its platform's public API before being added - never
  guessed from a company being locally famous. That "famous company"
  shortcut is exactly what broke this project earlier: real Nashville/
  remote hits turned out to be a long tail spread across small, unlikely
  companies, not concentrated in big regional names. Growing this list is
  a deliberate, manual act - verify a company actually has a relevant
  posting, then add it here. There's no auto-discovery, on purpose.

- Every source is a public, unauthenticated, no-signup API: Greenhouse,
  Ashby, SmartRecruiters, BambooHR, and Workday's public job-search
  endpoint. These are the same ATS platforms verified ToS-safe earlier in
  this project - no login, no scraping HTML, no bypassing anything. If a
  future company doesn't expose one of these, it doesn't get added.
  (iCIMS was dropped: its public sitemap carries no location field at all,
  so a job can't be scope-filtered without an extra per-job request per
  posting - not worth the added request volume for one company.)

- Each run REPLACES the dataset outright rather than merging with the
  previous run. No retention windows, no dedup keys - a job that's no
  longer returned by its company's API just isn't in the next output.
  Simpler than merging, and correct by construction.

- One company's API being down doesn't blank the whole run - it's
  skipped and logged, everything else still gets written.
"""

import gzip
import json
import re
from datetime import datetime, timedelta, timezone

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "application/json"}
TIMEOUT = 20

OUTPUT_DIR = "data/chunks"

# ============================================================
# COMPANY LIST - grows only by hand-verifying a new hit, never by guessing.
# ============================================================

GREENHOUSE_COMPANIES = [
    "checkr", "gleanwork", "bpd", "seisandbox",
    "imaginepediatrics", "harrowhealth", "getbuilt",
]
ASHBY_COMPANIES = ["rain"]
SMARTRECRUITERS_COMPANIES = ["mitsubishimotors", "iheartmedia"]
BAMBOOHR_COMPANIES = ["envisionhealth", "ardent", "servpro"]
# "company|wdN|site_id" triples, same convention the old scraper used.
# Workday's API has no friendly company-name field, only the raw tenant
# slug (e.g. "abglobal") - mapped to real names below rather than showing
# that to anyone.
WORKDAY_COMPANY_NAMES = {
    "vumc": "Vanderbilt University Medical Center",
    "alliance": "Nissan",
    "bridgestone": "Bridgestone Americas",
    "asurion": "Asurion",
    "abglobal": "AllianceBernstein",
    "cat": "Caterpillar Financial Services",
    "hcahealthcare": "HCA Healthcare",
}
WORKDAY_COMPANIES = [
    "vumc|wd1|vumccareers",
    "alliance|wd3|nissanjobs",
    "bridgestone|wd5|external",
    "bridgestone|wd5|latamexternalcareers",
    "bridgestone|wd5|wf_external_careers",
    "asurion|wd5|asurioncareers_us",
    "abglobal|wd1|alliancebernsteincareers",
    "cat|wd5|caterpillarcareers",
    "hcahealthcare|wd3|hcacareers",
]


# ============================================================
# SCOPE FILTER - Nashville, TN or fully remote, nothing else
# ============================================================

def _levenshtein(a, b):
    dp = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        dp[i][0] = i
    for j in range(len(b) + 1):
        dp[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            dp[i][j] = dp[i - 1][j - 1] if a[i - 1] == b[j - 1] else 1 + min(
                dp[i - 1][j - 1], dp[i - 1][j], dp[i][j - 1]
            )
    return dp[len(a)][len(b)]


def _is_nashville(location):
    if not location:
        return False
    loc = location.lower()
    if "nashville" in loc:
        return True
    # Same fuzzy fallback as the frontend's own location filter, so nothing
    # the site would match gets dropped here first (typos, odd formatting).
    for word in re.split(r"\W+", loc):
        if not word:
            continue
        max_len = max(len(word), len("nashville"))
        if 1 - _levenshtein("nashville", word) / max_len >= 0.75:
            return True
    return False


def _is_remote(location):
    return "remote" in (location or "").lower()


def in_scope(location):
    return _is_remote(location) or _is_nashville(location)


# ============================================================
# SKILL TIER - lightweight heuristic, same spirit as before
# ============================================================

_TIER_PATTERNS = [
    (re.compile(r"\b(?:chief|cto|ceo|cfo|vp|vice president|director)\b"), 50),
    (re.compile(r"\b(?:principal|distinguished|fellow)\b"), 40),
    (re.compile(r"\b(?:staff|lead|head of)\b"), 30),
    (re.compile(r"\b(?:senior|sr\.?)\b"), 20),
    (re.compile(r"\b(?:architect|manager)\b"), 15),
    (re.compile(r"\b(?:associate)\b"), -10),
    (re.compile(r"\b(?:junior|jr\.?)\b"), -20),
    (re.compile(r"\bentry[\s-]?level\b"), -25),
    (re.compile(r"\bintern(?:ship)?\b"), -100),
]


def skill_level(title):
    score = sum(weight for pat, weight in _TIER_PATTERNS if pat.search((title or "").lower()))
    if score <= -50:
        return "intern"
    if score <= -5:
        return "entry"
    if score >= 15:
        return "senior"
    return "mid"


RECRUITER_TERMS = ("recruit", "staffing", "talent", "consulting", "placement", "agency")


def is_recruiter(name):
    return any(term in (name or "").lower() for term in RECRUITER_TERMS)


def parse_workday_posted_on(text):
    """Workday returns relative strings like "Posted 2 Days Ago" instead of
    a real date - convert to ISO so the frontend's "Posted" column and
    date-posted filter actually work instead of showing "-" for everything."""
    if not text or not isinstance(text, str):
        return None
    t = text.strip().lower()
    today = datetime.now(timezone.utc).date()
    if "today" in t:
        return today.isoformat()
    m = re.search(r"(\d+)\s+day", t)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()
    m = re.search(r"(\d+)\s+week", t)
    if m:
        return (today - timedelta(weeks=int(m.group(1)))).isoformat()
    m = re.search(r"(\d+)\s+month", t)
    if m:
        return (today - timedelta(days=int(m.group(1)) * 30)).isoformat()
    return None


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_job(**kwargs):
    job = {
        "company": None, "company_slug": None, "title": None, "location": None,
        "remote": False, "url": None, "absolute_url": None, "departments": [],
        "id": None, "updated_at": None, "is_recruiter": False, "ats": None,
        "skill_level": "mid", "scraped_at": now_iso(), "source": "manual",
        "first_seen": now_iso(),
    }
    job.update(kwargs)
    return job


# ============================================================
# PER-PLATFORM FETCHERS - one API call per company, no threading needed
# at this scale (~20 companies, seconds total).
# ============================================================

def fetch_greenhouse(slug):
    resp = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    jobs = []
    for j in resp.json().get("jobs", []):
        location = (j.get("location") or {}).get("name", "")
        if not in_scope(location):
            continue
        title = j.get("title", "")
        jobs.append(make_job(
            company=(j.get("company_name") or slug).strip(), company_slug=slug, title=title,
            location=location, remote=_is_remote(location),
            url=j.get("absolute_url"), absolute_url=j.get("absolute_url"),
            id=j.get("id"), updated_at=j.get("updated_at"),
            is_recruiter=is_recruiter(j.get("company_name") or slug),
            ats="Greenhouse", skill_level=skill_level(title),
        ))
    return jobs


def fetch_ashby(slug):
    payload = {
        "operationName": "ApiJobBoardWithTeams",
        "variables": {"organizationHostedJobsPageName": slug},
        "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }",
    }
    resp = requests.post(
        "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams",
        json=payload, headers=HEADERS, timeout=TIMEOUT,
    )
    resp.raise_for_status()
    jb = (resp.json().get("data") or {}).get("jobBoard") or {}
    jobs = []
    for j in jb.get("jobPostings") or []:
        location = j.get("locationName", "")
        if not in_scope(location):
            continue
        title = j.get("title", "")
        url = f"https://jobs.ashbyhq.com/{slug}/{j.get('id')}"
        jobs.append(make_job(
            company=slug, company_slug=slug, title=title, location=location,
            remote=_is_remote(location), url=url, absolute_url=url, id=j.get("id"),
            is_recruiter=is_recruiter(slug), ats="Ashby", skill_level=skill_level(title),
        ))
    return jobs


def fetch_smartrecruiters(slug):
    jobs = []
    offset = 0
    while True:
        resp = requests.get(
            f"https://api.smartrecruiters.com/v1/companies/{slug}/postings",
            params={"offset": offset, "limit": 100}, headers=HEADERS, timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data.get("content") or []
        if not content:
            break
        for j in content:
            loc = j.get("location") or {}
            location = ", ".join(filter(None, [loc.get("city"), loc.get("region"), loc.get("country")]))
            if not in_scope(location) and not loc.get("remote"):
                continue
            title = j.get("name", "")
            company_name = (j.get("company") or {}).get("name") or slug
            url = f"https://jobs.smartrecruiters.com/{slug}/{j.get('id')}"
            jobs.append(make_job(
                company=company_name, company_slug=slug, title=title, location=location or "Remote",
                remote=bool(loc.get("remote")) or _is_remote(location), url=url, absolute_url=url,
                id=j.get("id"), updated_at=j.get("releasedDate"),
                is_recruiter=is_recruiter(company_name), ats="SmartRecruiters", skill_level=skill_level(title),
            ))
        offset += 100
        if offset >= data.get("totalFound", 0):
            break
    return jobs


def fetch_bamboohr(slug):
    resp = requests.get(f"https://{slug}.bamboohr.com/careers/list", headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    if "application/json" not in resp.headers.get("Content-Type", ""):
        return []
    jobs = []
    for j in resp.json().get("result", []):
        loc = j.get("location") or {}
        location = ", ".join(filter(None, [loc.get("city"), loc.get("state")])) if isinstance(loc, dict) else str(loc)
        if not in_scope(location):
            continue
        title = j.get("jobOpeningName", "")
        url = f"https://{slug}.bamboohr.com/careers/{j.get('id')}"
        jobs.append(make_job(
            company=slug, company_slug=slug, title=title, location=location,
            remote=_is_remote(location), url=url, absolute_url=url, id=j.get("id"),
            is_recruiter=is_recruiter(slug), ats="BambooHR", skill_level=skill_level(title),
        ))
    return jobs


def fetch_workday(triple):
    parts = triple.split("|")
    if len(parts) != 3:
        return []
    company, wd, site_id = parts
    company_name = WORKDAY_COMPANY_NAMES.get(company, company)
    wd_num = wd.replace("wd", "")
    base_url = f"https://{company}.wd{wd_num}.myworkdayjobs.com"
    api_url = f"{base_url}/wday/cxs/{company}/{site_id}/jobs"
    headers = {**HEADERS, "Content-Type": "application/json", "Origin": base_url, "Referer": f"{base_url}/{site_id}"}

    jobs = []
    offset = 0
    limit = 20
    while True:
        resp = requests.post(
            api_url, json={"appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""},
            headers=headers, timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        postings = data.get("jobPostings", [])
        if not postings:
            break
        for j in postings:
            location = j.get("locationsText") or ""
            if not in_scope(location):
                continue
            title = j.get("title", "")
            url = f"{base_url}/{site_id}{j.get('externalPath', '')}"
            jobs.append(make_job(
                company=company_name, company_slug=triple, title=title, location=location,
                remote=_is_remote(location), url=url, absolute_url=url,
                updated_at=parse_workday_posted_on(j.get("postedOn")), is_recruiter=is_recruiter(company_name),
                ats="Workday", skill_level=skill_level(title),
            ))
        offset += limit
        if offset >= data.get("total", 0):
            break
    return jobs


SOURCES = [
    (fetch_greenhouse, GREENHOUSE_COMPANIES, "Greenhouse"),
    (fetch_ashby, ASHBY_COMPANIES, "Ashby"),
    (fetch_smartrecruiters, SMARTRECRUITERS_COMPANIES, "SmartRecruiters"),
    (fetch_bamboohr, BAMBOOHR_COMPANIES, "BambooHR"),
    (fetch_workday, WORKDAY_COMPANIES, "Workday"),
]


def main():
    all_jobs = []
    for fetcher, companies, name in SOURCES:
        print(f"{name}: checking {len(companies)} compan{'y' if len(companies) == 1 else 'ies'}...")
        for company in companies:
            try:
                jobs = fetcher(company)
                if jobs:
                    print(f"  {company}: {len(jobs)} in-scope job(s)")
                all_jobs.extend(jobs)
            except Exception as e:
                print(f"  {company}: FAILED ({e}) - skipping, rest of run continues")

    all_jobs.sort(key=lambda j: ((j.get("company") or "").lower(), (j.get("title") or "").lower()))

    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with gzip.open(os.path.join(OUTPUT_DIR, "jobs_chunk_0.json.gz"), "wt", encoding="utf-8") as f:
        json.dump(all_jobs, f, ensure_ascii=False)

    manifest = {
        "chunks": ["jobs_chunk_0.json.gz"],
        "totalJobs": len(all_jobs),
        "last_updated": now_iso(),
    }
    with open(os.path.join(OUTPUT_DIR, "jobs_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    companies_count = len({j["company_slug"] for j in all_jobs})
    print(f"\nWrote {len(all_jobs)} job(s) from {companies_count} compan{'y' if companies_count == 1 else 'ies'} to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
