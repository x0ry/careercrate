import requests, threading, json, random, time, re, os, gzip, argparse, html
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
from urllib.parse import unquote
from geolocation import build_lookup, lookup_location
from requests.adapters import HTTPAdapter

# ============================================================
# CONFIGURATION
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

# This tool only ever surfaces Nashville, TN or fully-remote jobs (see
# is_in_scope() below), so the per-company ATS platforms scan a small,
# individually-verified list of Nashville-connected companies instead of
# the full ~90k-company harvested lists - scanning those exhaustively to
# find the ~4.5% that are ever in scope was hours of wasted network time
# per run. The full lists (greenhouse_companies.json etc.) are left on
# disk, unused, in case broader scanning is wanted again later.
GREENHOUSE_FILE = os.path.join(ROOT_DIR, "data", "nashville_greenhouse_companies.json")
ASHBY_FILE = os.path.join(ROOT_DIR, "data", "nashville_ashby_companies.json")
BAMBOOHR_FILE = os.path.join(ROOT_DIR, "data", "nashville_bamboohr_companies.json")
WORKDAY_FILE = os.path.join(ROOT_DIR, "data", "nashville_workday_companies.json")
LEVER_FILE = os.path.join(ROOT_DIR, "data", "nashville_lever_companies.json")
ICIMS_FILE = os.path.join(ROOT_DIR, "data", "nashville_icims_companies.json")
PAYLOCITY_FILE = os.path.join(ROOT_DIR, "data", "nashville_paylocity_companies.json")
WORKABLE_FILE = os.path.join(ROOT_DIR, "data", "nashville_workable_companies.json")
SMARTRECRUITERS_FILE = os.path.join(ROOT_DIR, "data", "nashville_smartrecruiters_companies.json")
RECRUITEE_FILE = os.path.join(ROOT_DIR, "data", "nashville_recruitee_companies.json")

LOCATIONS_FILE = os.path.join(ROOT_DIR, "data", "locations.json")


PAGEDATA_RE = re.compile(r"window\.pageData\s*=\s*(\{.*?\});\s*</script>", re.DOTALL)

# guid -> company name, filled by load_paylocity()
PAYLOCITY_NAMES = {}

# one requests.Session per worker thread, with a capped connection pool
_paylocity_local = threading.local()


def load_paylocity(filepath):
    """Paylocity clean file is [{guid, name, jobs}, ...], not a flat slug list.
    Returns a set of GUIDs and fills PAYLOCITY_NAMES (guid -> name)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            rows = json.load(f)
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return set()
    guids = set()
    for r in rows:
        g = r.get("guid")
        if not g:
            continue
        guids.add(g)
        name = r.get("name") or g
        PAYLOCITY_NAMES[g] = html.unescape(name)
    print(f"Loaded {len(guids):,} Paylocity companies from {filepath}")
    return guids


def _paylocity_session():
    s = getattr(_paylocity_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        _paylocity_local.session = s
    return s


OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# GEOLOCATION LOOKUP (loaded once, shared across all workers)
# ============================================================

print("Loading location lookup from locations.json...")
LOCATION_MAPS = build_lookup(LOCATIONS_FILE)
print(f"  {len(LOCATION_MAPS['city']):,} city-only entries loaded")


def enrich_location(location_str):
    """Resolve a location string to (remote, coords). Safe to call from worker threads."""
    result = lookup_location(location_str, LOCATION_MAPS)
    return result["remote"], result["coords"]


RECRUITER_TERMS = [
    "recruit",
    "recruiting",
    "recruiter",
    "staffing",
    "staff",
    "talent",
    "talenthub",
    "talentgroup",
    "solutions",
    "consulting",
    "placement",
    "search",
    "resources",
    "agency",
]

USER_AGENTS = [
    # Chrome 144 - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Chrome 144 - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Chrome 144 - Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Firefox 147 - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    # Firefox 147 - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0",
    # Firefox 147 - Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    # Safari 26 - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15",
    # Edge 144 - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]

# ============================================================
# LOAD COMPANIES
# ============================================================


def load_companies(filepath):
    """Load companies from JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            companies = set(json.load(f))
        print(f"Loaded {len(companies):,} companies from {filepath}")
        return companies
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return set()


# ============================================================
# VERIFY ACTIVE JOBS + FETCH ALL JOBS
# ============================================================

# API requests for testing in browser console
"""
fetch("https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({
    operationName: "ApiJobBoardWithTeams",
    variables: {organizationHostedJobsPageName: "zip"},
    query: "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }"
  })
}).then(r => r.json()).then(console.log)

fetch("https://{slug}.bamboohr.com/careers/list"){
    method: "GET",
    headers: {"Content-Type": "application/json"},
}.then(r => r.json()).then(console.log)

}
"""

SOURCE_TYPE = "automated"


def get_job_metadata():
    """Generate consistent metadata for each job."""
    return {
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": SOURCE_TYPE,
    }


def fetch_company_jobs_greenhouse(slug):
    """Fetch all jobs for a company."""
    try:
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            data = response.json()
            jobs = data.get("jobs", [])

            if jobs:
                # Normalize job structure for frontend
                normalized = []
                for job in jobs:
                    location = job.get("location", {}).get("name", "Not specified")
                    remote, coords = enrich_location(location)
                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("title"),
                            "location": location,
                            "remote": remote,
                            "coords": coords,
                            "url": job.get("absolute_url"),
                            "absolute_url": job.get("absolute_url"),
                            "departments": [
                                d.get("name") for d in job.get("departments", [])
                            ],
                            "id": job.get("id"),
                            "updated_at": job.get("updated_at"),
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "Greenhouse",
                            "skill_level": job_tier_classification(
                                job.get("title", "")
                            ),
                            **get_job_metadata(),
                        }
                    )

                return slug, normalized, response.status_code

        return slug, [], response.status_code  # got a response, just not 200

    except Exception as e:
        print(f"Error fetching Greenhouse for {slug}: {e}")
    return slug, [], None


def fetch_company_jobs_ashby(slug):
    try:
        url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
        payload = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": slug},
            "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }",
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }

        # Jitter before request to spread out concurrent workers
        time.sleep(random.uniform(0.5, 2.0))

        max_retries = 2
        for attempt in range(max_retries + 1):
            response = requests.post(url, json=payload, headers=headers, timeout=30)

            if response.status_code == 200:
                break
            elif response.status_code in (429, 503, 502):
                if attempt < max_retries:
                    backoff = (2**attempt) + random.uniform(0.5, 1.5)
                    print(
                        f"  Ashby {slug}: {response.status_code}, retrying in {backoff:.1f}s"
                    )
                    time.sleep(backoff)
                    headers["User-Agent"] = random.choice(USER_AGENTS)
                    continue
            # Non-retryable status
            return slug, [], response.status_code

        if response.status_code != 200:
            return slug, [], response.status_code

        data = response.json()
        jobs = (data.get("data") or {}).get("jobBoard") or {}
        jobs = jobs.get("jobPostings") or []

        if jobs:
            normalized = []
            for job in jobs:
                normalized.append(
                    {
                        "company": slug,
                        "company_slug": slug,
                        "title": job.get("title", ""),
                        "location": job.get("locationName", "Not specified")[:50],
                        "url": f"https://jobs.ashbyhq.com/{slug}/{job.get('id')}",
                        "is_recruiter": is_recruiter_company(slug),
                        "ats": "Ashby",
                        "skill_level": job_tier_classification(job.get("title", "")),
                        **get_job_metadata(),
                    }
                )
            return slug, normalized, response.status_code

        return slug, [], response.status_code  # got a response, just not 200

    except Exception as e:
        print(f"Error fetching Ashby for {slug}: {e}")
    return slug, [], None


def fetch_company_jobs_bamboohr(slug):
    """https://{slug}.bamboohr.com/careers
    https://{slug}.bamboohr.com/careers/list

    """
    url = f"https://{slug}.bamboohr.com/careers/list"

    time.sleep(random.uniform(0.5, 2.0))

    max_retries = 2
    for attempt in range(max_retries + 1):
        headers = {
            "Accept": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }

        try:
            response = requests.get(url, timeout=30, headers=headers)

            if response.status_code == 200:
                if "application/json" not in response.headers.get("Content-Type", ""):
                    return slug, [], 404

                data = response.json()
                jobs = data.get("result", [])

                if jobs:
                    normalized = []
                    for job in jobs:
                        loc = job.get("location") or {}
                        if isinstance(loc, dict):
                            city = loc.get("city", "")
                            state = loc.get("state", "")
                            location = (
                                ", ".join(filter(None, [city, state]))
                                or "Not specified"
                            )
                        else:
                            location = str(loc) if loc else "Not specified"

                        remote, coords = enrich_location(location)
                        normalized.append(
                            {
                                "company": slug,
                                "company_slug": slug,
                                "title": job.get("jobOpeningName"),
                                "location": location[:50],
                                "remote": remote,
                                "coords": coords,
                                "url": f"https://{slug}.bamboohr.com/careers/{job.get('id')}",
                                "is_recruiter": is_recruiter_company(slug),
                                "ats": "BambooHR",
                                "skill_level": job_tier_classification(
                                    job.get("jobOpeningName", "")
                                ),
                                **get_job_metadata(),
                            }
                        )
                    return slug, normalized, response.status_code

                return slug, [], response.status_code

            if response.status_code in (429, 503, 502):
                if attempt < max_retries:
                    backoff = (2**attempt) + random.uniform(0.5, 1.5)
                    time.sleep(backoff)
                    continue

            return slug, [], response.status_code

        except requests.exceptions.SSLError:
            if attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            return slug, [], None
        except Exception as e:
            print(f"Error fetching BambooHR for {slug}: {e}")
            return slug, [], None

    return slug, [], None


def fetch_company_jobs_lever(slug):
    """https://api.lever.co/v0/postings/{slug}"""

    try:
        url = f"https://api.lever.co/v0/postings/{slug}"
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            jobs = response.json()

            if jobs:
                normalized = []
                for job in jobs:
                    categories = job.get("categories", {})
                    location = categories.get("location", "Not specified")[:50]
                    remote, coords = enrich_location(location)
                    normalized.append(
                        {
                            "company": slug,
                            "company_slug": slug,
                            "title": job.get("text"),
                            "location": location,
                            "remote": remote,
                            "coords": coords,
                            "url": job.get("hostedUrl"),
                            "is_recruiter": is_recruiter_company(slug),
                            "ats": "Lever",
                            "skill_level": job_tier_classification(job.get("text", "")),
                            **get_job_metadata(),
                        }
                    )
                return slug, normalized, response.status_code
        return slug, [], response.status_code  # got a response, just not 200
    except Exception as e:
        print(f"Error fetching Lever for {slug}: {e}")
    return slug, [], None


def _parse_workday_posted_on(text):
    """Convert Workday's relative string (e.g. 'Posted 2 Days Ago') to an ISO date."""
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


def fetch_company_jobs_workday(slug):
    """
    slug format: "company|wd#|site_id" e.g. "kohls|wd1|kohlscareers"
    url: https://{company}.wd{num}.myworkdayjobs.com/wday/cxs/{company}/{site_id}/jobs
    """

    try:
        parts = slug.split("|")
        if len(parts) != 3:
            return slug, [], None

        company, wd, site_id = parts
        wd_num = wd.replace("wd", "")

        base_url = f"https://{company}.wd{wd_num}.myworkdayjobs.com"
        api_url = f"{base_url}/wday/cxs/{company}/{site_id}/jobs"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
            "Origin": base_url,
            "Referer": f"{base_url}/{site_id}",
        }

        normalized = []
        offset = 0
        limit = 20
        retries = 0
        max_retries = 2
        observed_total = None

        while True:
            payload = {
                "appliedFacets": {},
                "limit": limit,
                "offset": offset,
                "searchText": "",
            }

            response = requests.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=30,
            )

            if response.status_code != 200:
                if retries < max_retries:
                    retries += 1
                    time.sleep(random.uniform(2.0, 4.0))
                    continue
                break

            data = response.json()
            jobs = data.get("jobPostings", [])
            total = data.get("total", 0)

            # Detect silent blocking / truncation
            if observed_total is None:
                observed_total = total
            elif total != observed_total:
                # Workday sometimes lies mid-pagination when blocking
                break

            if not jobs:
                break

            for job in jobs:
                job_path = job.get("externalPath", "")
                location = (job.get("locationsText") or "Not specified")[:50]
                remote, coords = enrich_location(location)
                normalized.append(
                    {
                        "company": company,
                        "company_slug": slug,
                        "title": job.get("title"),
                        "location": location,
                        "remote": remote,
                        "coords": coords,
                        "url": f"{base_url}/{site_id}{job_path}",
                        "updated_at": _parse_workday_posted_on(job.get("postedOn")),
                        "is_recruiter": is_recruiter_company(company),
                        "ats": "Workday",
                        "skill_level": job_tier_classification(job.get("title", "")),
                        **get_job_metadata(),
                    }
                )

            offset += limit

            if offset >= total:
                break

            # Jitter between pages (critical)
            time.sleep(random.uniform(0.3, 1.0))

        return slug, normalized, response.status_code

    except Exception:
        return slug, [], None


def fetch_company_jobs_icims(slug):
    """
    https://careers-{slug}.icims.com/sitemap.xml

    Sitemap contains job URLs like:
        https://careers-{slug}.icims.com/jobs/9620/financial-service-representative/job

    Title extracted from URL path. Location not available via sitemap. Might look into fetching individual job pages for location,
    but that would be a lot more requests so skipping for now.
    """

    sitemap_url = f"https://careers-{slug}.icims.com/sitemap.xml"
    headers = {
        "Accept": "application/xml",
        "User-Agent": random.choice(USER_AGENTS),
    }

    try:
        resp = requests.get(sitemap_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return slug, [], resp.status_code

        root = ET.fromstring(resp.content)
        ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        normalized = []
        for url_el in root.findall(".//s:url", ns):
            loc_el = url_el.find("s:loc", ns)
            if loc_el is None:
                continue
            job_url = loc_el.text.strip() if loc_el.text else ""
            if (
                not job_url
                or "/jobs/" not in job_url
                or job_url.endswith("/jobs/intro")
            ):
                continue

            path = job_url.split("/jobs/")[-1]
            parts = path.split("/")
            if len(parts) >= 2:
                title = unquote(parts[1]).replace("-", " ").strip().title()
            else:
                continue

            lastmod_el = url_el.find("s:lastmod", ns)
            updated_at = (
                lastmod_el.text.strip()
                if lastmod_el is not None and lastmod_el.text
                else None
            )

            remote, coords = False, None
            normalized.append(
                {
                    "company": slug,
                    "company_slug": slug,
                    "title": title,
                    "location": "Not specified",
                    "remote": remote,
                    "coords": coords,
                    "url": job_url,
                    "updated_at": updated_at,
                    "is_recruiter": is_recruiter_company(slug),
                    "ats": "iCIMS",
                    "skill_level": job_tier_classification(title),
                    **get_job_metadata(),
                }
            )

        return slug, normalized, resp.status_code

    except Exception as e:
        print(f"Error fetching iCIMS for {slug}: {e}")
        return slug, [], None


def fetch_company_jobs_workable(slug):
    """https://apply.workable.com/api/v1/widget/accounts/{slug}
    Public widget API backing Workable's embeddable job list. No auth.
    """
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"

    time.sleep(random.uniform(0.5, 2.0))

    max_retries = 2
    for attempt in range(max_retries + 1):
        headers = {
            "Accept": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }
        try:
            response = requests.get(url, timeout=30, headers=headers)
        except Exception as e:
            print(f"Error fetching Workable for {slug}: {e}")
            return slug, [], None

        if response.status_code == 200:
            break
        if response.status_code in (429, 503, 502):
            if attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
        return slug, [], response.status_code

    try:
        data = response.json()
    except ValueError:
        return slug, [], 200

    company = data.get("name") or slug
    jobs = data.get("jobs") or []

    normalized = []
    for job in jobs:
        city, state, country = job.get("city"), job.get("state"), job.get("country")
        location = ", ".join(filter(None, [city, state, country])) or "Not specified"
        remote, coords = enrich_location(location)
        remote = bool(job.get("telecommuting")) or remote
        normalized.append(
            {
                "company": company,
                "company_slug": slug,
                "title": job.get("title"),
                "location": location[:50],
                "remote": remote,
                "coords": coords,
                "url": job.get("shortlink") or job.get("url"),
                "absolute_url": job.get("shortlink") or job.get("url"),
                "departments": [job.get("department")] if job.get("department") else [],
                "id": job.get("shortcode"),
                "updated_at": job.get("published_on") or job.get("created_at"),
                "is_recruiter": is_recruiter_company(company),
                "ats": "Workable",
                "skill_level": job_tier_classification(job.get("title", "")),
                **get_job_metadata(),
            }
        )

    return slug, normalized, response.status_code


def fetch_company_jobs_smartrecruiters(slug):
    """https://api.smartrecruiters.com/v1/companies/{slug}/postings
    Public Posting API, no auth. Paginated via offset/limit.
    """
    api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    normalized = []
    offset = 0
    limit = 100
    max_retries = 2
    last_status = None

    while True:
        headers = {
            "Accept": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(
                    api_url,
                    params={"offset": offset, "limit": limit},
                    headers=headers,
                    timeout=30,
                )
            except Exception as e:
                print(f"Error fetching SmartRecruiters for {slug}: {e}")
                return slug, normalized, last_status

            last_status = response.status_code
            if response.status_code == 200:
                break
            if response.status_code in (429, 503, 502) and attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            return slug, normalized, response.status_code

        data = response.json()
        content = data.get("content") or []
        total = data.get("totalFound", 0)

        if not content:
            break

        for job in content:
            loc = job.get("location") or {}
            city = loc.get("city")
            region = loc.get("region")
            country = loc.get("country")
            location = ", ".join(filter(None, [city, region, country])) or "Not specified"
            remote, coords = enrich_location(location)
            remote = bool(loc.get("remote")) or remote
            job_id = job.get("id")
            company_name = (job.get("company") or {}).get("name") or slug
            dept = (job.get("department") or {}).get("label")
            normalized.append(
                {
                    "company": company_name,
                    "company_slug": slug,
                    "title": job.get("name"),
                    "location": location[:50],
                    "remote": remote,
                    "coords": coords,
                    "url": f"https://jobs.smartrecruiters.com/{slug}/{job_id}",
                    "absolute_url": f"https://jobs.smartrecruiters.com/{slug}/{job_id}",
                    "departments": [dept] if dept else [],
                    "id": job_id,
                    "updated_at": job.get("releasedDate"),
                    "is_recruiter": is_recruiter_company(company_name),
                    "ats": "SmartRecruiters",
                    "skill_level": job_tier_classification(job.get("name", "")),
                    **get_job_metadata(),
                }
            )

        offset += limit
        if offset >= total:
            break
        time.sleep(random.uniform(0.3, 1.0))

    return slug, normalized, last_status


def fetch_company_jobs_recruitee(slug):
    """https://{slug}.recruitee.com/api/offers/
    Public careers-site API, no auth.
    """
    url = f"https://{slug}.recruitee.com/api/offers/"

    time.sleep(random.uniform(0.5, 2.0))

    max_retries = 2
    for attempt in range(max_retries + 1):
        headers = {
            "Accept": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }
        try:
            response = requests.get(url, timeout=30, headers=headers)
        except Exception as e:
            print(f"Error fetching Recruitee for {slug}: {e}")
            return slug, [], None

        if response.status_code == 200:
            break
        if response.status_code in (429, 503, 502):
            if attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
        return slug, [], response.status_code

    try:
        data = response.json()
    except ValueError:
        return slug, [], 200

    offers = data.get("offers") or []
    normalized = []
    for job in offers:
        city, state, country = (
            job.get("city"),
            job.get("state_code"),
            job.get("country_code"),
        )
        location = ", ".join(filter(None, [city, state, country])) or "Not specified"
        remote, coords = enrich_location(location)
        remote = bool(job.get("remote")) or remote
        job_url = job.get("careers_url") or url
        normalized.append(
            {
                "company": job.get("company_name") or slug,
                "company_slug": slug,
                "title": job.get("title"),
                "location": location[:50],
                "remote": remote,
                "coords": coords,
                "url": job_url,
                "absolute_url": job_url,
                "departments": [job.get("department")] if job.get("department") else [],
                "id": job.get("id"),
                "updated_at": job.get("published_at") or job.get("created_at"),
                "is_recruiter": is_recruiter_company(job.get("company_name") or slug),
                "ats": "Recruitee",
                "skill_level": job_tier_classification(job.get("title", "")),
                **get_job_metadata(),
            }
        )

    return slug, normalized, response.status_code


def _paylocity_location(j):
    """JobLocation carries the real city/state. LocationName is an internal
    label ('Main', 'AVI') and is only a last-resort fallback."""
    loc = j.get("JobLocation") or {}
    city, state = loc.get("City"), loc.get("State")
    if city and state:
        return html.unescape(f"{city}, {state}")
    if city:
        return html.unescape(city)
    return html.unescape(j.get("LocationName") or "Not specified")


def fetch_company_jobs_paylocity(slug):
    """slug is the Paylocity tenant GUID. Jobs come from window.pageData in the
    page HTML, not a JSON API. Name is resolved from PAYLOCITY_NAMES."""
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{slug}/"
    session = _paylocity_session()
    # jitter so concurrent workers don't fire in lockstep
    time.sleep(random.uniform(0.5, 2.0))
    max_retries = 3
    for attempt in range(max_retries + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            response = session.get(url, timeout=30, headers=headers)
        except requests.RequestException as e:
            # reset/abort = throttling. drop the poisoned connection, back off, retry.
            try:
                session.close()
            except Exception:
                pass
            _paylocity_local.session = None
            session = _paylocity_session()
            if attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(1.0, 2.0))
                continue
            print(f"Error fetching Paylocity for {slug}: {e}")
            return slug, [], None
        if response.status_code in (429, 503, 502):
            if attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(1.0, 2.0))
                continue
            return slug, [], response.status_code
        if response.status_code != 200:
            return slug, [], response.status_code
        m = PAGEDATA_RE.search(response.text)
        if not m:
            # page loaded but blob missing = soft block or layout drift.
            # return 200 so it retries next run, NOT cached as dead.
            return slug, [], 200
        try:
            data = json.loads(m.group(1))
        except ValueError:
            return slug, [], 200
        company = PAYLOCITY_NAMES.get(slug, slug)
        normalized = []
        for job in data.get("Jobs") or []:
            job_id = job.get("JobId")
            title = html.unescape(job.get("JobTitle") or "")
            location = _paylocity_location(job)
            inferred_remote, coords = enrich_location(location)
            remote = bool(job.get("IsRemote")) or inferred_remote
            dept = job.get("HiringDepartment")  # almost always null on Paylocity
            if dept:
                dept = html.unescape(dept)
            detail = (
                f"https://recruiting.paylocity.com/recruiting/Jobs/Details/{job_id}"
                if job_id
                else url
            )
            normalized.append(
                {
                    "company": company,
                    "company_slug": slug,  # the GUID
                    "title": title,
                    "location": location,
                    "remote": remote,
                    "coords": coords,
                    "url": detail,
                    "absolute_url": detail,
                    "departments": [dept] if dept else [],
                    "id": job_id,
                    "updated_at": job.get("PublishedDate"),
                    "is_recruiter": is_recruiter_company(company),
                    "ats": "Paylocity",
                    "skill_level": job_tier_classification(title or ""),
                    **get_job_metadata(),
                }
            )
        return slug, normalized, response.status_code
    return slug, [], None


# ============================================================
# AGGREGATOR SOURCES
# ============================================================
# Unlike the ATS platforms above (one API per company, fetched by slug), these
# are single feeds that each return jobs from many companies in one shot. All
# are free, public, and either explicitly encourage third-party use (Hacker
# News) or publish an unauthenticated JSON API meant for exactly this kind of
# reuse (with light attribution asks, satisfied by our own `ats` badge + the
# direct link-through to their `url`).


def _build_salary_estimate(min_v, max_v, currency, period):
    """Build a {p25, median, p75, n} estimate from a source's own stated range.
    n=1 signals "this is the single reported range from the listing itself",
    not an aggregated percentile stat like the internal salary_lookup produces.
    Only trusted when it's a plain annual USD figure - anything else (hourly,
    non-USD) would render misleadingly through the frontend's flat '$Nk' format.
    """
    if currency and currency.upper() != "USD":
        return None
    if period and period.lower() not in ("year", "yearly", "annual", "annually"):
        return None
    lo = min_v or max_v
    hi = max_v or min_v
    if not lo or lo <= 0:
        return None
    return {"p25": lo, "median": round((lo + hi) / 2), "p75": hi, "n": 1}


_HN_NONTITLE_TOKENS = (
    "remote", "onsite", "on-site", "on site", "hybrid",
    "full-time", "full time", "part-time", "part time",
    "contract", "contractor", "freelance", "internship", "intern",
)


def _hn_strip_html(text):
    if not text:
        return ""
    text = re.sub(r"<p>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def _parse_hn_job_comment(comment):
    """Best-effort parse of the community "Company | Title | Location | ..."
    convention used in HN's Who is Hiring threads. Not everyone follows it
    exactly, so entries that don't yield a confident company + title are
    skipped rather than shown with guessed/garbled fields."""
    plain = _hn_strip_html(comment.get("text"))
    header = plain.split("\n", 1)[0].strip()
    if not header:
        return None

    parts = [p.strip() for p in header.split("|") if p.strip()]
    if len(parts) < 2:
        return None

    company = re.sub(r"\s*\(https?://\S+\)\s*", " ", parts[0]).strip()
    if not company:
        return None

    title = None
    for part in parts[1:4]:
        lowered = part.lower()
        if any(tok in lowered for tok in _HN_NONTITLE_TOKENS) or len(part) > 100:
            continue
        title = part
        break
    if not title:
        return None

    location = "Not specified"
    for part in parts[1:]:
        lowered = part.lower()
        if "remote" in lowered or "onsite" in lowered or "hybrid" in lowered or "," in part:
            location = part
            break

    return {"company": company, "title": title, "location": location, "remote": "remote" in header.lower()}


def fetch_source_hn_who_is_hiring():
    """Current-month "Ask HN: Who is hiring?" thread via HN's official, keyless
    Algolia API - YC explicitly built this for third-party use, no ToS risk.
    Postings are free text, not structured JSON, so parsing is best-effort.
    """
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        search_resp = requests.get(
            "https://hn.algolia.com/api/v1/search_by_date",
            params={"tags": "story", "query": "Who is hiring", "restrictSearchableAttributes": "title"},
            headers=headers, timeout=30,
        )
        if search_resp.status_code != 200:
            return []
        hits = search_resp.json().get("hits", [])
        story = next((h for h in hits if (h.get("title") or "").startswith("Ask HN: Who is hiring?")), None)
        if not story:
            return []

        thread_resp = requests.get(
            f"https://hn.algolia.com/api/v1/items/{story['objectID']}", headers=headers, timeout=30
        )
        if thread_resp.status_code != 200:
            return []
        thread = thread_resp.json()

        normalized = []
        for comment in thread.get("children") or []:
            if comment.get("type") != "comment" or not comment.get("author") or not comment.get("text"):
                continue
            parsed = _parse_hn_job_comment(comment)
            if not parsed:
                continue

            inferred_remote, coords = enrich_location(parsed["location"])
            comment_url = f"https://news.ycombinator.com/item?id={comment.get('id')}"
            normalized.append(
                {
                    "company": parsed["company"],
                    "company_slug": parsed["company"].lower().replace(" ", "-"),
                    "title": parsed["title"],
                    "location": parsed["location"][:50],
                    "remote": parsed["remote"] or inferred_remote,
                    "coords": coords,
                    "url": comment_url,
                    "absolute_url": comment_url,
                    "departments": [],
                    "id": comment.get("id"),
                    "updated_at": comment.get("created_at"),
                    "is_recruiter": is_recruiter_company(parsed["company"]),
                    "ats": "HackerNews",
                    "skill_level": job_tier_classification(parsed["title"]),
                    **get_job_metadata(),
                }
            )
        return normalized
    except Exception as e:
        print(f"Error fetching HN Who is Hiring: {e}")
        return []


def fetch_source_arbeitnow():
    """https://www.arbeitnow.com/api/job-board-api - free public API. Terms ask
    only that you don't abuse it and link back to the site."""
    normalized = []
    page = 1
    max_retries = 2

    while page <= 200:  # safety valve
        headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(
                    "https://www.arbeitnow.com/api/job-board-api",
                    params={"page": page}, headers=headers, timeout=30,
                )
            except Exception as e:
                print(f"Error fetching Arbeitnow page {page}: {e}")
                return normalized
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503, 502) and attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            return normalized

        data = resp.json()
        jobs = data.get("data") or []
        if not jobs:
            break

        for job in jobs:
            location = (job.get("location") or "Not specified")[:50]
            inferred_remote, coords = enrich_location(location)
            title = job.get("title", "")
            created_at = job.get("created_at")
            updated_at = (
                datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                if created_at else None
            )
            normalized.append(
                {
                    "company": job.get("company_name"),
                    "company_slug": (job.get("company_name") or "").lower().replace(" ", "-"),
                    "title": title,
                    "location": location,
                    "remote": bool(job.get("remote")) or inferred_remote,
                    "coords": coords,
                    "url": job.get("url"),
                    "absolute_url": job.get("url"),
                    "departments": [],
                    "id": job.get("slug"),
                    "updated_at": updated_at,
                    "is_recruiter": is_recruiter_company(job.get("company_name") or ""),
                    "ats": "Arbeitnow",
                    "skill_level": job_tier_classification(title),
                    **get_job_metadata(),
                }
            )

        if not (data.get("links") or {}).get("next"):
            break
        page += 1
        time.sleep(random.uniform(0.3, 1.0))

    return normalized


def fetch_source_jobicy():
    """https://jobicy.com/api/v2/remote-jobs - free public API, remote-only board.
    Jobicy asks that they be credited and that apply buttons link to the job URL
    given in the feed - both satisfied by our `ats` badge and direct `url` link."""
    try:
        headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(
            "https://jobicy.com/api/v2/remote-jobs", params={"count": 200}, headers=headers, timeout=30
        )
        if resp.status_code != 200:
            return []
        jobs = resp.json().get("jobs") or []

        normalized = []
        for job in jobs:
            location = (job.get("jobGeo") or "Remote")[:50]
            _, coords = enrich_location(location)
            title = job.get("jobTitle", "")
            entry = {
                "company": job.get("companyName"),
                "company_slug": (job.get("companyName") or "").lower().replace(" ", "-"),
                "title": title,
                "location": location,
                "remote": True,
                "coords": coords,
                "url": job.get("url"),
                "absolute_url": job.get("url"),
                "departments": job.get("jobIndustry") or [],
                "id": job.get("id"),
                "updated_at": job.get("pubDate"),
                "is_recruiter": is_recruiter_company(job.get("companyName") or ""),
                "ats": "Jobicy",
                "skill_level": job_tier_classification(title),
                **get_job_metadata(),
            }
            salary = _build_salary_estimate(
                job.get("salaryMin"), job.get("salaryMax"), job.get("salaryCurrency"), job.get("salaryPeriod")
            )
            if salary:
                entry["salary"] = salary
            normalized.append(entry)
        return normalized
    except Exception as e:
        print(f"Error fetching Jobicy: {e}")
        return []


def fetch_source_himalayas(max_pages=100):
    """https://himalayas.app/jobs/api - free public API, remote-focused, cursor
    paginated. Total historical volume is huge (100k+), so each run pulls a
    capped number of most-recent pages rather than the entire archive."""
    normalized = []
    cursor = None
    max_retries = 2

    for _ in range(max_pages):
        params = {"cursor": cursor} if cursor else {}
        headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get("https://himalayas.app/jobs/api", params=params, headers=headers, timeout=30)
            except Exception as e:
                print(f"Error fetching Himalayas: {e}")
                return normalized
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503, 502) and attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            return normalized

        data = resp.json()
        jobs = data.get("jobs") or []
        if not jobs:
            break

        for job in jobs:
            company_slug = job.get("companySlug")
            # A handful of listings omit companyName but still carry a slug -
            # fall back to a title-cased version of that rather than emitting
            # a None company (crashes the frontend sort further downstream).
            company = job.get("companyName") or (
                company_slug.replace("-", " ").title() if company_slug else None
            )
            if not company:
                continue

            locations = job.get("locationRestrictions") or []
            location = (", ".join(locations) or "Remote")[:50]
            _, coords = enrich_location(location)
            title = job.get("title", "")
            pub = job.get("pubDate")
            updated_at = (
                datetime.fromtimestamp(pub, tz=timezone.utc).isoformat().replace("+00:00", "Z") if pub else None
            )
            job_url = job.get("applicationLink") or job.get("guid")
            entry = {
                "company": company,
                "company_slug": company_slug,
                "title": title,
                "location": location,
                "remote": True,
                "coords": coords,
                "url": job_url,
                "absolute_url": job_url,
                "departments": job.get("parentCategories") or [],
                "id": job.get("guid"),
                "updated_at": updated_at,
                "is_recruiter": is_recruiter_company(job.get("companyName") or ""),
                "ats": "Himalayas",
                "skill_level": job_tier_classification(title),
                **get_job_metadata(),
            }
            salary = _build_salary_estimate(
                job.get("minSalary"), job.get("maxSalary"), job.get("currency"), job.get("salaryPeriod")
            )
            if salary:
                entry["salary"] = salary
            normalized.append(entry)

        cursor = data.get("nextCursor")
        if not cursor:
            break
        time.sleep(random.uniform(0.3, 1.0))

    return normalized


def fetch_source_themuse(max_pages=150):
    """https://www.themuse.com/api/public/jobs - free public API, no key required
    (keyless access allows 500 req/hour, far above what one run here uses). The
    catalog is huge (~400k) and not reliably sorted by recency, so each run pulls
    a capped number of pages rather than the whole archive."""
    normalized = []
    max_retries = 2

    for page in range(1, max_pages + 1):
        headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(
                    "https://www.themuse.com/api/public/jobs", params={"page": page}, headers=headers, timeout=30
                )
            except Exception as e:
                print(f"Error fetching The Muse page {page}: {e}")
                return normalized
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503, 502) and attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            return normalized

        data = resp.json()
        results = data.get("results") or []
        if not results:
            break

        for job in results:
            company = (job.get("company") or {}).get("name")
            if not company:
                continue  # a handful of listings omit the company name entirely

            locations = [loc.get("name") for loc in (job.get("locations") or []) if loc.get("name")]
            location = (", ".join(locations) or "Not specified")[:50]
            inferred_remote, coords = enrich_location(location)
            title = job.get("name", "")
            job_url = (job.get("refs") or {}).get("landing_page")
            categories = [c.get("name") for c in (job.get("categories") or []) if c.get("name")]

            normalized.append(
                {
                    "company": company,
                    "company_slug": (job.get("company") or {}).get("short_name"),
                    "title": title,
                    "location": location,
                    "remote": inferred_remote,
                    "coords": coords,
                    "url": job_url,
                    "absolute_url": job_url,
                    "departments": categories,
                    "id": job.get("id"),
                    "updated_at": job.get("publication_date"),
                    "is_recruiter": is_recruiter_company(company or ""),
                    "ats": "TheMuse",
                    "skill_level": job_tier_classification(title),
                    **get_job_metadata(),
                }
            )

        if page >= data.get("page_count", page):
            break
        time.sleep(random.uniform(0.3, 1.0))

    return normalized


def fetch_source_remoteok():
    """https://remoteok.com/api - free public API. Their terms ask for a visible
    backlink/mention as source, satisfied by our `ats` badge + direct `url` link."""
    try:
        headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get("https://remoteok.com/api", headers=headers, timeout=30)
        if resp.status_code != 200:
            return []
        rows = resp.json() or []
        jobs = rows[1:] if rows and "legal" in rows[0] else rows

        normalized = []
        for job in jobs:
            location = (job.get("location") or "").strip(", ") or "Remote"
            location = location[:50]
            _, coords = enrich_location(location)
            title = job.get("position", "")
            entry = {
                "company": job.get("company"),
                "company_slug": (job.get("company") or "").lower().replace(" ", "-"),
                "title": title,
                "location": location,
                "remote": True,
                "coords": coords,
                "url": job.get("url") or job.get("apply_url"),
                "absolute_url": job.get("url") or job.get("apply_url"),
                "departments": job.get("tags") or [],
                "id": job.get("id"),
                "updated_at": job.get("date"),
                "is_recruiter": is_recruiter_company(job.get("company") or ""),
                "ats": "RemoteOK",
                "skill_level": job_tier_classification(title),
                **get_job_metadata(),
            }
            salary_min = job.get("salary_min") or 0
            if salary_min > 0:
                salary = _build_salary_estimate(salary_min, job.get("salary_max") or salary_min, "USD", "yearly")
                if salary:
                    entry["salary"] = salary
            normalized.append(entry)
        return normalized
    except Exception as e:
        print(f"Error fetching RemoteOK: {e}")
        return []


WWR_FEEDS = [
    ("https://weworkremotely.com/categories/remote-programming-jobs.rss", "Programming"),
    ("https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss", "DevOps & Sysadmin"),
    ("https://weworkremotely.com/categories/remote-design-jobs.rss", "Design"),
    ("https://weworkremotely.com/categories/remote-data-jobs.rss", "Data"),
]


def _parse_wwr_pubdate(raw):
    """WWR's RSS pubDate is RFC 2822, unlike every other source in this file."""
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, TypeError):
        return None


def fetch_source_weworkremotely():
    """https://weworkremotely.com/categories/*.rss - public RSS feeds, no auth,
    explicitly designed for this kind of consumption. Titles follow a
    "Company: Job Title" convention; entries that don't match it are skipped
    rather than guessed at (same philosophy as the HN parser). Pulls a fixed
    set of tech-relevant category feeds rather than every WWR category,
    matching this project's existing tech/startup slant; a URL-based dedup
    guards against any cross-category overlap."""
    normalized = []
    seen_urls = set()
    max_retries = 2

    for feed_url, feed_name in WWR_FEEDS:
        headers = {"Accept": "application/rss+xml, application/xml", "User-Agent": random.choice(USER_AGENTS)}
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(feed_url, headers=headers, timeout=30)
            except Exception as e:
                print(f"Error fetching WeWorkRemotely feed {feed_name}: {e}")
                resp = None
                break
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503, 502) and attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            resp = None
            break
        if resp is None:
            continue

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            print(f"Error parsing WeWorkRemotely feed {feed_name}: {e}")
            continue

        for item in root.findall(".//item"):
            title_el = item.find("title")
            raw_title = (title_el.text or "").strip() if title_el is not None else ""
            if ":" not in raw_title:
                continue  # doesn't follow "Company: Title" convention - skip rather than guess
            company, _, title = raw_title.partition(":")
            company, title = company.strip(), title.strip()
            if not company or not title:
                continue

            link_el = item.find("link")
            job_url = (link_el.text or "").strip() if link_el is not None else None
            if not job_url or job_url in seen_urls:
                continue
            seen_urls.add(job_url)

            region_el = item.find("region")
            location = ((region_el.text or "").strip() if region_el is not None else "") or "Remote"
            _, coords = enrich_location(location)

            category_el = item.find("category")
            dept = category_el.text.strip() if category_el is not None and category_el.text else feed_name

            pubdate_el = item.find("pubDate")

            normalized.append(
                {
                    "company": company,
                    "company_slug": company.lower().strip().replace(" ", "-"),
                    "title": title,
                    "location": location[:50],
                    "remote": True,
                    "coords": coords,
                    "url": job_url,
                    "absolute_url": job_url,
                    "departments": [dept],
                    "id": job_url,
                    "updated_at": _parse_wwr_pubdate(pubdate_el.text if pubdate_el is not None else None),
                    "is_recruiter": is_recruiter_company(company),
                    "ats": "WeWorkRemotely",
                    "skill_level": job_tier_classification(title),
                    **get_job_metadata(),
                }
            )

        time.sleep(random.uniform(0.3, 1.0))

    return normalized


def fetch_source_usajobs():
    """https://data.usajobs.gov/api/search - genuinely self-serve free API
    (instant key by email registration, no approval queue), but the only
    source in this file requiring a credential. Requires USAJOBS_API_KEY and
    USAJOBS_USER_AGENT (the email you registered with) as environment
    variables; degrades to a no-op if either is missing so the rest of the
    run isn't affected. Filtered to the federal "Information Technology
    Management" occupational series (2210) - the standard catch-all for
    federal tech roles."""
    api_key = os.environ.get("USAJOBS_API_KEY")
    user_agent = os.environ.get("USAJOBS_USER_AGENT")
    if not api_key or not user_agent:
        print("USAJOBS_API_KEY/USAJOBS_USER_AGENT not set, skipping USAJobs")
        return []

    base_url = "https://data.usajobs.gov/api/search"
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": user_agent,
        "Authorization-Key": api_key,
    }

    normalized = []
    page = 1
    max_retries = 2

    while True:
        params = {"JobCategoryCode": "2210", "ResultsPerPage": 500, "Page": page}
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(base_url, headers=headers, params=params, timeout=30)
            except Exception as e:
                print(f"Error fetching USAJobs page {page}: {e}")
                return normalized
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503, 502) and attempt < max_retries:
                time.sleep((2**attempt) + random.uniform(0.5, 1.5))
                continue
            return normalized

        data = resp.json()
        items = (data.get("SearchResult") or {}).get("SearchResultItems") or []
        if not items:
            break

        for item in items:
            job = item.get("MatchedObjectDescriptor") or {}
            title = job.get("PositionTitle", "")
            org = job.get("OrganizationName") or job.get("DepartmentName") or "U.S. Government"
            locations = job.get("PositionLocation") or []
            location = (locations[0].get("LocationName") if locations else None) or "United States"
            _, coords = enrich_location(location)

            remuneration = job.get("PositionRemuneration") or []
            salary = None
            if remuneration:
                pay = remuneration[0]
                try:
                    lo, hi = float(pay.get("MinimumRange")), float(pay.get("MaximumRange"))
                except (TypeError, ValueError):
                    lo = hi = None
                if lo:
                    salary = _build_salary_estimate(lo, hi, "USD", pay.get("RateIntervalCode", "Per Year"))

            job_url = job.get("PositionURI") or (job.get("ApplyURI") or [None])[0]
            entry = {
                "company": org,
                "company_slug": org.lower().strip().replace(" ", "-"),
                "title": title,
                "location": location[:50],
                "remote": False,
                "coords": coords,
                "url": job_url,
                "absolute_url": job_url,
                "departments": [],
                "id": job.get("PositionID"),
                "updated_at": job.get("PositionStartDate"),
                "is_recruiter": is_recruiter_company(org),
                "ats": "USAJobs",
                "skill_level": job_tier_classification(title),
                **get_job_metadata(),
            }
            if salary:
                entry["salary"] = salary
            normalized.append(entry)

        if len(items) < 500:
            break
        page += 1
        time.sleep(random.uniform(0.3, 1.0))

    return normalized


def run_aggregator_source(fetcher, name):
    """Adapter so single-shot aggregator feeds slot into the same
    (active_companies, jobs) completion loop as the per-company ATS platforms."""
    print("=" * 80)
    print(f"FETCHING JOBS FROM AGGREGATOR SOURCE: {name}")
    print("=" * 80 + "\n")

    jobs = fetcher()
    active = {}
    for job in jobs:
        key = job.get("company_slug") or job.get("company") or "unknown"
        active[key] = active.get(key, 0) + 1

    print(f"  {name}: {len(jobs):,} jobs from {len(active):,} companies/postings")
    return active, jobs


def fetch_all_jobs(companies, fetcher, platform="ATS"):
    """Fetch jobs from all companies in parallel."""
    print("=" * 80)
    print(f"FETCHING JOBS FROM {len(companies):,} COMPANIES FROM PLATFORM: {platform}")
    print("=" * 80 + "\n")

    platform_lower = platform.lower()

    # Skip known dead slugs
    dead_slugs = load_dead_slugs(platform_lower)
    live_companies = [s for s in companies if s not in dead_slugs]
    if dead_slugs:
        print(f"  Skipping {len(dead_slugs):,} known dead slugs")
        print(f"  Checking {len(live_companies):,} potentially active companies\n")

    all_jobs = []
    active_companies = {}
    failed = 0
    new_dead = set()

    MAX_WORKERS = {
        "bamboohr": 10,
        "greenhouse": 30,
        "ashby": 5,
        "lever": 30,
        "workday": 50,
        "icims": 30,
        "paylocity": 5,
        "workable": 20,
        "smartrecruiters": 20,
        "recruitee": 20,
    }

    max_workers = MAX_WORKERS.get(platform_lower, 30)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetcher, slug): slug for slug in live_companies}

        for i, future in enumerate(as_completed(futures), 1):

            # fetcher returns slug, jobs, status_code (if implemented)
            slug, jobs, status_code = future.result()

            if jobs:
                all_jobs.extend(jobs)
                active_companies[slug] = len(jobs)
                print(f"  [{i}/{len(live_companies)}] {slug}: {len(jobs)} jobs")
            else:
                failed += 1
                # Only cache permanent failures
                if status_code in (404, 410):
                    new_dead.add(slug)
                if i % 50 == 0:
                    print(
                        f"  [{i}/{len(live_companies)}] Checked... ({failed} inactive)"
                    )

    # Update dead slug cache
    if new_dead:
        all_dead = dead_slugs | new_dead
        save_dead_slugs(platform_lower, all_dead)

    print(f"\nDETAILED STATS FOR {platform}:")
    print(f"  Companies checked: {len(live_companies)}")
    print(f"  Companies with jobs: {len(active_companies)}")
    print(f"  Failed/empty: {failed}")
    print(f"  Newly dead: {len(new_dead)}")
    print(f"  Total jobs: {len(all_jobs)}")

    return active_companies, all_jobs


# ============================================================
# Helper Functions
# ============================================================
# ============================================================
# TARGET SCOPE - Nashville, TN and fully-remote only
# ============================================================
# A 25,000-job sample of a full unfiltered scrape showed only ~4.4% were
# remote or Nashville-adjacent - the other ~95.6% would be fetched,
# enriched, deduped, chunked, and shipped to the frontend only to never be
# shown to anyone, since that's the only thing this tool is for. Filtering
# here means every downstream step (merge, chunk, IndexedDB cache, initial
# page load) only ever handles jobs that could actually be relevant.


def _location_matches_nashville(location):
    if not location:
        return False
    location = location.lower()
    if "nashville" in location:
        return True
    # Same per-word Levenshtein fallback as the frontend's location filter
    # (js/filters.js fuzzyMatch) and scripts/export_jobs_md.py, so a job that
    # would show up under the site's own Nashville filter never gets
    # discarded here first.
    words = [w for w in re.split(r"\W+", location) if w]
    for word in words:
        max_len = max(len(word), len("nashville"))
        if max_len == 0:
            continue
        distance = levenshtein("nashville", word)
        if 1 - distance / max_len >= 0.75:
            return True
    return False


def levenshtein(a, b):
    dp = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        dp[i][0] = i
    for j in range(len(b) + 1):
        dp[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j - 1], dp[i - 1][j], dp[i][j - 1])
    return dp[len(a)][len(b)]


def is_in_scope(job):
    """This tool only serves Nashville, TN and fully-remote jobs. Everything
    else is discarded here rather than kept and filtered away at display
    time."""
    if job.get("remote") is True:
        return True
    return _location_matches_nashville(job.get("location"))


def is_recruiter_company(slug):
    slug = slug.lower()

    # Keyword-based detection
    if any(term in slug for term in RECRUITER_TERMS):
        return True

    return False


def clean_job_data(jobs):
    """Remove invalid/useless job entries."""
    cleaned = []
    skipped_reasons = {"no_title": 0, "no_url": 0, "no_company": 0}

    for job in jobs:
        title = (job.get("title") or "").strip().lower()
        url = job.get("url") or job.get("absolute_url")
        company = job.get("company") or job.get("company_slug")

        # Skip jobs with invalid titles
        if not title or title in ["not specified", "n/a", "unknown", ""]:
            skipped_reasons["no_title"] += 1
            continue

        # Skip jobs without URLs
        if not url:
            skipped_reasons["no_url"] += 1
            continue

        # Skip jobs without company info
        if not company:
            skipped_reasons["no_company"] += 1
            continue

        cleaned.append(job)

    # Print summary
    total_skipped = sum(skipped_reasons.values())
    if total_skipped > 0:
        print(f"\n  Skipped {total_skipped:,} invalid jobs:")
        for reason, count in skipped_reasons.items():
            if count > 0:
                print(f"    - {reason.replace('_', ' ').title()}: {count:,}")

    return cleaned


# module level, compiled once
_TIER_PATTERNS = [
    (re.compile(r"\b(?:chief|cto|ceo|cfo|vp|vice president|director)\b"), 50),
    (re.compile(r"\b(?:principal|distinguished|fellow)\b"), 40),
    (re.compile(r"\b(?:staff|lead|head of)\b"), 30),
    (re.compile(r"\b(?:senior|sr\.?)\b"), 20),
    (re.compile(r"\b(?:architect|manager)\b"), 15),
    (re.compile(r"\b(?:iii|iv|v|vi)\b"), 15),
    (re.compile(r"\blevel\s*[4-9]\b"), 15),
    (re.compile(r"\bengr?\s*[4-6]\b"), 15),
    (re.compile(r"\b(?:counsel|of\s*counsel)\b"), 20),
    (re.compile(r"\b(?:attending|charge)\b"), 20),
    (re.compile(r"\b(?:ii|2)\b"), 5),
    (re.compile(r"\blevel\s*3\b"), 5),
    (re.compile(r"\b(?:associate)\b"), -10),
    (re.compile(r"\b(?:junior|jr\.?)\b"), -20),
    (re.compile(r"\bentry[\s-]?level\b"), -25),
    (re.compile(r"\b(?:i|1)\b(?!\s*-|\d)"), -15),
    (re.compile(r"\b(?:trainee|graduate|new\s*grad)\b"), -25),
    (re.compile(r"\b(?:paralegal|clerk)\b"), -15),
    (re.compile(r"\b(?:resident|clinical\s*fellow)\b"), -15),
    (re.compile(r"\b(?:aide|assistant|tech)\b"), -10),
    (re.compile(r"\bintern(?:ship)?\b"), -100),
]


def job_tier_classification(title):
    title_lower = title.lower()
    score = 0
    for pattern, weight in _TIER_PATTERNS:
        if pattern.search(title_lower):
            score += weight
    if score <= -50:
        return "intern"
    elif score <= -5:
        return "entry"
    elif score >= 15:
        return "senior"
    else:
        return "mid"


# ============================================================
# DEAD SLUG CACHE
# ============================================================

DEAD_SLUG_DIR = os.path.join(ROOT_DIR, "data", "dead_slugs")
os.makedirs(DEAD_SLUG_DIR, exist_ok=True)


def load_dead_slugs(platform):
    """Load cached dead slugs for a platform."""
    filepath = os.path.join(DEAD_SLUG_DIR, f"{platform}.json")
    if not os.path.exists(filepath):
        return set()
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (json.JSONDecodeError, IOError):
        return set()


def save_dead_slugs(platform, slugs):
    """Save dead slugs for a platform."""
    filepath = os.path.join(DEAD_SLUG_DIR, f"{platform}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(sorted(slugs), f, ensure_ascii=False, indent=2)
    print(f"  Cached {len(slugs):,} dead slugs for {platform}")


# ============================================================
# SAVE RESULTS
# ============================================================
def save_results(all_companies, active_companies, all_jobs):
    """Save all data to JSON files."""
    print("=" * 80)
    print("SAVING RESULTS")
    print("=" * 80 + "\n")

    original_count = len(all_jobs)
    all_jobs = clean_job_data(all_jobs)
    cleaned_count = original_count - len(all_jobs)
    print(f"Removed {cleaned_count:,} invalid jobs (blank/not specified titles)")

    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Save all companies list
    companies_file = os.path.join(OUTPUT_DIR, "all_companies.json")
    with open(companies_file, "w", encoding="utf-8") as f:
        json.dump(sorted(list(all_companies)), f, ensure_ascii=False, indent=2)
    print(f"All companies: {companies_file}")

    # Save active companies with job counts
    active_file = os.path.join(OUTPUT_DIR, "active_companies.json")
    with open(active_file, "w", encoding="utf-8") as f:
        json.dump(active_companies, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"Active companies: {active_file}")

    # Load salary lookup once
    salary_lookup_path = os.path.join(ROOT_DIR, "data", "salary", "salary_lookup.json")
    salary_lookup = {}
    salary_fallback = {}
    if os.path.exists(salary_lookup_path):
        with open(salary_lookup_path, encoding="utf-8") as f:
            data = json.load(f)
            salary_lookup = data.get("primary", {})
            salary_fallback = data.get("fallback", {})
        print(f"Loaded {len(salary_lookup):,} salary entries")

    # Enrich jobs with salary data
    for job in all_jobs:
        company = (job.get("company") or "").lower().strip()
        title = (job.get("title") or "").lower().strip()
        level = job.get("skill_level", "mid")

        primary_key = f"{company}|{title}|{level}"
        fallback_key = f"{title}|{level}"

        # A few sources (Jobicy, Himalayas, RemoteOK) report their own stated
        # salary range at fetch time - don't clobber that with the aggregate
        # lookup, which wouldn't match these listings' company names anyway.
        job["salary"] = job.get("salary") or salary_lookup.get(primary_key) or salary_fallback.get(
            fallback_key
        )

    # Save all jobs
    all_jobs_file = os.path.join(OUTPUT_DIR, "all_jobs.json")
    with open(all_jobs_file, "w", encoding="utf-8") as f:
        json.dump(all_jobs, f, ensure_ascii=False, indent=2)
    print(f"All jobs: {all_jobs_file} ({len(all_jobs):,} jobs)")

    # Build slim version for frontend
    FRONTEND_FIELDS = {
        "title",
        "company",
        "location",
        "url",
        "ats",
        "skill_level",
        "is_recruiter",
        "workplaceType",
        "scraped_at",
        "remote",
        "coords",
        "salary",
        "updated_at",
        "first_seen",
    }

    slim_jobs = [
        {k: job.get(k) for k in FRONTEND_FIELDS if k in job} for job in all_jobs
    ]

    # Pre-sort by company name for better frontend caching. `.get(k, "")`
    # only covers a missing key - a job whose company/title key is present
    # but explicitly None (has happened with The Muse's company.name) still
    # needs the `or ""` to avoid crashing .lower().
    slim_jobs.sort(
        key=lambda x: ((x.get("company") or "").lower(), (x.get("title") or "").lower())
    )

    # Chunks go in a subdirectory to keep the output folder organized
    chunks_dir = os.path.join(OUTPUT_DIR, "chunks")
    os.makedirs(chunks_dir, exist_ok=True)

    # Remove old chunk files to prevent confusion and save space
    for old_chunk in os.listdir(chunks_dir):
        if old_chunk.startswith("jobs_chunk_") and old_chunk.endswith(".json.gz"):
            os.remove(os.path.join(chunks_dir, old_chunk))

    # Split into chunks of ~25k for frontend loading (with gzip compression)
    CHUNK_SIZE = 25_000

    chunks = [
        slim_jobs[i : i + CHUNK_SIZE] for i in range(0, len(slim_jobs), CHUNK_SIZE)
    ]

    chunk_filenames = []
    for idx, chunk in enumerate(chunks):
        chunk_file = os.path.join(chunks_dir, f"jobs_chunk_{idx}.json.gz")
        with gzip.open(chunk_file, "wt", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=0)
        chunk_filenames.append(f"jobs_chunk_{idx}.json.gz")
        size_mb = os.path.getsize(chunk_file) / (1024 * 1024)
        print(f"  Chunk {idx}: {len(chunk):,} jobs ({size_mb:.1f}MB)")

    # Manifest so the frontend knows what to load
    manifest = {
        "chunks": chunk_filenames,
        "totalJobs": len(slim_jobs),
        "last_updated": timestamp,
    }
    manifest_file = os.path.join(chunks_dir, "jobs_manifest.json")
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    recruiter_jobs = sum(1 for job in all_jobs if job.get("is_recruiter"))

    # Save metadata summary
    metadata = {
        "last_updated": timestamp,
        "total_companies": len(all_companies),
        "active_companies": len(active_companies),
        "total_jobs": len(all_jobs),
        "recruiter_jobs": recruiter_jobs,
        "source_type": SOURCE_TYPE,
        "platforms": "greenhouse_api, ashby_api, bamboohr_api, lever_api, workday_api, icims_sitemap, paylocity_scrape, workable_api, smartrecruiters_api, recruitee_api, hackernews_whoishiring, arbeitnow_api, jobicy_api, himalayas_api, themuse_api, remoteok_api, weworkremotely_rss, usajobs_api",
    }

    metadata_file = os.path.join(OUTPUT_DIR, "metadata.json")
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"Metadata: {metadata_file}")

    print()


def main():
    print("\n" + "=" * 80)
    print("JOB BOARD AGGREGATOR")
    print("Scraping all jobs from ATS companies")
    print("=" * 80)

    # Load existing companies
    greenhouse_companies = load_companies(GREENHOUSE_FILE)
    ashby_companies = load_companies(ASHBY_FILE)
    bamboohr_companies = load_companies(BAMBOOHR_FILE)
    lever_companies = load_companies(LEVER_FILE)
    workday_companies = load_companies(WORKDAY_FILE)
    icims_companies = load_companies(ICIMS_FILE)
    paylocity_companies = load_paylocity(PAYLOCITY_FILE)
    workable_companies = load_companies(WORKABLE_FILE)
    smartrecruiters_companies = load_companies(SMARTRECRUITERS_FILE)
    recruitee_companies = load_companies(RECRUITEE_FILE)

    if (
        not greenhouse_companies
        and not ashby_companies
        and not bamboohr_companies
        and not lever_companies
        and not workday_companies
        and not icims_companies
        and not paylocity_companies
        and not workable_companies
        and not smartrecruiters_companies
        and not recruitee_companies
    ):
        print("Exiting - no companies loaded!")
        return

    # Define all platform jobs
    platforms = [
        (greenhouse_companies, fetch_company_jobs_greenhouse, "GREENHOUSE"),
        (ashby_companies, fetch_company_jobs_ashby, "ASHBY"),
        (bamboohr_companies, fetch_company_jobs_bamboohr, "BAMBOOHR"),
        (lever_companies, fetch_company_jobs_lever, "LEVER"),
        (workday_companies, fetch_company_jobs_workday, "WORKDAY"),
        (icims_companies, fetch_company_jobs_icims, "iCIMS"),
        (paylocity_companies, fetch_company_jobs_paylocity, "PAYLOCITY"),
        (workable_companies, fetch_company_jobs_workable, "WORKABLE"),
        (smartrecruiters_companies, fetch_company_jobs_smartrecruiters, "SMARTRECRUITERS"),
        (recruitee_companies, fetch_company_jobs_recruitee, "RECRUITEE"),
    ]

    # Single-shot aggregator feeds - each returns jobs from many companies at
    # once, so they don't take a company-slug list like the ATS platforms do.
    aggregator_sources = [
        (fetch_source_hn_who_is_hiring, "HACKERNEWS"),
        (fetch_source_arbeitnow, "ARBEITNOW"),
        (fetch_source_jobicy, "JOBICY"),
        (fetch_source_himalayas, "HIMALAYAS"),
        (fetch_source_themuse, "THEMUSE"),
        (fetch_source_remoteok, "REMOTEOK"),
        (fetch_source_weworkremotely, "WEWORKREMOTELY"),
        (fetch_source_usajobs, "USAJOBS"),
    ]

    # Run all platforms + aggregator sources concurrently
    all_active_companies = {}
    all_jobs = []

    with ThreadPoolExecutor(max_workers=len(platforms) + len(aggregator_sources)) as platform_executor:
        futures = {
            platform_executor.submit(fetch_all_jobs, companies, fetcher, name): name
            for companies, fetcher, name in platforms
        }
        futures.update(
            {
                platform_executor.submit(run_aggregator_source, fetcher, name): name
                for fetcher, name in aggregator_sources
            }
        )

        for future in as_completed(futures):
            name = futures[future]
            active, jobs = future.result()
            all_active_companies.update(active)
            all_jobs.extend(jobs)
            print(
                f"\n  >>> {name} COMPLETE: {len(active):,} active, {len(jobs):,} jobs <<<\n"
            )

    # Keep only jobs in scope for this tool (Nashville, TN or fully remote) -
    # see is_in_scope() for why this happens here rather than not at all.
    before_filter = len(all_jobs)
    all_jobs = [job for job in all_jobs if is_in_scope(job)]
    pct = f"{len(all_jobs) / before_filter:.1%}" if before_filter else "n/a"
    print(f"\nScope filter: kept {len(all_jobs):,} of {before_filter:,} jobs ({pct}) - Nashville or remote only\n")

    # Recompute active companies from the filtered set - a company with zero
    # in-scope jobs left isn't "active" for this tool's purposes anymore.
    all_active_companies = {}
    for job in all_jobs:
        key = job.get("company_slug") or job.get("company") or "unknown"
        all_active_companies[key] = all_active_companies.get(key, 0) + 1

    # Combine all company sets for total count
    all_companies = (
        greenhouse_companies
        | ashby_companies
        | bamboohr_companies
        | lever_companies
        | workday_companies
        | icims_companies
        | paylocity_companies
        | workable_companies
        | smartrecruiters_companies
        | recruitee_companies
    )
    # Aggregator sources discover their own companies dynamically (no static
    # slug file), so fold whatever they found into the total count too.
    all_companies |= set(all_active_companies.keys())

    save_results(all_companies, all_active_companies, all_jobs)

    # Final summary
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Total companies:   {len(all_companies):,}")
    print(f"Active companies:  {len(all_active_companies):,}")
    print(f"Total jobs:        {len(all_jobs):,}")
    print(f"\nAll data saved to '{OUTPUT_DIR}/' directory")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Job Board Aggregator Scraper")
    parser.add_argument(
        "--source",
        choices=["automated", "manual"],
        default="automated",
        help="Source type: automated (GitHub Actions) or manual (local run)",
    )

    args = parser.parse_args()
    SOURCE_TYPE = args.source

    print(f"\nRunning in {SOURCE_TYPE.upper()} mode\n")

    main()
