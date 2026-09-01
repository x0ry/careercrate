"""
Exports the jobs matching the frontend's default view - title contains "AI",
location fuzzy-matches "Nashville", recruiter-posted jobs hidden - to
./job.md as Markdown, with every field from the source data included per job.

Pulls directly from the live data feed (the same chunks the site itself
loads), so this reflects exactly what a fresh page load of the site shows -
no need to run the scraper locally first.

Mirrors DEFAULT_FILTERS in js/app.js and the matching logic in
js/filters.js's filterJobs(). If those defaults ever change, update the
constants below to match.
"""

import gzip
import json
import os
import re
from datetime import datetime, timezone

import requests

BASE_URL = "https://feashliaa.github.io/job-board-data/data/chunks"
OUTPUT_FILE = "job.md"

DEFAULT_TITLE = "ai"
DEFAULT_LOCATION = "nashville"
HIDE_RECRUITERS = True


def levenshtein(a, b):
    """Same algorithm as filters.js's levenshtein() - kept in lockstep so
    fuzzy_match below behaves identically to the frontend's location match."""
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


def fuzzy_match(search, text, threshold=0.75):
    """Mirrors filters.js's fuzzyMatch(): substring match first, else a
    per-word Levenshtein similarity fallback."""
    if not search:
        return True
    search = search.lower()
    text = (text or "").lower()
    if search in text:
        return True
    words = [w for w in re.split(r"\W+", text) if w]
    for word in words:
        max_len = max(len(word), len(search))
        if max_len == 0:
            continue
        similarity = 1 - levenshtein(search, word) / max_len
        if similarity >= threshold:
            return True
    return False


def matches_default_filters(job):
    if HIDE_RECRUITERS and job.get("is_recruiter") is True:
        return False

    title = (job.get("title") or "").lower()
    if not re.search(rf"\b{re.escape(DEFAULT_TITLE)}\b", title):
        return False

    location = job.get("location") or ""
    if isinstance(location, dict):
        location = location.get("name") or ""
    if not fuzzy_match(DEFAULT_LOCATION, location):
        return False

    return True


def fetch_all_jobs():
    manifest = requests.get(f"{BASE_URL}/jobs_manifest.json", timeout=30).json()
    chunks = manifest["chunks"]
    print(f"Fetching {len(chunks)} chunk(s), {manifest['totalJobs']:,} total jobs...")

    all_jobs = []
    for i, chunk_name in enumerate(chunks, 1):
        resp = requests.get(f"{BASE_URL}/{chunk_name}", timeout=60)
        resp.raise_for_status()
        chunk_jobs = json.loads(gzip.decompress(resp.content))
        all_jobs.extend(chunk_jobs)
        print(f"  [{i}/{len(chunks)}] {chunk_name}: {len(chunk_jobs):,} jobs")

    return all_jobs, manifest


def format_value(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(format_value(v) for v in value) + "]" if value else "[]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k}: {format_value(v)}" for k, v in value.items()) + "}" if value else "{}"
    return str(value)


def load_previously_checked(path):
    """Parse an existing job.md for jobs already marked [x], keyed by URL, so
    re-running the export doesn't wipe out marks made by hand in between runs."""
    if not os.path.exists(path):
        return set()

    checked_urls = set()
    current_checked = False
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            box = re.match(r"- \[([ xX])\]", line)
            if box:
                current_checked = box.group(1).lower() == "x"
                continue
            if current_checked:
                url_line = re.match(r"\s+- \*\*(?:url|absolute_url):\*\* (\S+)", line)
                if url_line:
                    checked_urls.add(url_line.group(1))
    return checked_urls


def write_markdown(jobs, manifest):
    previously_checked = load_previously_checked(OUTPUT_FILE)
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    lines = [
        f'# Jobs — {len(jobs)} matches (title: "{DEFAULT_TITLE}", location: "{DEFAULT_LOCATION}", recruiters hidden)',
        f"_Generated {timestamp} from data last updated {manifest.get('last_updated', 'unknown')}_",
        "",
    ]

    for job in jobs:
        company = job.get("company") or job.get("company_slug") or "Unknown"
        title = job.get("title") or "Untitled"
        url = job.get("url") or job.get("absolute_url") or ""
        mark = "x" if url in previously_checked else " "

        lines.append(f"- [{mark}] **{company}** — {title}")
        for key in sorted(job.keys()):
            lines.append(f"  - **{key}:** {format_value(job[key])}")
        lines.append("")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    carried = sum(1 for job in jobs if (job.get("url") or job.get("absolute_url")) in previously_checked)
    print(f"Wrote {len(jobs):,} matching job(s) to {OUTPUT_FILE} ({carried} previously-checked mark(s) preserved)")


def main():
    all_jobs, manifest = fetch_all_jobs()
    matching = [job for job in all_jobs if matches_default_filters(job)]
    write_markdown(matching, manifest)


if __name__ == "__main__":
    main()
