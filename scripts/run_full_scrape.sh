#!/bin/bash
# One-shot full scrape + merge, meant to be run detached (nohup ... &).
set -e
PYEXE="/c/Users/TRICKY/AppData/Local/Programs/Python/Python312/python.exe"
cd "$(dirname "$0")"
"$PYEXE" scraper.py --source manual
cd ..
"$PYEXE" scripts/merge_data.py
echo "===RUN_COMPLETE==="
"$PYEXE" -c "import json; print('FINAL_TOTAL_JOBS=' + str(json.load(open('data/metadata.json'))['total_jobs']))"
