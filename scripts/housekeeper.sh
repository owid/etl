#!/bin/bash
#
#  housekeeper.sh
#
#  Keep things at OWID clean
#

set -e

start_time=$(date +%s)

echo '--- Keep OWID clean'
cd /home/owid/etl
uv run python snapshots/covid/latest/cases_deaths.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: update: covid-19 cases and deaths" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
