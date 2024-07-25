#!/bin/bash
#
#  update-covid-cases-deaths.sh
#
#  Update COVID-19 cases and deaths dataset data://grapher/covid/latest/cases_deaths
#

set -e

start_time=$(date +%s)

echo '--- Update COVID-19 cases and deaths'
cd /home/owid/etl
poetry run python snapshots/covid/latest/cases_deaths.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: update: covid-19 cases and deaths" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
