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
uv run etls covid/latest/cases_deaths

# Files this job owns. Several update-*.sh jobs run concurrently against this same
# checkout, so we only ever stage and commit these - `git add .` would sweep in another
# job's half-written snapshots.
snapshot_files=(
    snapshots/covid/latest/cases_deaths.csv.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add "${snapshot_files[@]}"
git commit -m ":robot: update: covid-19 cases and deaths" -- "${snapshot_files[@]}" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
