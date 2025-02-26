#!/bin/bash
#
#  update-wildfires.sh
#
#  Update wildfires dataset data://garden/climate/latest/weekly_wildfires
#

set -e

start_time=$(date +%s)

echo '--- Update measles'
cd /home/owid/etl

uv run python snapshots/cdc/latest/measles_cases.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: automatic measles update" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
