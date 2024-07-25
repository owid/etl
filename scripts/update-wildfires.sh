#!/bin/bash
#
#  update-wildfires.sh
#
#  Update wildfires dataset data://garden/climate/latest/weekly_wildfires
#

set -e

start_time=$(date +%s)

echo '--- Update wildfires'
cd /home/owid/etl

poetry run python snapshots/climate/latest/weekly_wildfires.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: automatic wildfires update" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
