#!/bin/bash
#
#  update-covid-sequence.sh
#
#  Update COVID-19 sequence dataset data://grapher/covid/latest/sequence
#

set -e

start_time=$(date +%s)

echo '--- Update COVID-19 sequences'
cd /home/owid/etl
poetry run python snapshots/covid/latest/sequence.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: update: covid-19 sequences" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
