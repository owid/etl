#!/bin/bash
#
#  update-covid-vaccinations.sh
#
#  Update COVID-19 vaccinations dataset data://grapher/covid/latest/vaccinations_global
#

set -e

source "$(dirname "$0")/commit-snapshots.sh"

start_time=$(date +%s)

echo '--- Update COVID-19 vaccinations'
cd /home/owid/etl
uv run etls covid/latest/vaccinations_global

# Files this job owns. commit_and_push_snapshots refuses to push a change to anything else.
# This excludes vaccinations_global.csv.dvc, which the same snapshot script only writes when
# given --path-to-file by hand.
snapshot_files=(
    snapshots/covid/latest/vaccinations_global_who.csv.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

commit_and_push_snapshots ":robot: update: covid-19 vaccinations" "${snapshot_files[@]}"

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
