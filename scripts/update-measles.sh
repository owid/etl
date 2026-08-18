#!/bin/bash
#
#  update-measles.sh
#
#  Update wildfires dataset data://garden/health/latest/measles_long_run
#

set -e

start_time=$(date +%s)

echo '--- Update measles'
cd /home/owid/etl

uv run etls cdc/latest/measles_cases

# Files this job owns. Several update-*.sh jobs run concurrently against this same
# checkout, so we only ever stage and commit these - `git add .` would sweep in another
# job's half-written snapshots.
snapshot_files=(
    snapshots/cdc/latest/measles_cases.json.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add "${snapshot_files[@]}"
git commit -m ":robot: automatic measles update" -- "${snapshot_files[@]}" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
