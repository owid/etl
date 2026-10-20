#!/bin/bash
#
#  update-measles.sh
#
#  Update measles dataset data://garden/health/latest/measles_long_run
#

set -e

source "$(dirname "$0")/commit-snapshots.sh"

start_time=$(date +%s)

echo '--- Update measles'
cd /home/owid/etl

uv run etls cdc/latest/measles_cases

# Files this job owns. commit_and_push_snapshots refuses to push a change to anything else.
snapshot_files=(
    snapshots/cdc/latest/measles_cases.json.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

commit_and_push_snapshots ":robot: automatic measles update" "${snapshot_files[@]}"

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
