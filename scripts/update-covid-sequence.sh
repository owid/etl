#!/bin/bash
#
#  update-covid-sequence.sh
#
#  Update COVID-19 sequence dataset data://grapher/covid/latest/sequence
#

set -e

source "$(dirname "$0")/commit-snapshots.sh"

start_time=$(date +%s)

echo '--- Update COVID-19 sequences'
cd /home/owid/etl
uv run etls covid/latest/sequence

# Files this job owns. commit_and_push_snapshots refuses to push a change to anything else.
snapshot_files=(
    snapshots/covid/latest/sequence.json.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

commit_and_push_snapshots ":robot: update: covid-19 sequences" "${snapshot_files[@]}"

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
