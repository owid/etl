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
uv run etls covid/latest/sequence

# Files this job owns. Several update-*.sh jobs run concurrently against this same
# checkout, so we only ever stage and commit these - `git add .` would sweep in another
# job's half-written snapshots.
snapshot_files=(
    snapshots/covid/latest/sequence.json.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add "${snapshot_files[@]}"
git commit -m ":robot: update: covid-19 sequences" -- "${snapshot_files[@]}" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
