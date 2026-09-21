#!/bin/bash
#
#  update-flunet.sh
#
#  Update flunet dataset data://explorers/who/latest/flu
#

set -e

start_time=$(date +%s)

echo '--- Update excess mortality'
cd /home/owid/etl
uv run etls excess_mortality/latest/wmd
uv run etls excess_mortality/latest/xm_karlinsky_kobak
uv run etls excess_mortality/latest/hmd_stmf

# Files this job owns. Several update-*.sh jobs run concurrently against this same
# checkout, so we only ever stage and commit these - `git add .` would sweep in another
# job's half-written snapshots.
snapshot_files=(
    snapshots/excess_mortality/latest/wmd.csv.dvc
    snapshots/excess_mortality/latest/xm_karlinsky_kobak.csv.dvc
    snapshots/excess_mortality/latest/xm_karlinsky_kobak_ages.csv.dvc
    snapshots/excess_mortality/latest/hmd_stmf.csv.dvc
)

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add "${snapshot_files[@]}"
git commit -m ":robot: automatic excess mortality update" -- "${snapshot_files[@]}" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
