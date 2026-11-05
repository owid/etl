#!/bin/bash
#
#  update-flunet.sh
#
#  Update flunet dataset data://explorers/who/latest/flu
#

set -e

source "$(dirname "$0")/commit-snapshots.sh"

start_time=$(date +%s)

echo '--- Update flunet'

cd /home/owid/etl

# Files this job owns: the only ones it commits, or reverts when a download fails.
# commit_and_push_snapshots refuses to push a change to anything else.
snapshot_files=(
    snapshots/who/latest/fluid.csv.dvc
    snapshots/who/latest/flunet.csv.dvc
)

exit_code_1=0
exit_code_2=0

uv run etls who/latest/fluid || exit_code_1=$?
uv run etls who/latest/flunet || exit_code_2=$?

if [ $exit_code_1 -eq 0 ] && [ $exit_code_2 -eq 0 ]
then
    # commit to master will trigger ETL which is gonna run the step
    echo '--- Commit and push changes'

    commit_and_push_snapshots ":robot: automatic flunet update" "${snapshot_files[@]}"
else
    echo "At least one of the Python scripts returned a non-zero exit code. Reverting our files..."
    git checkout -- "${snapshot_files[@]}"
    exit 1
fi

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
