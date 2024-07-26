#!/bin/bash
#
#  update-flunet.sh
#
#  Update flunet dataset data://explorers/who/latest/flu
#

set -e

start_time=$(date +%s)

echo '--- Update flunet'

cd /home/owid/etl

exit_code_1=0
exit_code_2=0

poetry run python snapshots/who/latest/fluid.py || exit_code_1=$?
poetry run python snapshots/who/latest/flunet.py || exit_code_2=$?

if [ $exit_code_1 -eq 0 ] && [ $exit_code_2 -eq 0 ]
then
    # commit to master will trigger ETL which is gonna run the step
    echo '--- Commit and push changes'

    git add .
    git commit -m ":robot: automatic flunet update" || true
    git push origin master -q || true
else
    echo "At least one of the Python scripts returned a non-zero exit code. Resetting all files..."
    git reset --hard HEAD
    exit 1
fi

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
