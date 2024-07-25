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
poetry run python snapshots/excess_mortality/latest/wmd.py
poetry run python snapshots/excess_mortality/latest/xm_karlinsky_kobak.py
poetry run python snapshots/excess_mortality/latest/hmd_stmf.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: automatic excess mortality update" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
