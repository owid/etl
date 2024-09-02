#!/bin/bash

set -e

start_time=$(date +%s)

echo '--- Update Mpox - Global Health'
cd /home/owid/etl
poetry run python snapshots/health/latest/global_health_mpox.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: update: monkeypox" || true
git push origin master -q || true

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
