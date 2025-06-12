#!/bin/bash

set -e

start_time=$(date +%s)

echo '--- Update Monkeypox'
cd /home/owid/etl
uv run python snapshots/who/latest/monkeypox.py
uv run python snapshots/health/latest/global_health_mpox.py

# commit to master will trigger ETL which is gonna run the step
echo '--- Commit and push changes'

git add .
git commit -m ":robot: update: monkeypox" || true
git push origin master -q || true

echo '--- Commit dataset to https://github.com/owid/monkeypox'
MONKEYPOX_COMMIT=1 uv run etlr github/who/latest/monkeypox --export --private

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
