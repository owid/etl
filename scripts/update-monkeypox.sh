#!/bin/bash

# This update has been discontinued.
# Global.health discontinued the mpox-2024 line list (last report 2024-12-20) and its download
# endpoint (https://mpox-2024.s3.eu-central-1.amazonaws.com/latest.csv) now returns 403 Access Denied.
# There is no replacement endpoint, so we no longer refresh this snapshot — the frozen historical data
# (supplementary suspected_cases_cumulative) stays cached in R2 and downstream steps keep using it.
# The whole monkeypox update is disabled below; re-enable if a maintained source becomes available.

echo '--- Monkeypox update is DISCONTINUED: Global.health mpox-2024 source returns 403 (no replacement). Skipping.'
exit 0

# set -e

# start_time=$(date +%s)

# echo '--- Update Monkeypox'
# cd /home/owid/etl
# uv run etls who/latest/monkeypox
# uv run etls health/latest/global_health_mpox

# # commit to master will trigger ETL which is gonna run the step
# echo '--- Commit and push changes'

# git add .
# git commit -m ":robot: update: monkeypox" || true
# git push origin master -q || true

# echo '--- Commit dataset to https://github.com/owid/monkeypox'
# MONKEYPOX_COMMIT=1 uv run etlr github/who/latest/monkeypox --export --private

# end_time=$(date +%s)

# echo "--- Done! ($(($end_time - $start_time))s)"
