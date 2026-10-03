#!/bin/bash
#
#  related-charts.sh
#
#  Refresh "related charts" recommendations from coviews
#

set -e

# For logging
start_time=$(date +%s)

# Go to relevant folder
cd /home/owid/etl

echo "--- Refreshing related charts recommendations..."
uv run etl related-charts

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
