#!/bin/bash
#
#  housekeeper.sh
#
#  Keep things at OWID clean
#

set -e

start_time=$(date +%s)

HOUR=$(TZ=Europe/Berlin date +%H)
echo '--- Keep OWID clean'
# cd /home/owid/etl
if [ "$HOUR" -eq "06" ]; then
    echo "--- Suggesting chart reviews..."
    if [ -n "$1" ]; then
        uv run etl d housekeeper --review-type chart --channel "$1"
    else
        uv run etl d housekeeper --review-type chart
    fi
fi

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
