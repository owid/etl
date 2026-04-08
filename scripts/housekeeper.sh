#!/bin/bash
#
#  housekeeper.sh
#
#  Keep things at OWID clean
#

set -e

# For logging
start_time=$(date +%s)

# Get hour, useful to orchestrate
HOUR=$(TZ=Europe/Berlin date +%H)

# Go to relevant folder
cd /home/owid/etl

# Run commands
# if [ "$HOUR" -eq "06" ]; then
echo "--- Suggesting chart reviews..."
if [ -n "$1" ]; then
    uv run etl d housekeeper --review-type chart --channel "$1"
else
    uv run etl d housekeeper --review-type chart
fi
# fi

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
