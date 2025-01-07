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
cd /home/owid/etl
if [ "$HOUR" -eq "01" ]; then
    uv run etl d housekeeper --review-type chart
fi

end_time=$(date +%s)

echo "--- Done! ($(($end_time - $start_time))s)"
