#!/bin/bash
#
#  refresh.sh
#
#  Refresh the browser (Google Chrome) after updating the docs.
#

macos_refresh() {
    osascript << EOF
tell application "Google Chrome" to repeat with W in windows
    reload (every tab in W whose URL contains "etl/docs")
end repeat
EOF
}

linux_refresh() {
    /usr/bin/xdotool search --onlyvisible --class Chrome windowfocus key ctrl+r
}

if [ -x /usr/bin/osascript ]; then
    macos_refresh

elif [ -x /usr/bin/xdotool ]; then
    linux_refresh
fi
