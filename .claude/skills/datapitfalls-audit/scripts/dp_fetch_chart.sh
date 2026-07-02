#!/usr/bin/env bash
# Download a published OWID grapher chart as a static PNG.
#   dp_fetch_chart.sh SLUG OUTPATH [tab]
# Examples:
#   dp_fetch_chart.sh share-of-population-in-extreme-poverty out/01_map.png
#   dp_fetch_chart.sh share-of-population-in-extreme-poverty out/02_line.png chart
#
# Uses GET (HEAD requests false-404 behind Cloudflare), a browser User-Agent,
# and follows redirects (slugs are often renamed). Verifies the response is a
# real PNG; exits non-zero on 404 / non-image so callers don't scan an HTML/JSON
# error body. The optional [tab] is appended as ?tab=<tab> (e.g. map|chart) —
# note some charts ignore it and always render their configured default tab.
set -uo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: dp_fetch_chart.sh SLUG OUTPATH [tab]" >&2
  exit 2
fi

SLUG="$1"; OUT="$2"; TAB="${3:-}"
URL="https://ourworldindata.org/grapher/${SLUG}.png"
[ -n "$TAB" ] && URL="${URL}?tab=${TAB}"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

mkdir -p "$(dirname "$OUT")"
read -r code ctype size < <(curl -s -A "$UA" -L -o "$OUT" \
  -w "%{http_code} %{content_type} %{size_download}" "$URL" 2>/dev/null)

if [ "$code" = "200" ] && printf '%s' "$ctype" | grep -qi 'image/png'; then
  echo "OK  $SLUG  ${code}  ${size} bytes  -> $OUT${TAB:+  (tab=$TAB)}"
else
  echo "FAIL  $SLUG  http=${code}  type=${ctype}  -> ${URL}" >&2
  rm -f "$OUT"
  exit 1
fi
