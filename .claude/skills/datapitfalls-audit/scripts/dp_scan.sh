#!/usr/bin/env bash
# Thin wrapper around the datapitfalls CLI.
#   dp_scan.sh OUTDIR SLUG <scan-args...>
# Examples:
#   dp_scan.sh ai/datapitfalls/raw garden_main path/to/step.py
#   dp_scan.sh ai/datapitfalls/raw charts a.png b.png          # cross-chart
#   dp_scan.sh ai/datapitfalls/raw text_method m.txt --text
#   dp_scan.sh ai/datapitfalls/raw garden_main step.py --thorough
#
# Sources ANTHROPIC_API_KEY from $DP_ENV_FILE (default /Users/parriagadap/etl/.env)
# for this process only; the value is never printed. Honors $ANTHROPIC_MODEL.
# Always runs with --json --all. Writes OUTDIR/SLUG.json and OUTDIR/SLUG.err.
set -uo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: dp_scan.sh OUTDIR SLUG <scan-args...>" >&2
  exit 2
fi

OUTDIR="$1"; SLUG="$2"; shift 2
ENV_FILE="${DP_ENV_FILE:-/Users/parriagadap/etl/.env}"

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  if [ -f "$ENV_FILE" ] && grep -q '^ANTHROPIC_API_KEY=' "$ENV_FILE"; then
    ANTHROPIC_API_KEY=$(grep '^ANTHROPIC_API_KEY=' "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"'"'"' ')
    export ANTHROPIC_API_KEY
  else
    echo "ERROR: ANTHROPIC_API_KEY not set and not found in $ENV_FILE" >&2
    exit 1
  fi
fi

mkdir -p "$OUTDIR"
npx --yes datapitfalls@latest scan "$@" --json --all \
    2> "$OUTDIR/$SLUG.err" > "$OUTDIR/$SLUG.json"
rc=$?

if command -v jq >/dev/null 2>&1 && jq -e . "$OUTDIR/$SLUG.json" >/dev/null 2>&1; then
  n=$(jq '.findings | length' "$OUTDIR/$SLUG.json")
  model=$(jq -r '.model // "?"' "$OUTDIR/$SLUG.json")
  sev=$(jq -c '[.findings[].severity] | group_by(.) | map({(.[0]): length}) | add' "$OUTDIR/$SLUG.json")
  echo "$SLUG  rc=$rc  model=$model  findings=$n  severities=$sev"
else
  echo "$SLUG  rc=$rc  !!! INVALID JSON — see $OUTDIR/$SLUG.err" >&2
  tail -3 "$OUTDIR/$SLUG.err" >&2
  exit 1
fi
