#!/bin/bash
#
# SessionStart hook for Claude Code cloud sessions ("Claude Code on the web").
# Installs the ETL Python environment so `.venv/bin/etl`, `etlr`, `pytest`, etc.
# are available when the session starts.
#
# Notes / gotchas this script guards against:
#   * It must run from the repo root. SessionStart hooks don't guarantee the
#     working directory is the checkout, and $CLAUDE_PROJECT_DIR can be empty,
#     so we cd there if set and otherwise derive the root from this script.
#   * We do NOT use `set -e`, and we always `exit 0`: a SessionStart hook hiccup
#     must never block the session from starting.
#   * The `.venv` must live in the session's own checkout, so it's built here
#     rather than in the environment setup script, which runs before the repo is
#     cloned and instead installs uv and warms its cache. See
#     docs/guides/data-work/claude-code-web.md.
#   * The hook also runs on `resume`, so a rare transient `uv sync` failure at
#     first boot self-heals on the next resume — no in-script retry needed.

START_TIME=$(date +%s)

echo "🚀 Setting up ETL environment for remote session..."
echo "   Started at: $(date '+%Y-%m-%d %H:%M:%S')"

# Only run in cloud sessions. CLAUDE_CODE_REMOTE is set to "true" there.
if [ "$CLAUDE_CODE_REMOTE" != "true" ]; then
  echo "⏭️  Skipping setup (not a remote session)"
  exit 0
fi

# Always operate from the repo root (where pyproject.toml / uv.lock live).
# Prefer $CLAUDE_PROJECT_DIR, but it can be empty depending on how the hook is
# invoked — fall back to deriving the root from this script's own location
# (scripts/remote_setup.sh → repo root is one level up).
if [ -n "$CLAUDE_PROJECT_DIR" ]; then
  cd "$CLAUDE_PROJECT_DIR" || { echo "❌ Could not cd into CLAUDE_PROJECT_DIR ($CLAUDE_PROJECT_DIR)"; exit 0; }
else
  SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
  cd "$SCRIPT_DIR/.." || { echo "❌ Could not cd to repo root from $SCRIPT_DIR"; exit 0; }
fi
if [ ! -f pyproject.toml ]; then
  echo "❌ pyproject.toml not found in $(pwd) — cannot install dependencies."
  echo "   (Expected the repo root. Is this script wired as a SessionStart hook?)"
  exit 0
fi

# --- Make sure uv is new enough ---------------------------------------------
# Below 0.10, uv can't parse `exclude-newer = "3 days"` in pyproject.toml and
# silently re-resolves the whole lockfile. The environment setup script installs
# a current uv, so this only fires in environments created before it existed.
# (`uv self update` hits GitHub API rate limits here.)
UV_MIN_MINOR=10
echo ""
if uv --version | awk '{split($2, v, "."); exit !(v[1] > 0 || v[2] >= '"$UV_MIN_MINOR"')}'; then
  echo "🔧 uv is current ($(uv --version))"
else
  echo "⬆️  $(uv --version) is too old (need >= 0.$UV_MIN_MINOR) — installing a current uv..."
  if curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1; then
    export PATH="$HOME/.local/bin:$PATH"
    hash -r
    echo "   ✓ now on $(uv --version)"
  else
    echo "   ✗ install failed — uv.lock may pick up a spurious full re-resolve."
    echo "     Check 'git status' before committing."
  fi
fi

# --- Install dependencies (the critical step) -------------------------------
# `uv sync` is what `make .venv` runs underneath, minus the install-hooks layer
# a sandbox doesn't need, and it's idempotent across resumes. Output is captured
# so a failure shows uv's own error rather than a bare "failed".
echo ""
echo "📦 Installing dependencies (uv sync)..."
INSTALL_START=$(date +%s)

SYNC_LOG=$(mktemp)
if uv sync --all-extras --group dev >"$SYNC_LOG" 2>&1; then
  echo "✅ Dependencies installed ($(($(date +%s) - INSTALL_START))s)"
else
  echo "❌ uv sync failed — the session may not have a working .venv:"
  tail -20 "$SYNC_LOG" | sed 's/^/      /'
fi
rm -f "$SYNC_LOG"

# --- Verify -----------------------------------------------------------------
echo ""
echo "🔍 Verifying installation..."
if [ -x .venv/bin/etl ]; then
  echo "   ✓ ETL CLI available ($(.venv/bin/python --version 2>&1))"
else
  echo "   ✗ .venv/bin/etl not found — run 'uv sync --all-extras --group dev' in the session."
fi

# A dirty uv.lock here is never intentional — `uv sync` only rewrites it when it
# decided to re-resolve. Say so loudly, because the diff is easy to commit by
# accident and carries unintended dependency bumps.
if ! git diff --quiet -- uv.lock 2>/dev/null; then
  echo "   ⚠ uv sync modified uv.lock — this is a re-resolve, not a real change."
  echo "     Discard it with 'git checkout -- uv.lock' and don't commit it."
fi

echo ""
echo "✅ Setup complete! Total time: $(($(date +%s) - START_TIME))s"
# Always exit 0: a SessionStart hook should never block the session from starting.
exit 0
