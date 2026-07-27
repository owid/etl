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
#   * The `.venv` must be created in the session's own checkout, so building it
#     belongs here rather than in the cloud environment "setup script" (which
#     runs before the repo is cloned). The setup script still matters: it
#     installs a current uv and warms the uv cache, both of which land in the
#     environment snapshot. See docs/guides/data-work/claude-code-web.md.
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
# The sandbox image has shipped uv 0.8.17, which cannot parse the relative
# `exclude-newer = "3 days"` in pyproject.toml. On that failure uv discards the
# WHOLE [tool.uv] table, concludes the lockfile's cutoff was removed, and
# re-resolves every dependency — a ~2000-line uv.lock diff that silently bumps
# hundreds of packages and can leak into an unrelated PR. uv 0.10 is the first
# release that handles it; anything older re-resolves.
#
# The environment setup script installs a current uv (and the snapshot keeps
# it), so this is a fallback for environments created before that script
# existed. `uv self update` is not used: it needs the GitHub API and hits
# anonymous rate limits in the sandbox.
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
# `uv sync` is exactly what `make .venv` runs underneath, without the extra
# install-hooks / sanity-check layers that aren't needed in a cloud sandbox.
# uv is idempotent, so it's safe to run on every session/resume. Capture output
# so a failure shows WHY (uv's own error) instead of a bare "failed".
#
# UV_HTTP_TIMEOUT is raised from uv's 30s default, which is a *read* timeout
# (30s without receiving a byte), not a transfer timeout. On a cold cache
# --all-extras pulls torch and the CUDA stack — 4.1 GB of linux-only wheels —
# and downloading those in parallel saturates the sandbox's egress proxy until
# some unrelated download stalls out. Which one dies varies from run to run.
echo ""
echo "📦 Installing dependencies (uv sync)..."
INSTALL_START=$(date +%s)

SYNC_LOG=$(mktemp)
if UV_HTTP_TIMEOUT="${UV_HTTP_TIMEOUT:-300}" uv sync --all-extras --group dev >"$SYNC_LOG" 2>&1; then
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
