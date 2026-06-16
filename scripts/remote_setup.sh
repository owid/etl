#!/bin/bash
#
# SessionStart hook for Claude Code cloud sessions ("Claude Code on the web").
# Installs the ETL Python environment so `.venv/bin/etl`, `etlr`, `pytest`, etc.
# are available when the session starts.
#
# Notes / gotchas this script guards against:
#   * It must run from the repo root. SessionStart hooks don't guarantee the
#     working directory is the checkout, so we cd into $CLAUDE_PROJECT_DIR.
#   * We do NOT use `set -e`. A non-critical step (e.g. installing gh) must
#     never abort the run before the venv is built.
#   * Dependency install belongs in a SessionStart hook, not the cloud
#     environment "setup script" — the setup script runs outside the repo, so
#     `uv sync` / `make .venv` can't find pyproject.toml there.

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

# --- Install dependencies (the critical step) -------------------------------
# `uv sync` is exactly what `make .venv` runs underneath, without the extra
# install-hooks / sanity-check layers that aren't needed in a cloud sandbox.
# uv is pre-installed in cloud sessions. uv is idempotent and fast when the
# lock is already satisfied, so it's safe to run on every session/resume.
echo ""
echo "📦 Installing dependencies (uv sync)..."
INSTALL_START=$(date +%s)

# Capture output so a failure shows WHY (uv's own error) instead of a bare
# "failed". One quick retry rides out a transient boot-time hiccup (e.g. the
# package index not yet reachable) without slowing the common success path.
SYNC_LOG=$(mktemp)
for attempt in 1 2; do
  if uv sync --all-extras --group dev >"$SYNC_LOG" 2>&1; then
    INSTALL_DURATION=$(($(date +%s) - INSTALL_START))
    echo "✅ Dependencies installed (${INSTALL_DURATION}s, attempt ${attempt})"
    break
  fi
  echo "⚠️  uv sync attempt ${attempt} failed:"
  tail -20 "$SYNC_LOG" | sed 's/^/      /'
  if [ "$attempt" -lt 2 ]; then
    sleep 3
  else
    echo "❌ uv sync failed — the session may not have a working .venv."
  fi
done
rm -f "$SYNC_LOG"

# --- Optional: install gh CLI (non-critical) --------------------------------
# Cloud sessions ship built-in GitHub tools that authenticate as the user, so
# gh is not strictly required. Install it best-effort for commands the built-in
# tools don't cover; never let a failure here block the session.
echo ""
if ! command -v gh &> /dev/null; then
  echo "📦 Installing gh CLI (optional)..."
  if GH_VERSION=$(curl -fsSL https://api.github.com/repos/cli/cli/releases/latest \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))" 2>/dev/null) \
      && [ -n "$GH_VERSION" ] \
      && curl -fsSL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz" -o /tmp/gh.tar.gz \
      && tar -xzf /tmp/gh.tar.gz -C /tmp/; then
    mkdir -p "$HOME/.local/bin"
    cp "/tmp/gh_${GH_VERSION}_linux_amd64/bin/gh" "$HOME/.local/bin/gh" && chmod +x "$HOME/.local/bin/gh"
    rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_amd64"
    export PATH="$HOME/.local/bin:$PATH"
    # Persist PATH for subsequent commands in this session.
    [ -n "$CLAUDE_ENV_FILE" ] && echo "PATH=$HOME/.local/bin:\$PATH" >> "$CLAUDE_ENV_FILE"
    echo "✅ gh CLI installed ($("$HOME/.local/bin/gh" --version | head -1))"
  else
    echo "ℹ️  gh CLI not installed (optional; built-in GitHub tools still work)."
  fi
else
  echo "✓ gh CLI already available ($(gh --version | head -1))"
fi

# --- Verify -----------------------------------------------------------------
echo ""
echo "🔍 Verifying installation..."
if [ -x .venv/bin/etl ]; then
  echo "   ✓ ETL CLI available ($(.venv/bin/python --version 2>&1))"
else
  echo "   ✗ .venv/bin/etl not found — run 'uv sync --all-extras --group dev' in the session."
fi

echo ""
echo "✅ Setup complete! Total time: $(($(date +%s) - START_TIME))s"
# Always exit 0: a SessionStart hook should never block the session from starting.
exit 0
