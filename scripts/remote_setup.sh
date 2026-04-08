#!/bin/bash
set -e

START_TIME=$(date +%s)

echo "🚀 Setting up ETL environment for remote session..."
echo "   Started at: $(date '+%Y-%m-%d %H:%M:%S')"

# Only run in remote environments
if [ "$CLAUDE_CODE_REMOTE" != "true" ]; then
  echo "⏭️  Skipping setup (not a remote session)"
  exit 0
fi

# Install gh CLI if not available
echo ""
if ! command -v gh &> /dev/null; then
  echo "📦 Installing gh CLI..."
  GH_START=$(date +%s)

  GH_VERSION=$(curl -fsSL https://api.github.com/repos/cli/cli/releases/latest | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))")
  if [ -n "$GH_VERSION" ]; then
    mkdir -p "$HOME/.local/bin"
    curl -fsSL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz" -o /tmp/gh.tar.gz
    tar -xzf /tmp/gh.tar.gz -C /tmp/
    cp "/tmp/gh_${GH_VERSION}_linux_amd64/bin/gh" "$HOME/.local/bin/gh"
    chmod +x "$HOME/.local/bin/gh"
    rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_amd64"
    export PATH="$HOME/.local/bin:$PATH"

    # Persist PATH for subsequent commands
    if [ -n "$CLAUDE_ENV_FILE" ]; then
      echo "PATH=$HOME/.local/bin:\$PATH" >> "$CLAUDE_ENV_FILE"
    fi

    GH_END=$(date +%s)
    GH_DURATION=$((GH_END - GH_START))
    echo "✅ gh CLI $(gh --version | head -1) installed (${GH_DURATION}s)"
  else
    echo "⚠️  Could not determine gh CLI version, skipping"
  fi
else
  echo "✓ gh CLI already available ($(gh --version | head -1))"
fi

# Use make .venv to set up the environment
echo ""
echo "📦 Running make .venv to install dependencies..."
INSTALL_START=$(date +%s)

if make .venv; then
  INSTALL_END=$(date +%s)
  INSTALL_DURATION=$((INSTALL_END - INSTALL_START))
  echo "✅ Dependencies installed successfully (${INSTALL_DURATION}s)"
else
  echo "❌ Failed to install dependencies"
  exit 1
fi

# Verify critical tools
echo ""
echo "🔍 Verifying installation..."

PYTHON_VERSION=$(.venv/bin/python --version 2>&1)
echo "   ✓ Python: $PYTHON_VERSION"

if [ -f ".venv/bin/etl" ]; then
  echo "   ✓ ETL CLI available"
else
  echo "   ✗ .venv/bin/etl not found"
  exit 1
fi

if [ -f ".venv/bin/etlr" ]; then
  echo "   ✓ ETLR available"
fi

if [ -f ".venv/bin/pytest" ]; then
  echo "   ✓ pytest available"
fi

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo ""
echo "✅ ETL environment setup complete!"
echo "   Total time: ${TOTAL_DURATION}s"
exit 0
