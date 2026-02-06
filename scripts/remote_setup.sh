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
