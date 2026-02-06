#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

log_info "Setting up ETL environment for remote session..."

# Only run in remote environments
if [ "$CLAUDE_CODE_REMOTE" != "true" ]; then
  log_warning "Skipping setup (not a remote session)"
  exit 0
fi

# Use make .venv to set up the environment
log_info "Running make .venv to install dependencies..."
if make .venv; then
  log_success "Dependencies installed successfully"
else
  log_error "Failed to install dependencies"
  exit 1
fi

# Verify critical tools
log_info "Verifying installation..."

PYTHON_VERSION=$(.venv/bin/python --version 2>&1)
log_success "Python: $PYTHON_VERSION"

if [ -f ".venv/bin/etl" ]; then
  log_success "ETL CLI available at .venv/bin/etl"
else
  log_error ".venv/bin/etl not found"
  exit 1
fi

if [ -f ".venv/bin/etlr" ]; then
  log_success "ETLR available at .venv/bin/etlr"
fi

if [ -f ".venv/bin/pytest" ]; then
  log_success "pytest available"
fi

log_success "ETL environment setup complete!"
exit 0
