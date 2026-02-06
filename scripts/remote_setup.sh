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

# Install gh CLI if not available
if ! command -v gh &> /dev/null; then
  log_info "Installing gh CLI..."
  GH_VERSION=$(curl -fsSL https://api.github.com/repos/cli/cli/releases/latest | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))")
  if [ -n "$GH_VERSION" ]; then
    mkdir -p "$HOME/.local/bin"
    curl -fsSL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz" -o /tmp/gh.tar.gz
    tar -xzf /tmp/gh.tar.gz -C /tmp/
    cp "/tmp/gh_${GH_VERSION}_linux_amd64/bin/gh" "$HOME/.local/bin/gh"
    chmod +x "$HOME/.local/bin/gh"
    rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_amd64"
    export PATH="$HOME/.local/bin:$PATH"
    log_success "gh CLI $(gh --version | head -1) installed"
  else
    log_warning "Could not determine gh CLI version, skipping"
  fi
else
  log_success "gh CLI already available"
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
