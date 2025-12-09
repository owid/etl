# Git Workflow

## Creating PRs

**Always use `etl pr` command** - never use `git checkout -b` + `gh pr create` manually.

```bash
# 1. Create PR (creates new branch)
.venv/bin/etl pr "Update dataset" data

# 2. Check status
git status

# 3. Add files
git add .  # or specific files

# 4. Commit
git commit -m "Description"
```

Note: `etl pr` creates a branch but does NOT auto-commit files.

## Commit Messages

Use emoji prefix + 🤖 for AI-written code:

| Emoji | When to use |
|-------|-------------|
| 🎉 | New feature for user |
| 🐛 | Bug fix for user |
| ✨ | Visible improvement (not new feature or bug fix) |
| 🔨 | Code change (not feature/bug fix) |
| 📜 | Documentation changes |
| ✅ | Adding/refactoring tests (no production change) |
| 🐝 | Upgrading dependencies/tooling |
| 💄 | Formatting only |
| 🚧 | Work in progress |
| 📊 | Data updates |

Example: `🐛🤖 Fix country mapping for South Sudan`

## Before Committing

**Always run `make check`** - formats code, fixes linting, runs type checks.

## Package Management

Use `uv` (not pip):

```bash
uv add package_name
uv remove package_name
uv sync
```
