---
name: fix-dependabot
description: Resolve Dependabot security alerts on owid/etl by upgrading vulnerable dependencies. Use when the user mentions "dependabot", "security alerts", "vulnerability", "CVE", "security fixes", "dependabot alerts", or wants to fix vulnerable packages. Also trigger when the user pastes a GitHub Dependabot URL or asks about outdated/insecure dependencies.
metadata:
  internal: true
---

# Fix Dependabot Alerts

Resolve open Dependabot security alerts by upgrading vulnerable dependencies across the ETL monorepo — including pip packages (`pyproject.toml` + `uv.lock`), lib/ subdirectories, and npm packages in `vscode_extensions/`.

## Step 1: Fetch and summarize alerts

```bash
gh api repos/owid/etl/dependabot/alerts \
  --jq '.[] | select(.state == "open") | {
    number, state,
    package: .dependency.package.name,
    ecosystem: .dependency.package.ecosystem,
    severity: .security_advisory.severity,
    summary: .security_advisory.summary,
    patched: .security_advisory.vulnerabilities[0].first_patched_version.identifier,
    manifest: .dependency.manifest_path
  }'
```

Present a summary table grouped by severity (critical > high > medium > low) with counts, then the detailed list showing package, ecosystem, severity, and summary.

Ask the user which severities to fix (e.g. "critical and high only" or "all"). If the user already specified a filter in their request, proceed with that.

## Step 2: Categorize each alert

For each alert to fix, determine:

1. **Ecosystem**: `pip` or `npm`
2. **Direct vs transitive**: Is the package listed directly in `pyproject.toml` (or a `lib/*/pyproject.toml`), or is it only in the lockfile as a transitive dependency?
3. **Manifest location**: Which `pyproject.toml` or `package.json` owns this dependency?

For pip packages, check all pyproject.toml files:
```bash
grep -rn '<package-name>' pyproject.toml lib/*/pyproject.toml
```

For npm packages, check the manifest path from the alert (usually under `vscode_extensions/`).

## Step 3: Fix pip dependencies

### Direct dependencies

If the package appears in a `pyproject.toml` `dependencies` or optional dependency group:

1. Update the version constraint to require at least the patched version:
   ```
   "package>=old_version"  →  "package>=patched_version"
   ```
2. Run `uv lock --upgrade-package <package>` to update the lockfile

### Transitive dependencies

If the package is only in `uv.lock` (not a direct dependency):

1. Identify which direct dependency pulls it in:
   ```bash
   uv tree --invert --package <vulnerable-package> --depth 1
   ```
2. Check if any of those parent packages cap the vulnerable package below the patched version. For each parent, check its constraints on PyPI:
   ```python
   python3 -c "
   import json, urllib.request
   r = urllib.request.urlopen('https://pypi.org/pypi/<parent>/json')
   d = json.loads(r.read())
   deps = d['info'].get('requires_dist') or []
   matches = [dep for dep in deps if '<vulnerable-pkg>' in dep.lower()]
   print(matches)
   "
   ```
3. **If no cap blocks it**: simply run `uv lock --upgrade-package <vulnerable-package>`
4. **If a cap blocks it**: check if a newer version of the parent package relaxes the cap. If it does, upgrade the parent. If the latest parent still caps it:
   - Check if the parent package is actually used in the codebase (`grep -rn 'import <parent>\|from <parent>' --include='*.py'`)
   - If unused: remove it from `pyproject.toml` (this unblocks the transitive dep)
   - If used: inform the user that the fix requires either waiting for the parent to update, or adding a `[tool.uv] override` (explain the tradeoff — overrides can cause runtime incompatibilities)

### After removing a dependency

When removing a package, check if it's imported anywhere in the codebase. If it is, replace its usage with an alternative approach. For example, when we removed `moviepy`, we replaced its `ImageSequenceClip` usage with a direct `ffmpeg` subprocess call.

Always search broadly:
```bash
rg 'import <package>|from <package>' --type py
```

## Step 4: Fix npm dependencies

For each vulnerable npm package:

1. Navigate to the directory containing the affected `package.json`
2. Update the package:
   ```bash
   cd <directory>
   npm install  # ensure node_modules exist
   npm install <package>@^<patched-version>
   ```
3. Verify the update:
   ```bash
   npm ls <package>
   ```

## Step 5: Verify changes

Run `make check` to ensure nothing is broken. This runs linting, formatting, and type checking.

If `make check` fails, fix the issues (commonly: removed imports that type checkers flag). Then re-run until it passes.

## Step 6: Create PR

Follow the standard ETL PR workflow:

```bash
.venv/bin/etl pr "Fix <severity> Dependabot vulnerabilities" data
```

Stage and commit all changed files:
- `pyproject.toml` and any `lib/*/pyproject.toml`
- `uv.lock`
- Any modified `.py` files (from removing unused deps)
- Any `package.json` / `package-lock.json` files

Use commit emoji `🐛🤖` (bug fix, AI-written).

After pushing, post `@codex review` as a PR comment.

## Important notes

- `lib/` contains subdirectories (`catalog`, `datautils`, `repack`, `walden`) with their own `pyproject.toml` files — check these too
- Multiple alerts may reference the same package (e.g. 4 alerts for pytest) — one upgrade fixes them all
- The `uv.lock` file may also be affected by `[tool.uv] exclude-newer` settings in pyproject.toml — if a patched version exists on PyPI but uv won't resolve it, check this setting
- Some alerts may be auto-dismissed by Dependabot (state `auto_dismissed`) — skip those
- When multiple packages need upgrading, batch them: `uv lock --upgrade-package pkg1 --upgrade-package pkg2`
