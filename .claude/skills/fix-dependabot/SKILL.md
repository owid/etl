---
name: fix-dependabot
description: Resolve Dependabot security alerts on owid/etl by upgrading vulnerable dependencies. Use when the user mentions "dependabot", "security alerts", "vulnerability", "CVE", "security fixes", "dependabot alerts", or wants to fix vulnerable packages. Also trigger when the user pastes a GitHub Dependabot URL or asks about outdated/insecure dependencies.
metadata:
  internal: true
---

# Fix Dependabot Alerts

Resolve open Dependabot security alerts by upgrading vulnerable dependencies across the ETL monorepo — including pip packages (`pyproject.toml` + `uv.lock`), lib/ subdirectories, and npm packages in `vscode_extensions/`.

**Goal: leave zero open Dependabot PRs.** Security alerts are the priority, but Dependabot also opens routine **version-update PRs** that have no associated alert (e.g. esbuild minor bumps, or js-yaml bumps in extensions GitHub didn't flag). Don't stop at the alerts — also clear these (Step 4c) so the PR list is empty afterward. The default is to fold every open Dependabot PR's bump into the single batch PR and close the originals; if the user only wants the security alerts, scope to those instead.

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

## Step 1b: Audit existing Dependabot PRs

Always list open Dependabot PRs, not just open alerts. Dependabot PRs can remain open after the default branch already contains the requested dependency version (especially old/conflicted PRs where automatic rebases have been disabled).

```bash
gh pr list --repo owid/etl --author app/dependabot --state open \
  --json number,title,headRefName,mergeStateStatus,files --limit 100 \
  --jq '.[] | {number,title,mergeStateStatus,files:[.files[].path]}'
```

For each open PR:

1. Identify the manifest/lockfile and target package/version from the title and changed files.
2. Compare against the **current default branch** (`origin/master` or `master`), not just your working branch. Check whether the dependency is already at the requested version or newer.
   - For `uv.lock` files, inspect the relevant package entry in the affected lockfile.
   - For npm lockfiles, parse `package-lock.json` and check all installed versions of the package in that extension.
3. If the PR is obsolete, close it with a clear comment, for example:
   ```bash
   gh pr close <number> --repo owid/etl --comment \
     "Closing as obsolete: the current default branch already has this dependency at the requested version or newer, so this stale Dependabot PR is no longer relevant."
   ```
4. If you create a replacement PR that batches or supersedes Dependabot PRs, close the superseded PRs and reference the replacement PR in the close comment.
5. Re-run the PR list and confirm there are no open irrelevant Dependabot PRs before reporting completion.

**Map each open PR to an alert (or not).** Cross-reference this PR list against the alert list from Step 1. A PR whose package/manifest matches an open alert is a *security* PR — handled by Steps 2–4. A PR with **no** matching open alert is a routine *version-update* PR — handled by Step 4c. Both kinds should be gone by the end.

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

### npm transitive vs. direct deps

Before adding a package to `dependencies`, check whether the extension actually imports it:

```bash
rg -l "from ['\"]<package>|require\(['\"]<package>" vscode_extensions/<ext>/src
```

- **Imported in `src/`** → it's a real runtime dependency; bump it in `dependencies` (`npm install <package>@^<version>`).
- **Not imported** (it's a dev-tooling transitive dep, e.g. pulled in by eslint/mocha) → do **not** add it to `dependencies` (that creates a phantom runtime dep). Instead force the patched version via the existing `overrides` block in `package.json`, then `npm install`. This matches how Dependabot itself fixes those (lockfile-only).

## Step 4c: Clear remaining (non-security) Dependabot version-update PRs

After the security alerts are handled, fold every *remaining* open Dependabot PR (the ones with no matching alert from Step 1b) into the same batch branch, then close them. These are routine bumps — usually npm devDeps in `vscode_extensions/` (e.g. esbuild) or js-yaml in extensions GitHub didn't alert on.

For each remaining PR, apply its bump locally rather than merging the Dependabot branch (keeps everything in one PR with one CI run):

1. Read the target package + version from the PR title (`Bump <pkg> from <old> to <new> in /<path>`).
2. `cd` into the manifest's directory and apply it the same way as Step 4 (direct bump if imported in `src/`, `overrides` if a dev-tooling transitive — see above). For a pure devDependency like esbuild, bump it in `devDependencies`: `npm install -D <pkg>@^<new>`.
3. Verify it resolved: `npm ls <pkg>`.

After applying all of them, **compile each touched extension** so a bad bump fails locally, not in CI:

```bash
cd vscode_extensions/<ext> && npm run compile   # or `npm run lint` if there's no compile script
```

Then close each superseded Dependabot PR referencing the batch PR (do this in Step 6, after the batch PR exists):

```bash
gh pr close <number> --repo owid/etl --comment \
  "Superseded by #<batch-PR>, which applies this bump as part of a single batched Dependabot sweep. Closing as obsolete."
```

If a bump is risky or a major version jump with breaking changes, don't force it into the batch — leave that PR open and flag it for the user instead.

## Step 5: Verify changes

Run `make check-all` (lint + format + typecheck across the root and every `lib/`). The plain `make check` only covers the top-level codebase, so breaking changes from upgrades to packages used by `lib/` (e.g. gdown's `fuzzy=` removal in `lib/datautils/`) would slip through.

If it fails, fix the issues (commonly: removed/renamed kwargs that type checkers flag) and re-run until it passes.

## Step 5b: Bump lib/ package versions

If you modified any `lib/{catalog,datautils,repack}/pyproject.toml`, also bump that file's `version = "x.y.z"` (patch bump). The `publish-owid-packages.yml` workflow republishes these packages to PyPI on master push and rejects the publish if the version already exists.

After bumping, re-run `uv lock` (top-level and inside each modified `lib/<name>/`) so the lockfiles pick up the new version.

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

Then close **every** open Dependabot PR this batch covers — both the security ones (Steps 2–4) and the version-update ones (Step 4c) — with a comment referencing this PR (see the close commands in Steps 1b and 4c). Finally, re-run the Step 1b PR list and confirm it's empty (modulo anything you deliberately left open and flagged for the user). If the title only covers security fixes but you also swept version updates, use a broader title like `Fix Dependabot vulnerabilities and clear version-update PRs`.

## Important notes

- `lib/` contains subdirectories (`catalog`, `datautils`, `repack`, `walden`) with their own `pyproject.toml` files — check these too
- Multiple alerts may reference the same package (e.g. 4 alerts for pytest) — one upgrade fixes them all
- The `uv.lock` file may also be affected by `[tool.uv] exclude-newer` settings in pyproject.toml — if a patched version exists on PyPI but uv won't resolve it, check this setting
- Some alerts may be auto-dismissed by Dependabot (state `auto_dismissed`) — skip those
- When multiple packages need upgrading, batch them: `uv lock --upgrade-package pkg1 --upgrade-package pkg2`
