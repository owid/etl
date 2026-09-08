# Maintaining `owid-catalog`

How this library is versioned, released and checked. For what the library *is* and how
to use it, see [`README.md`](README.md); for the architecture and coding patterns, see
[`CLAUDE.md`](CLAUDE.md).

## Where it lives, and who sees your change

The library lives at `lib/catalog/` inside [owid/etl](https://github.com/owid/etl) and is
published to PyPI as [`owid-catalog`](https://pypi.org/project/owid-catalog/).

The `etl` repo consumes it as an **editable path dependency** — the repo-root
`pyproject.toml` declares:

```toml
owid-catalog = { path = "lib/catalog", editable = true }
```

So a change is live for everything in this repo (ETL steps, `apps/`, wizard, tests) the
moment it merges — no version bump, no release, no waiting. A release only matters for
consumers *outside* the repo: notebooks and other repos that `pip install owid-catalog`.

## Nothing bumps the version for you

`version = "..."` in `lib/catalog/pyproject.toml` is edited by hand. There is no
auto-increment, no release-please, no version-from-tag. A PR that changes only source
files publishes nothing and leaves the version where it was.

Publishing is automated *once the version changes*, by
[`.github/workflows/publish-owid-packages.yml`](../../.github/workflows/publish-owid-packages.yml):

- **Trigger**: push to `master` whose diff touches `lib/catalog/pyproject.toml` (same for
  `lib/datautils` and `lib/repack`) — the `paths:` filter — or a manual
  `workflow_dispatch` with `package` = `catalog` | `datautils` | `repack` | `all`.
- **`detect-changes` job**: diffs the last commit on `master` (`git diff HEAD~1 HEAD`) to
  decide which of the three packages to build.
- **`publish` job**: reads the version out of `pyproject.toml`, asks
  `https://pypi.org/pypi/owid-catalog/<version>/json`, and **fails** with
  *"Please increment the version before publishing"* if that version already exists.
  Otherwise `uv build` + `uv publish --trusted-publishing always` — PyPI trusted
  publishing over OIDC (the job's `id-token: write` permission and the `pypi`
  environment), so there is no API token to rotate.

Consequences worth knowing:

- **Source changes accumulate.** Everything merged since the last bump ships with
  whoever bumps next — they are, in effect, releasing other people's work. Write the
  changelog from the commit range, not from memory (see below).
- `lib/owl` and `lib/walden` are **not** in this workflow — no release of them happens
  from here (`lib/walden` has no `pyproject.toml` at all).
- A published version can never be replaced. If you shipped something broken, bump again.

## Cutting a release

From `lib/catalog/`:

1. **Bump** `version` in `pyproject.toml`.
2. **Write the changelog** — a new section at the top of the *Changelog* in `README.md`,
   covering everything since the last release, which you can list with:

   ```bash
   # <last-bump> = the commit that set the previous version
   git log --oneline --no-merges <last-bump>..HEAD -- lib/catalog/owid
   ```

   `lib/catalog/owid` (rather than `lib/catalog`) filters out commits that only touched
   the lockfile or CI. Group entries the way existing sections do, and name the
   replacement for anything you removed.
3. **Refresh both lockfiles.** The version is recorded twice: in `lib/catalog/uv.lock`
   and in the repo-root `uv.lock` (as `owid-catalog … source = { editable = "lib/catalog" }`).
   Run `make .venv` in `lib/catalog/` **and** at the repo root — or `uv lock` in both.
   Every past bump touches all three files; a bump that only edits `pyproject.toml`
   leaves the repo out of sync.
4. **Check** (`make check && make test` from `lib/catalog/`) and, because the whole repo
   uses the working copy, run the root `make unittest` too.
5. **Merge to `master`.** The workflow publishes within a few minutes; confirm on
   [PyPI](https://pypi.org/project/owid-catalog/) and in the
   [Actions run](https://github.com/owid/etl/actions/workflows/publish-owid-packages.yml).

### Choosing the number

There is no written policy; this is what the history does (the package is still
`Development Status :: 4 - Beta`, and the README calls the API experimental):

| Part | Used for | Examples |
|------|----------|----------|
| MAJOR | redesign of the public surface | `1.0.0` — restructure into `core`/`api`, unified `Client` |
| MINOR | removing or renaming public API | `1.1.0` dropped the processing log; `1.2.0` dropped `Source`/`sources` |
| PATCH | additive API, bug fixes, dependency bumps, Python-support changes | `1.2.1`–`1.2.5` |

Note that breaking *removals* have not been treated as major: `1.1.0` and `1.2.0` took
public API away in minor releases, while `1.2.4` dropped `display.yearIsDay` and `1.2.5`
dropped a `CHANNEL` member — both in patches. So pinning `owid-catalog` loosely is not
enough to be safe, and when you do remove something, say so prominently in the changelog.

### When a bump is worth it

Bump when someone outside this repo needs the change: an external notebook or repo, a
security fix in a dependency of the published wheel (`1.2.3`), or a rebuild to pick up
tooling changes (`1.2.2` was a republish and nothing else). Skip it when the change only
matters to `etl` itself — it is already live — and accept that it will ride along with
the next bump.

## Development environment

`lib/catalog/` has **its own** venv, `Makefile` and `uv.lock`, separate from the repo root:

```bash
cd lib/catalog
make .venv          # uv sync --all-extras --group dev
.venv/bin/pytest tests/
```

- **Never run bare `uv sync`** — it prunes optional dependencies. `make .venv` runs the
  right command.
- Change dependencies with `uv add` / `uv remove` from inside `lib/catalog/`, then
  refresh the root lockfile as well (step 3 above).
- Targets come from [`../../default.mk`](../../default.mk): `make test`
  (= `check-formatting check-linting check-typing unittest`), `make check` (fix + lint +
  typecheck), `make watch`, `make unittest`, `make coverage`, `make format`, `make lint`.
- The typechecker is **`ty`**, pinned in the dev dependencies (`ty==0.0.55` at the time of
  writing), together with `ruff` for lint and format. `pyright` and `mypy` are long gone.
- A stale, gitignored `.python-version` in this directory is a common local trap: `uv`
  honours it and then warns that it *"resolved to Python 3.10.x, which is incompatible
  with the project's Python requirement"*. Fix it with `uv python pin 3.13` (or delete
  the file) and rebuild the venv.
- `make bump` exists in `default.mk` but is dead here — `bump2version` is not a dependency
  of this package. Edit `pyproject.toml`.

## What actually checks your change

- **The repo-root pre-commit hook does not cover this directory.** It runs the root
  `make check`, whose `SRC` is `etl snapshots apps api_search tests docs owid_mcp
  owl_steps lib/owl/owl` — `lib/catalog` is not in it. Committing a library change from
  the repo root lints and typechecks nothing of it. Run `make check` and `make test`
  from inside `lib/catalog/`.
- From the repo root, `make test-all`, `make check-all` and `make format-all` loop over
  `LIBS` (`lib/catalog lib/datautils lib/owl lib/repack`) after doing the root itself.
- `lib/catalog/.pre-commit-config.yaml` wires the same three checks for anyone running
  `pre-commit` inside this directory.
- `lib/catalog/.github/workflows/python-package.yml` is a leftover from when the library
  had its own repository. GitHub only reads workflows from the repo root, so **it never
  runs here**, even though action-pinning PRs keep updating it.
- Unit tests are run by the Buildkite pipeline behind the badge at the top of
  `README.md` (`owid-catalog-unit-tests`); that pipeline's configuration lives outside
  this repo, so its run history is the place to check what it did.

## Backwards compatibility

External code imports from `owid.catalog`, so treat that surface as public:

- Top-level stub modules (`meta.py`, `tables.py`, `utils.py`, `processing.py`) re-export
  from `owid/catalog/core/`. Keep them importable.
- `Variable` and `Indicator` are the same class; both names are in use.
- Metadata dataclasses are serialized into published catalog artifacts (`index.json`,
  `*.meta.json`) and read back by this library, so renaming or dropping a metadata field
  affects data that is already out there — check that older artifacts still load.
- Anything removed belongs in the changelog with its replacement named, the way the
  `1.1.0` and `1.2.0` entries do.

## Documentation

- Prose docs: `docs/libraries/catalog/{intro,api,structures,maintenance}.md`, with the
  nav in `zensical.toml`. `maintenance.md` is the reader-facing version of this file.
- Some pages are generated at build time by `docs/ignore/pre-build/bake_*.py` (run by
  `make docs.pre` from the repo root), including `bake_catalog_api.py`.
- `docs/libraries/catalog/llms.txt` is generated from docstrings and docs by
  `make docs.llms` (repo root). Regenerate it after changing public docstrings.
