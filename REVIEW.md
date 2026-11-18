# Review guidelines

## Risk

What makes a change risky in this repository. The `/owid-staff:rate-pr-risk` skill (opt-in) reads this to suggest whether a PR needs a second review: `risk: low` (no), `risk: medium` (recommended) or `risk: high` (needed). The PR owner decides.

What hurts most is a wrong number on a published chart that nobody notices: readers cite it, and a red build never flags it. Publishing what can't be taken back ranks with it: licensed private data in the public catalog, a leaked secret, files pushed to public GitHub repositories or R2. A broken chart or a failed step ranks below both, and stale data below that: the production run uses `--continue-on-failure`, so a failing step leaves its dataset on the old version and shows up as a red Buildkite build. It also stops the rest of that deploy (the `viz://` steps, chart sync, the catalog publish), and every later merge stops the same way until the step is fixed. The grapher database is backed up and the catalog rebuilds from snapshots, so a wrong value is fixed by a follow-up PR once someone sees it.

The diff doesn't show what a merge runs. Merging to `master` runs the production deploy: every step whose checksum changed with `--grapher` (upserting to the production database), the `viz://chart`, `viz://explorer` and `viz://bespoke` steps, `chart-sync` of the charts approved in chart diff on the branch's staging server, the catalog publish, and the few `export://` steps the deploy names. The wizard and the search API restart from `master`. `lib/catalog`, `lib/datautils` and `lib/repack` publish to PyPI when their `pyproject.toml` changes. A step's checksum covers its own files, its dependencies' checksums, the pandas version and `ETL_EPOCH`, so a change to a widely used dataset, to pandas or to `ETL_EPOCH` rebuilds and republishes a large part of the DAG on merge. A change to shared code (`etl/helpers.py`, `lib/catalog`) or to another library changes no checksum: production picks it up only as steps rebuild for other reasons, so its effect shows up later, in unrelated updates. Snapshots are uploaded to R2 when someone runs `etls`, before the merge, to the public bucket unless the `.dvc` sets `is_public: false`.

### High: where we need to be careful

- **Code many datasets run through.** `etl/helpers.py`, `etl/steps/__init__.py` (running steps and their checksums), `etl/data_helpers/`, harmonization, `lib/catalog` (processing, metadata and origins propagation), and widely used inputs such as the regions, population and income groups datasets. A mistake changes numbers in charts nobody looked at.
- **What writes to production.** `etl/grapher/` (the database upsert), `apps/chart_sync/`, chart diff (`apps/wizard/app_pages/chart_diff/`, which decides what `chart-sync` carries over), `apps/indicator_upgrade/` and `etl/indicator_upgrade/` (which rewrite charts), `etl/collection/` (MDims and explorers), `etl/publish.py`, and the `export://` steps the deploy runs.
- **What can't be taken back.** Licensed data reaching the public catalog: a private step made public (`data-private://` to `data://`), a new step or snapshot for restricted data added as public (its snapshot is public as soon as someone runs `etls`), or a change to what keeps private data out of the public buckets (`is_public`, the private-bucket routing in `etl/publish.py` and `etl/snapshot.py`); `export://` steps that push to public GitHub repositories or R2, and a published `lib/catalog`, `lib/datautils` or `lib/repack` release.
- **Who can do what.** Secrets and `.env` handling, `.github/workflows/` (the PyPI publish), the admin API client, and untrusted input reaching the public services that run from this repository (`api_search/`, `owid_mcp/`).

### Medium

- Updating an existing dataset or adding a new one with charts, when the author shows the output: chart diff on the branch's staging server reviewed, and `etl diff` with the expected changes. The reviewer's time goes to the numbers that moved, not the step code. Without that evidence, it is high.
- Version-bumping a grapher dataset that existing charts use: the charts stay on the old indicators until they are remapped.
- Dependency bumps that can change numbers (`pandas`, `numpy`, anything a step computes with), and changes to checksum or change detection that can rebuild most of the DAG.
- Wizard pages staff rely on daily, apart from chart diff, and CLI tools that write to staging.

### Low

Docs, tests, agent instructions and skills, `ai/` and playground files, read-only dev tooling, the VS Code extensions, a public dataset that no published chart uses yet, and a metadata or chart-text change whose result chart diff shows.
