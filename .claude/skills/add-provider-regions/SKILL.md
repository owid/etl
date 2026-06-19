---
name: add-provider-regions
description: Add an external provider's regional aggregation (e.g. World Bank, WHO, Maddison, WID, ILO) to OWID's regions dataset — definitions in regions.yml, per-provider grapher map indicators, and metadata. First checks whether the provider's dataset already encodes the regions and their country composition; if not, asks the user for a reference (link/doc) to derive it from. Trigger when the user wants to add/define a provider's world regions, expose "{Provider} regions" on a map, or migrate an in-dataset region variable to the shared regions dataset.
metadata:
  internal: true
---

# Add provider regions

Add an external data provider's regional grouping (World Bank, WHO, UN, IEA, Pew, Maddison, WID, ILO, …) to OWID's shared regions dataset. The regions are defined once in `regions.yml`; the grapher step then turns each provider into a categorical country→region map indicator (`{defined_by}_region`) that powers the [world-region-map-definitions](https://ourworldindata.org/world-region-map-definitions) page.

There are two repos involved: the **`etl`** repo (Steps 1–8 — definitions, indicators, metadata, optional chart migration) and the **`owid-grapher`** repo (Step 9 — registering the provider in the frontend so its regions show up in entity selectors, tooltips, and admin presets). The owid-grapher work is a separate PR done *after* the ETL regions are merged and published to the catalog.

**The defining principle of this skill:** the country composition of each region must come from the *source* — either the provider's own dataset or a reference document the provider publishes — and be verified by set-equality. Never hand-type or guess which countries belong to a region.

## Inputs

- **Provider** — name and a short slug for `defined_by` (e.g. `wb`, `who`, `maddison`). Region names will be suffixed `(Provider)`.
- **Dataset path** (if one exists) — the provider's garden/grapher dataset, e.g. `data/garden/<ns>/<version>/<short_name>`. Many providers ship their regional grouping inside their own dataset.
- **Reference URL** (optional) — where the provider publishes the classification, used only if the dataset doesn't encode it.

All paths below are repo-relative. Always use `.venv/bin/` for `python`/`etl`/`etlr`.

The files you'll touch in **`etl`**:
- `etl/steps/data/garden/regions/2023-01-01/regions.yml` — region definitions (the header comment documents every field).
- `etl/steps/data/grapher/regions/2023-01-01/regions.py` — builds the `{defined_by}_region` indicators (only edit for a cross-tier back-fill, Step 3).
- `etl/steps/data/grapher/regions/2023-01-01/regions.meta.override.yml` — origin anchors + per-indicator metadata.
- **Not** `regions.codes.csv` — that file lists countries and OWID historical codes only; provider aggregates never go there.

And in **`owid-grapher`** (Step 9, separate PR): the auto-generated `packages/@ourworldindata/utils/src/regions/regions.data.ts` plus a few hand-maintained label registries. Reference PR: [owid/owid-grapher#6465](https://github.com/owid/owid-grapher/pull/6465) (IEA regions).

---

## Step 1 — Find the region composition (the key check)

Inspect the provider's dataset for an embedded aggregation, in this priority order:

1. A categorical **`region` / `subregion` column** mapping each country to its region.
2. **Region entities** present as rows in the data (e.g. `"East Asia (Provider)"` alongside countries).
3. A **table-of-contents / dictionary table** with tier columns (some providers ship `*_region`, `*_subregion_broad`, `*_subregion_detailed` style columns — these are authoritative tier definitions).

```python
from owid.catalog import Dataset
ds = Dataset("data/garden/<ns>/<version>/<short_name>")
print(ds.table_names)
tb = ds.read("<table>", safe_types=False)
print([c for c in tb.columns])
# region column -> country mapping:
print(tb.dropna(subset=["region"]).groupby("region")["country"].unique())
# or region entities present in the data:
print(sorted(c for c in tb["country"].unique() if "(" in str(c)))
```

**If the composition is in the data:** derive each region's member set directly from it. This is the source of truth.

**If it is NOT in the data:** ask the user for a **reference** — a link, PDF, or doc where the provider publishes the classification (e.g. a "regional groupings" page or methodology annex). Fetch it with WebFetch and derive membership from there. Keep the reference URL; it becomes the origin `url_main` in Step 6.

> Lesson: membership comes from the source and is verified by set-equality — not from judgment or memory.

---

## Step 2 — Resolve members to OWID region codes

Each member must map to a code that exists in `regions.yml`. Use `regions.codes.csv` for ISO alpha-2/alpha-3, with a name/alias fallback for non-standard codes (microstates, Kosovo-style cases).

```python
import csv, yaml
regions = yaml.safe_load(open("etl/steps/data/garden/regions/2023-01-01/regions.yml"))
by_code = {r["code"]: r for r in regions}
name_to_code = {}
for r in regions:
    name_to_code.setdefault(r["name"], r["code"])
    for a in r.get("aliases", []):
        name_to_code.setdefault(a, r["code"])
alpha2, alpha3 = {}, {}
for row in csv.DictReader(open("etl/steps/data/garden/regions/2023-01-01/regions.codes.csv")):
    if row["iso_alpha2"]: alpha2[row["iso_alpha2"]] = row["code"]
    if row["iso_alpha3"]: alpha3[row["iso_alpha3"]] = row["code"]

def resolve(member):  # member is a provider country name or code
    return alpha2.get(member) or alpha3.get(member) or name_to_code.get(member)

unresolved = [m for m in provider_members if resolve(m) is None]
assert not unresolved, f"Resolve these before continuing: {unresolved}"
```

Handle deliberately:
- **Historical entities** the provider assigns to a region (`OWID_USS`, `OWID_CZS`, `OWID_YGS`, `OWID_SDN`, …) — include them where the source does.
- **OWID aggregate codes** (e.g. Channel Islands `OWID_CIS`) — if the provider lists a sub-territory that OWID models as an aggregate, decide once where it lives and avoid listing it twice across regions (the garden step's duplicate-member check will catch a double-count). Document any such choice with a `# NOTE:` in the YAML.
- A region's `members` may reference **other aggregate codes** (not just countries); the garden step's `replace_aggregate_members` expands them recursively (see UN M49 in `regions.yml` for the pattern).

---

## Step 3 — Decide the tier structure

- **Single flat partition** (most providers): one `defined_by: <provider>`, one indicator. Done.
- **Hierarchical provider** (broad regions split into subregions): use **one `defined_by` per level** — `<provider>_1` (broadest), `<provider>_2`, … Look at the existing `un_m49_1/2/3` and `ilo_1/ilo_2` sections in `regions.yml` as templates.

Why split: the grapher step inverts *all* aggregates sharing a `defined_by` into a single country→region column. If two tiers share one `defined_by`, every country lands in 2+ regions, producing `"belongs to multiple regions"` warnings and a scrambled, order-dependent indicator. One `defined_by` per level keeps each indicator a clean partition.

Two rules for multi-tier providers:
- **Completeness:** each kept tier must partition the provider's covered world. You cannot keep a sub-breakdown of one parent without its siblings — e.g. if you keep one parent's sub-regions, keep every parent's, so the tier still covers everyone. Drop intermediate levels that don't form a complete partition; keep only tiers that are both useful and complete.
- **Region shared across tiers:** when one region exists at two levels (a broad region with no finer breakdown), it carries a single `defined_by`, so it appears natively in only one indicator. Tag it at one level, then **back-fill** it into the other level's indicator in the grapher step — mirror the existing `process_un_definitions` pattern in `grapher/regions/2023-01-01/regions.py` (one extra `fillna`/masked assignment after the inversion loop).

---

## Step 4 — Edit `regions.yml`

Append a section, delimited like the others:

```yaml
##########
# <Provider full name>
##########
- code: PROVIDER_XXX
  name: "<Region> (<Provider>)"
  region_type: "aggregate"
  defined_by: <provider>            # or <provider>_1 / <provider>_2 for tiers
  members:
    - ISO3
    - ISO3
    - PROVIDER_SUB                  # may reference another aggregate code
```

- **Names** carry the `(Provider)` suffix and should match the entity names the provider's own dataset publishes (so charts line up).
- **Codes** are `PROVIDER_XXX`, uppercase, unique.
- For composite levels whose members are sub-aggregates, you can either list the sub-aggregate codes (expanded recursively) or list the union of countries directly. When you need the country union, compute it programmatically rather than hand-typing:

```python
def expand(code):
    out = []
    for m in by_code[code]["members"]:
        out.extend(expand(m) if m in by_code and m.startswith("PROVIDER_") else [m])
    return sorted(set(out))
```

For anything beyond a couple of regions, **regenerate the whole provider section with a small script** (compute members, emit the YAML block, splice it into the file) rather than many manual edits — it's less error-prone and keeps formatting uniform.

---

## Step 5 — Build and verify the garden step

```bash
.venv/bin/etlr data://garden/regions/2023-01-01/regions --private
```

(No `--force` — editing the YAML is enough to trigger a rebuild.) The step runs sanity checks: unique codes/names, unique members within a region, all referenced codes exist, and cycle detection during aggregate expansion.

Then verify against the source and the partition property:

```python
from owid.catalog import Dataset
import json
tb = Dataset("data/garden/regions/2023-01-01/regions").read("regions").reset_index()

# 1) set-equality vs the source mapping (expected = region -> set of OWID codes from Step 1/2)
m = {r["code"]: set(json.loads(r["members"])) for _, r in tb[tb["defined_by"].str.startswith("<provider>")].iterrows()}
for code, exp in expected.items():
    assert m[code] == exp, f"{code}: missing {exp - m[code]}, extra {m[code] - exp}"

# 2) each tier partitions the same set and is pairwise-disjoint
for tier, codes in {"<provider>_1": [...], "<provider>_2": [...]}.items():
    union = set().union(*(m[c] for c in codes))
    for i, a in enumerate(codes):
        for b in codes[i+1:]:
            assert not (m[a] & m[b]), f"{a} & {b} overlap in {tier}"
    print(tier, len(union), "countries")
```

If a sanity check fails, fix the upstream logic or the membership — don't suppress the assertion.

---

## Step 6 — Grapher indicators + metadata

The grapher step auto-creates a `{defined_by}_region` column for every `defined_by`. Each needs a metadata block or the build fails on a missing title (`grapher_checks`).

**6a. Origin anchor** — add to the `definitions:` block in `regions.meta.override.yml`, taken from the provider dataset's *actual* origin:

```python
from owid.catalog import Dataset
ds = Dataset("data/garden/<ns>/<version>/<short_name>")
tb = ds.read(ds.table_names[0], safe_types=False)
o = tb[[c for c in tb.columns if c not in ("country", "year")][0]].metadata.origins[0]
print(o.producer, "|", o.title, "|", o.url_main, "|", o.date_accessed, "|", o.attribution, "|", o.attribution_short)
```

```yaml
  origins_<provider>: &origins_<provider>
    producer: <producer>
    title: <title>
    url_main: <url_main>              # or the reference URL from Step 1
    date_accessed: "<YYYY-MM-DD>"
    attribution: <attribution>        # only if the source defines it
    attribution_short: <short>        # only if the source defines it
```

> **Omit `date_published`.** Region definitions are time-invariant; a publication year would render next to the source line below the chart, where it's meaningless.

**6b. Indicator block** — one per `{defined_by}_region`, under `tables.regions.variables`:

```yaml
      <provider>_region:                       # or <provider>_1_region / <provider>_2_region
        title: World regions according to <Provider>
        description_short: |-
          Regions as defined by <Provider full name>.
        type: ordinal
        sort:
          - <Region> (<Provider>)              # legend order
          - ...
        origins:
          - *origins_<provider>
        presentation:
          grapher_config:
            hideAnnotationFieldsInTitle:
              time: true                        # hide the placeholder data year in titles
            map:
              colorScale:
                customCategoryLabels:
                  "<Region> (<Provider>)": "<Region>"   # strip the suffix on the legend
                customHiddenCategories:
                  "No data": true
```

**6c. Cross-tier back-fill** — if Step 3 found a region shared across tiers, add the masked back-fill to `grapher/regions/2023-01-01/regions.py` after the inversion loop (see the existing `process_un_definitions` example for the shape).

**6d. Build and verify:**

```bash
.venv/bin/etlr data://grapher/regions/2023-01-01/regions --private
```

```python
from owid.catalog import Dataset
tb = Dataset("data/grapher/regions/2023-01-01/regions").read("regions")
for col in ["<provider>_region", ...]:
    o = tb[col].metadata.origins[0]
    print(col, tb[col].notna().sum(), "countries,", tb[col].nunique(), "categories | attr:", o.attribution_short, "| date_published:", o.date_published)
```

Confirm: the new columns exist with titles, attribution carried, `date_published` is `None`, **no `"belongs to multiple regions"` warning** in the build log, and (multi-tier) each indicator covers the full partition.

---

## Step 7 (optional) — Migrate an existing in-dataset region chart

Do this only if the provider's *own* dataset already has a `region` variable powering a region-definition map chart that should now read the shared indicator (an "indicator upgrade"). Skip otherwise.

Find the old variable and its chart on the staging DB for the current branch:

```python
from etl.config import OWID_ENV
# variables of the provider dataset (note: query % LIKE with params=)
OWID_ENV.read_sql("""
  SELECT v.id, v.shortName FROM variables v JOIN datasets d ON v.datasetId=d.id
  WHERE d.shortName=%(s)s AND v.shortName='region'
""", params={"s": "<short_name>"})
# charts using it — slug lives on chart_configs, not charts
OWID_ENV.read_sql("""
  SELECT c.id, cc.slug, cc.full->>'$.title' AS title
  FROM charts c JOIN chart_dimensions cd ON cd.chartId=c.id
  JOIN chart_configs cc ON cc.id=c.configId
  WHERE cd.variableId=%(v)s
""", params={"v": OLD_VAR_ID})
```

Repoint the chart at the new `{provider}_region` variable:

```python
from etl.config import OWID_ENV
from apps.chart_sync.admin_api import AdminAPI
api = AdminAPI(OWID_ENV)
cfg = api.get_chart_config(CHART_ID)
cfg["dimensions"] = [{"property": "y", "variableId": NEW_VAR_ID}]
# re-key map colors/labels from the OLD category values to the NEW "(Provider)"-suffixed values,
# because the new variable's category values differ (they carry the suffix):
cs = cfg.setdefault("map", {}).setdefault("colorScale", {})
cs["customCategoryColors"] = {f"{name} (<Provider>)": color for name, color in OLD_COLORS.items()}
cs["customCategoryLabels"] = {f"{name} (<Provider>)": name for name in OLD_COLORS}
api.update_chart(CHART_ID, cfg)
```

Staging admin writes work behind Tailscale without `ADMIN_API_KEY`. The change surfaces in **chart-diff** for review before it reaches production. Verify by re-reading the config (dimensions point at the new variable; colors/labels intact).

---

## Step 8 — Commit and open a PR

```bash
make check
git add etl/steps/data/garden/regions/2023-01-01/regions.yml \
        etl/steps/data/grapher/regions/2023-01-01/regions.meta.override.yml \
        etl/steps/data/grapher/regions/2023-01-01/regions.py   # if back-fill added
git commit -m "📊🤖 Add <Provider> regions to regions dataset"
```

If not already on a feature branch, create one and a PR with `etl pr "Add <Provider> regions" data`, then push. In the PR body, open with the disclosure blockquote (`> _Written by Claude Code — @<handle> at the wheel._`) and keep any reviewer attribution out of committed code/YAML.

---

## Step 9 — Register the provider in owid-grapher (separate repo + PR)

The grapher frontend keeps its own copy of the regions and a few hand-maintained registries. The provider must be added there too, or its `(Provider)` entities won't be grouped/labelled correctly in entity selectors, map tooltips, and admin presets. Reference: [owid/owid-grapher#6465](https://github.com/owid/owid-grapher/pull/6465) (IEA).

**Sequencing:** the frontend's `regions.data.ts` is regenerated from the **production** catalog (`https://catalog.ourworldindata.org/external/owid_grapher/latest/regions/regions.csv`). So do Step 9 **after** the ETL PR (Step 8) is merged and the `data://external/owid_grapher/latest/regions` step has rebuilt on prod. (To preview earlier, you can point the updater at a staging catalog, but the committed PR should be regenerated from prod.)

Work in the `owid-grapher` repo on a new branch. Two paths, depending on how much the frontend should know about the provider:

### Path A — full region definitions (what IEA did)

The provider's regions (with member countries) live in `regions.data.ts`. Use this when you want tooltips to list member countries and the frontend to validate membership.

1. **Regenerate `regions.data.ts`:**
   ```bash
   yarn runRegionsUpdater
   ```
   This fetches the catalog CSV and rewrites `packages/@ourworldindata/utils/src/regions/regions.data.ts` (auto-generated — never hand-edit). The new `PROVIDER_XXX` aggregates appear with `definedBy: "<provider>"`. The `RegionDataProvider` / `RegionGroupKey` / `TooltipKey` union types are *derived* from this file, so the provider key becomes known to the type system automatically.

2. **Add the hand-maintained labels** (TypeScript will fail to compile until each `Record<RegionDataProvider, …>` has an entry — that's your checklist):
   - `adminSiteClient/EntityPresets.ts` → `REGION_DATA_PROVIDER_LABELS`: `<provider>: "<Short> regions"` (short, for the admin dropdown).
   - `packages/@ourworldindata/grapher/src/core/RegionGroups.ts` → `regionGroupLabels`: `<provider>: "<Provider full name> regions"` (in the *"we have region definitions"* group).
   - `packages/@ourworldindata/grapher/src/seriesLabel/RegionTooltipData.ts` → `descriptions`: `<provider>: "The **<Provider>** defines N world regions:"`. Optionally add a left-to-right map order to `customRegionDisplayOrder` (omit → alphabetical). See *"The region hover"* below for exactly what these two edits drive.

### Path B — suffix-only recognition (what Maddison / WID / ILO did)

The frontend recognizes the provider's entities purely by the `(Provider)` name suffix; no member definitions in `regions.data.ts`. Lighter; use when full member lists in the frontend aren't needed.

- `packages/@ourworldindata/grapher/src/core/GrapherConstants.ts` → add the slug to `ADDITIONAL_REGION_DATA_PROVIDERS` (this defines the `AdditionalRegionDataProvider` type).
- `adminSiteClient/EntityPresets.ts` → `ADDITIONAL_REGION_DATA_PROVIDER_LABELS`: `<provider>: "<Short> regions"`.
- `packages/@ourworldindata/grapher/src/core/RegionGroups.ts` → `regionGroupLabels`: `<provider>: "<Provider full name> regions"` (in the *"recognize by suffix"* group).
- No `RegionTooltipData` entry (its `TooltipKey` only covers full-definition providers).

For a **multi-level provider**, the frontend key usually maps to the bare provider slug (suffix matching), even though the ETL uses per-level `defined_by` (`<provider>_1`, `<provider>_2`). Check how `un_m49_1/2/3` vs `ilo` are keyed in `RegionGroups.ts` and mirror the closest precedent.

### The region hover (tooltip) — Path A only, fully data-driven

When a Grapher chart plots a region entity (e.g. `"Africa (IEA)"`) as a series, hovering its label shows a tooltip with a description, a mini world map, and a legend (`RegionTooltip.tsx` → `RegionMap.tsx`). Worth understanding because it surprises people:

- **It is NOT tied to any published chart or to the `world-region-map-definitions` article.** The tooltip is assembled entirely in owid-grapher from `regions.data.ts` + the registries. The `descriptions` text merely *links* to the article (an anchor URL), so that section should exist for the link to land — but the tooltip renders regardless. You do **not** need to publish a chart for hovers to work.
- **It only exists for Path A providers.** `TooltipKey = RegionDataProvider | "incomeGroups" | "continents"`, so suffix-only (Path B) providers get no tooltip — to give them one, promote to Path A.
- **The mini-map's configuration is computed in code, not taken from your ETL metadata or the chart's `customCategoryColors`:**
  - *Membership* (which country → which region) comes from `regions.data.ts` (`getCountriesByRegion`); no-data countries fall back to grey.
  - *Geometry* is owid-grapher's bundled world geojson (`getGeoFeaturesForMap`).
  - *Colors* come from a fixed palette in `getRegionsForKey`: continents use `MapContinentColors`; every other provider uses `CategoricalMapPalette17[index]`, where `index` is the region's position in **`customRegionDisplayOrder[<provider>]`** (the hand-maintained order array in `RegionTooltipData.ts`) — or alphabetical if you omit it.
- **So for a new Path A provider the entire hover is two hand-edits in `RegionTooltipData.ts`** — `descriptions[<provider>]` (text + article link) and optionally `customRegionDisplayOrder[<provider>]` (left-to-right map order, which also fixes the legend order and palette assignment). Colors auto-assign from the palette by that order; you don't (and can't) set them from the ETL side.

### Verify & open the PR

```bash
yarn typecheck          # surfaces any missing label-record entries (the Record<…> types are exhaustive)
yarn fixLintChanged     # lint the changed files; yarn fixFormatChanged to format
```
Confirm the provider appears in `regionGroupLabels` and the relevant label record(s), and that typecheck is clean (a missing entry in a `Record<RegionDataProvider, …>` / `Record<TooltipKey, …>` registry is a compile error — that's your safety net). Open a PR in `owid-grapher` (title like `🔨 update regions file`), with the disclosure blockquote in the body.

---

## Gotchas

- **`.venv/bin/`** for every `python` / `etl` / `etlr` call.
- **No `--force`** — `etlr` rebuilds on edited YAML/code automatically; `--force --only` only when nothing in the repo changed.
- **Grapher DB `%`-LIKE** needs `params={...}` (pymysql treats bare `%` as a format spec). Prefer `OWID_ENV.read_sql(...)` in Python over `make query` for `%`/quoted SQL.
- **`chart_configs.slug`**, not `charts.slug`.
- **Provider aggregates never go in `regions.codes.csv`.**
- If you write any helper that touches Tables, preserve metadata/origins: use `pr.*` (no `pd.concat`/`np.where`), and verify member lists with set-equality rather than eyeballing.
- **owid-grapher is a separate repo and a separate PR**, done *after* the ETL regions are merged & on the prod catalog. `regions.data.ts` is auto-generated (`yarn runRegionsUpdater`) — never hand-edit it; the hand-maintained parts are the label/description registries.
- The grapher `RegionDataProvider` / `RegionGroupKey` / `TooltipKey` types are exhaustive `Record<…>` unions, so a forgotten label is a **typecheck error**, not a silent gap — run `yarn typecheck` to find them.
