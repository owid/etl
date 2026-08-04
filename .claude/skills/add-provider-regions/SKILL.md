---
name: add-provider-regions
description: Add an external provider's regional aggregation (e.g. World Bank, WHO, Maddison, WID, ILO) to OWID's regions dataset — definitions in regions.yml, per-provider grapher map indicators, and metadata — then register it in owid-grapher, including proposing each region's chart color (ContinentColors) and map color (MapContinentColors) for design sign-off. First checks whether the provider's dataset already encodes the regions and their country composition; if not, asks the user for a reference (link/doc) to derive it from. Trigger when the user wants to add/define a provider's world regions, expose "{Provider} regions" on a map, pick or fix the colors of a provider's regions, or migrate an in-dataset region variable to the shared regions dataset.
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

And in **`owid-grapher`** (Step 9, separate PR): the auto-generated `packages/@ourworldindata/utils/src/regions/regions.data.ts`, a few hand-maintained label registries, and the two region-color dictionaries in `packages/@ourworldindata/grapher/src/color/CustomSchemes.ts`. Reference PRs: [owid/owid-grapher#6465](https://github.com/owid/owid-grapher/pull/6465) (IEA regions) and [#6852](https://github.com/owid/owid-grapher/pull/6852) (regions colored by name on categorical maps).

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
        sort:                                    # legend/map order — see ordering rule below
          - <Region> (<Provider>)
          - ...
        origins:
          - *origins_<provider>
        presentation:
          grapher_config:
            hideAnnotationFieldsInTitle:
              time: true                        # hide the placeholder data year in titles
            map:
              colorScale:
                baseColorScheme: OwidCategoricalMap    # name-keyed region colors — see Step 9
                customCategoryLabels:
                  "<Region> (<Provider>)": "<Region>"   # strip the suffix on the legend
                customHiddenCategories:
                  "No data": true
                # No customCategoryColors. Colors live in MapContinentColors (Step 9),
                # keyed by region name; a block here would override them and fork the
                # source of truth.
```

> **Set the palette, don't hardcode the colors.** A categorical map with no `baseColorScheme` falls back to `BuGn` — a sequential green ramp (`MapChartState.ts:53`). Region colors are only looked up by name when the map is on **`OwidCategoricalMap`**, the scheme that carries `colorMap: MapContinentColors`. Setting it on the *indicator* means every chart built on it inherits the palette (the same inheritance that already carries `customCategoryLabels`). A chart's **own** patch still wins over the inherited value, so once the chart exists, confirm on staging that it isn't patched to something else (`world-regions-according-to-pew` is patched to `continents` — a *chart* palette on a map, which pulls the strong colors instead of the muted ones).

> **Ordering rule for `sort`:** the map legend renders as a single row in `sort` order, so order the regions to read **left-to-right across a world map**. The house sweep is:
>
> **(North/Northern) America → Latin America / Caribbean → Africa (Sub-Saharan before Northern, when split) → Middle East / North Africa → Europe → CIS / Russia / Central Asia → South Asia → East and South-East Asia → Australia and New Zealand → Oceania**
>
> Drop what the provider doesn't have, keep the rest in this relative order. Two wrinkles worth knowing: **Europe sits after Africa and the Middle East**, because Europe and Africa share the same longitudes (Europe north, Africa south) so west-to-east alone can't separate them — the convention sweeps the southern band first. And at sub-region granularity, a "Western Asia" that the provider models as *part of Asia* stays in the Asia block rather than moving up to the Middle East slot (compare `un_m49_2` with `ei`).
>
> **The order is defined twice — keep the two equal.** This `sort` drives the published map's legend; `customRegionDisplayOrder[<provider>]` in owid-grapher's `RegionTooltipData.ts` (Step 9) drives the legend of the mini-map in the region hover. When they diverge, the same provider lists its regions in two different orders on the same page. Treat `customRegionDisplayOrder` as the reference and copy it into `sort` verbatim; if you're adding a new provider, write the order once and paste it into both. Colors are keyed by region name (Step 9), so reordering is purely an ordering change — it moves legend entries without touching a single fill.

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
cs = cfg.setdefault("map", {}).setdefault("colorScale", {})
# re-key the labels onto the NEW "(Provider)"-suffixed category values (the new
# variable's categories carry the suffix):
cs["customCategoryLabels"] = {f"{name} (<Provider>)": name for name in OLD_REGION_NAMES}
# put the map on the name-keyed palette and drop any hardcoded colors — the old
# chart's customCategoryColors would override MapContinentColors (Step 9):
cs["baseColorScheme"] = "OwidCategoricalMap"
cs.pop("customCategoryColors", None)
api.update_chart(CHART_ID, cfg)
```

Staging admin writes work behind Tailscale without `ADMIN_API_KEY`. The change surfaces in **chart-diff** for review before it reaches production. Verify by re-reading the config: dimensions point at the new variable, labels are re-keyed, `customCategoryColors` is gone, and the map renders in the muted map colors (chart-diff will show the color change — that's expected, and it's what Step 9 pins).

---

## Step 8 — Commit and open a PR

```bash
make check
git add etl/steps/data/garden/regions/2023-01-01/regions.yml \
        etl/steps/data/grapher/regions/2023-01-01/regions.meta.override.yml \
        etl/steps/data/grapher/regions/2023-01-01/regions.py   # if back-fill added
git commit -m "📊🤖 Add <Provider> regions to regions dataset"
```

If not already on a feature branch, create one and a PR with `etl pr "Add <Provider> regions" data`, then push. In the PR body, open with the disclosure blockquote (`> _Written by Claude <model name> — @<handle> at the wheel._`, model name = the model actually generating the content) and keep any reviewer attribution out of committed code/YAML.

> **Heads-up:** once this merges, the post-merge deploy is **slow** — editing the regions dataset invalidates much of the DAG, so it can take hours for the new regions to reach the production catalog. The owid-grapher follow-up (Step 9) can't start until they do, so don't expect to chain straight into it.

---

## Step 9 — Register the provider in owid-grapher (separate repo + PR)

The grapher frontend keeps its own copy of the regions and a few hand-maintained registries. The provider must be added there too, or its `(Provider)` entities won't be grouped/labelled correctly in entity selectors, map tooltips, and admin presets. Reference: [owid/owid-grapher#6465](https://github.com/owid/owid-grapher/pull/6465) (IEA).

**Sequencing — and expect a long wait:** the frontend's `regions.data.ts` is regenerated from the **production** catalog (`https://catalog.ourworldindata.org/external/owid_grapher/latest/regions/regions.csv`). So do Step 9 **after** the ETL PR (Step 8) is merged *and* the `data://external/owid_grapher/latest/regions` step has rebuilt on prod. **That rebuild is slow — often hours, not minutes** — because editing the regions dataset invalidates a huge swath of the DAG (almost everything that aggregates by region or merges population/regions depends on it), so the post-merge deploy has a lot to rebuild before the new regions reach the catalog. Don't run `yarn runRegionsUpdater` until the regions are actually live there, or it'll regenerate from stale data. Verify first:

```bash
curl -s "https://catalog.ourworldindata.org/external/owid_grapher/latest/regions/regions.csv?nocache" | grep -c "PROVIDER_"
```

A non-zero count means it's ready. If you're waiting, poll this every few minutes rather than running the updater blind.

> **Preview from your ETL branch's staging catalog while you wait.** The updater reads `ETL_REGIONS_URL` if it's set (`devTools/regionsUpdater/update.ts`), and every ETL staging server publishes its own catalog on port 8881 with the same schema as prod. So you can regenerate `regions.data.ts` — and everything downstream of it, including the colors and the test page — before the ETL PR is anywhere near merged:
>
> **The host name is not the branch name — derive it, don't hand-write it.** `etl.config.get_container_name()` replaces `/`, `.` and `_` with `-`, strips a leading `staging-site-`, truncates what's left to **28 characters**, then drops any trailing hyphen. Substituting the raw branch (or truncating it yourself) silently produces a host that is either unreachable or, worse, *another* branch's staging server. Get it from the **etl** repo:
>
> ```bash
> .venv/bin/python -c "from etl.config import get_container_name; print(get_container_name('<etl-branch>'))"
> ```
>
> Then, in **owid-grapher**, use that value (it already carries the `staging-site-` prefix):
>
> ```bash
> ETL_REGIONS_URL="http://<container-name>.tail6e23.ts.net:8881/external/owid_grapher/latest/regions/regions.csv" \
>   yarn runRegionsUpdater
> ```
>
> Use this for previews and for the color review only — before merging the grapher PR, re-run `yarn runRegionsUpdater` with no env var so the committed file is prod-derived (see *Renaming existing regions* for why a stale/hand-edited `regions.data.ts` is a trap).

Work in the `owid-grapher` repo on a new branch. There are two ways to register a provider — `regionGroupLabels` in `RegionGroups.ts` documents the split in its own comments: *"…where we have region definition about what constitutes these regions in regions.ts"* vs *"…we don't have region definitions … (we recognize them by their suffix)"*. They correspond to the `RegionDataProvider` and `AdditionalRegionDataProvider` types. *(Earlier drafts of this skill called these "Path A / Path B" — that's not a codebase term; ignore that wording.)*

### Full-definition provider — a `RegionDataProvider`

The provider's regions (with member countries) live in `regions.data.ts`, so it's a `RegionDataProvider`. This is the natural outcome once you've added the provider to the ETL regions dataset (Steps 4–8), and it's what gives both entity-selector grouping **and** hover tooltips. Use it whenever the regions are formally defined.

1. **Regenerate `regions.data.ts`:**
   ```bash
   yarn runRegionsUpdater
   ```
   This fetches the catalog CSV and rewrites `packages/@ourworldindata/utils/src/regions/regions.data.ts` (auto-generated — never hand-edit). The new `PROVIDER_XXX` aggregates appear with `definedBy: "<provider>"`. The `RegionDataProvider` / `RegionGroupKey` / `TooltipKey` union types are *derived* from this file, so the provider key becomes known to the type system automatically.

2. **Add the hand-maintained labels** (TypeScript will fail to compile until each `Record<RegionDataProvider, …>` has an entry — that's your checklist):
   - `adminSiteClient/EntityPresets.ts` → `REGION_DATA_PROVIDER_LABELS`: `<provider>: "<Short> regions"` (short, for the admin dropdown).
   - `packages/@ourworldindata/grapher/src/core/RegionGroups.ts` → `regionGroupLabels`: `<provider>: "<Provider full name> regions"` (in the *"we have region definitions"* group).
   - `packages/@ourworldindata/grapher/src/seriesLabel/RegionTooltipData.ts` → `descriptions`: `<provider>: "The **<Provider>** defines [N world regions](https://ourworldindata.org/world-region-map-definitions#<anchor>):"` — **embed the article link in the count phrase**, matching the WB/WHO/UN entries. `<anchor>` is the slug of the provider's article-section heading (e.g. `maddison-project-database-maddison`); you must add that section to the article — see **Step 10**. Optionally add a left-to-right map order to `customRegionDisplayOrder` (omit → alphabetical). See *"The region hover"* below for what these edits drive.

### Suffix-only provider — an `AdditionalRegionDataProvider` (e.g. FAO, OECD)

The frontend recognizes the provider's entities purely by the `(Provider)` name suffix; no member definitions in `regions.data.ts`. Use this only when the regions are **not** in the ETL regions dataset — it's lighter, but there's no hover tooltip and no member validation.

- `packages/@ourworldindata/grapher/src/core/GrapherConstants.ts` → add the slug to `ADDITIONAL_REGION_DATA_PROVIDERS` (this defines the `AdditionalRegionDataProvider` type).
- `adminSiteClient/EntityPresets.ts` → `ADDITIONAL_REGION_DATA_PROVIDER_LABELS`: `<provider>: "<Short> regions"`.
- `packages/@ourworldindata/grapher/src/core/RegionGroups.ts` → `regionGroupLabels`: `<provider>: "<Provider full name> regions"` (in the *"recognize by suffix"* group).
- No `RegionTooltipData` entry (its `TooltipKey` only covers full-definition providers).

**Multi-level full-definition providers** define one `RegionDataProvider` *per level* (e.g. `un_m49_1/2/3`; ILO uses `ilo_1`/`ilo_2`) — each level gets its own label, sort order, and tooltip. Mirror the `un_m49_1/2/3` entries in `RegionGroups.ts`.

> **Don't drop the bare suffix slug.** `parseLabel` resolves an entity to a provider by its `(Provider)` name suffix — lowercased, spaces stripped — **not** by `definedBy`. So when the suffix doesn't match the per-level keys (e.g. `"Northern Africa (ILO)"` → `ilo`, not `ilo_1`), the bare slug (`ilo`) must *also* stay registered in `ADDITIONAL_REGION_DATA_PROVIDERS` + its `ADDITIONAL_REGION_DATA_PROVIDER_LABELS` and `regionGroupLabels` entries — it's the recognition handle. Drop it and `providerKey` comes back undefined, which **silently breaks both entity-selector grouping and the hover** even though the per-level definitions exist. Keep it alongside the per-level providers, exactly as `unm49` sits beside `un_m49_1/2/3`. Single-tier providers whose suffix already equals their key (Maddison → `maddison`, WID → `wid`) need no extra entry.

### The region hover (tooltip) — full-definition providers only, fully data-driven

When a Grapher chart plots a region entity (e.g. `"Sub-Saharan Africa (ILO)"`) as a series, hovering its label shows a tooltip with a description, a mini world map, and a legend (`RegionTooltip.tsx` → `RegionMap.tsx`). Worth understanding because it surprises people:

- **It is NOT tied to any published chart** and renders entirely from `regions.data.ts` + the registries — you don't need to publish a chart for hovers to work. The `descriptions` text *does* link into the `world-region-map-definitions` article (an anchor), so **add the matching section to that article (Step 10)** or the link lands on the page top. The tooltip itself renders regardless.
- **It only exists for full-definition providers.** `TooltipKey = RegionDataProvider | "incomeGroups" | "continents"`, so suffix-only providers (`AdditionalRegionDataProvider`) get no tooltip — to give one, add the provider's definitions to `regions.data.ts`.
- **The mini-map's configuration is computed in code, not taken from your ETL metadata or the chart's `customCategoryColors`:**
  - *Membership* (which country → which region) comes from `regions.data.ts` (`getCountriesByRegion`); no-data countries fall back to grey.
  - *Geometry* is owid-grapher's bundled world geojson (`getGeoFeaturesForMap`).
  - *Colors* come from `getRegionsForKey`, which looks up **`MapContinentColors[regionName]` first** and only falls back to `CategoricalMapPalette17[index]` for regions that aren't pinned (where `index` is the region's position in **`customRegionDisplayOrder[<provider>]`** in `RegionTooltipData.ts` — or alphabetical if you omit it). Pin the provider's regions (next section) and the hover follows automatically.
- **So for a full-definition provider the hover is two required hand-edits in `RegionTooltipData.ts`** — `descriptions[<provider>]` (text + article link) and optionally `customRegionDisplayOrder[<provider>]` (left-to-right map order, which fixes the legend order and the fallback palette assignment) — **plus the color step below.** You can't set the hover's colors from the ETL side (it ignores the chart's `customCategoryColors`); they come from `MapContinentColors` in `CustomSchemes.ts`.
- **Which map a region shows is its own `defined_by` tier**, because `regionIconInfo` returns `tooltipKey: region.definedBy` (in `SeriesLabelState.ts`) — *not* the name suffix. So for a multi-tier provider each tier needs its own `descriptions[<provider>_1]` / `descriptions[<provider>_2]` entry, and a region tagged `<provider>_1` always hovers to the level-1 map, one tagged `<provider>_2` to the level-2 map.

> **Shared-region caveat (multi-tier providers).** A region that exists in two tiers carries a *single* `defined_by`, so its hover only ever shows that one tier's map — the indicator-level back-fill (Step 3 / Step 6c) populates the *other* tier's map indicator but does **not** give the entity a second `defined_by` in `regions.data.ts`. Concrete consequence: in a chart built on the level-2 indicator, the shared region hovers to the level-1 map while its level-2 siblings hover to the level-2 map. It's not wrong (the level-1 map still highlights it), but the "belongs to a set of N regions" framing differs for that one entity. There's no way to make it show *both* — tagging it the other tier just flips which map it shows. (e.g. ILO's Arab States, tagged `ilo_1`, always hovers to the 5-region broad map.)

> **And it leaves a grey hole in the *other* tier's hover map — fix with a frontend back-fill.** Because the shared region is absent from the other tier's provider set in `regions.data.ts` (`getAggregatesByProvider` filters by exact `definedBy`), its member countries are unmapped there and render grey (no-data) whenever you hover *another* region of that tier. Concretely: hovering any `ilo_2` subregion greys out the 12 Arab States countries, and the legend shows 10 regions instead of 11 — even though the ETL `ilo_2_region` *indicator* (and its published map) shows Arab States colored, because the ETL back-fill never reaches `regions.data.ts`. Mirror that back-fill **centrally**, in `getAggregatesByProvider` (`regionsUtils.ts`), via a data-driven map — `PROVIDER_REGION_BACKFILLS = { ilo_2: ["Arab States (ILO)"] }`, appended to the direct `definedBy` matches. Do it *there*, not in `getRegionsForKey` — `getAggregatesByProvider` feeds *every* consumer of the sub-tier (the hover **and** the admin entity presets, …), so a fix in `getRegionsForKey` alone leaves the presets still dropping the region (a real Codex catch). Add the shared region to `customRegionDisplayOrder[<tier>]` for a stable legend slot. Then that tier's hover is a complete partition matching the published map. (The shared region itself still hovers to its home tier — this only fills the hole the *other* regions' map would otherwise have.)

### Region colors — two dictionaries, two jobs

**Colors are not an ETL artifact.** Nothing you can write in `regions.meta.override.yml` sets a region's color. The ETL side sets exactly one thing — `baseColorScheme: OwidCategoricalMap` (Step 6b), which decides that colors are looked up *by region name*. The color values themselves are TypeScript in owid-grapher, so the whole color conversation happens in the Step 9 PR, on an owid-grapher branch:

| Where | What it can show | Good for |
|---|---|---|
| ETL staging (`staging-site-<etl-branch>`) | the region chart with **fallback** colors — muted map palette handed out in legend order, because the names aren't pinned yet | checking the *indicator*: membership, legend order, labels. **Not** the color proposal |
| owid-grapher staging (`staging-site-<grapher-branch>`) → `/admin/test-region-maps` | the real thing: your pinned map colors on a map, beside the chart colors on a line chart | **this is where you present the colors** |
| Production `/admin/test-region-maps` | every provider as currently deployed | picking which provider to mirror, before you write a line |

So the answer to *"where do I show the colors while I'm working in ETL?"* is: you don't — you open the grapher branch and show them from its staging server. You don't have to wait for the ETL PR to merge to do that; regenerate `regions.data.ts` from your ETL branch's staging catalog (see the `ETL_REGIONS_URL` note at the top of Step 9), push the grapher branch, and its test page renders the new provider with your proposed colors. Re-run the updater against prod before that PR merges.

Both dictionaries live in `packages/@ourworldindata/grapher/src/color/CustomSchemes.ts`, and every provider needs an entry in **both**:

| Dictionary | For | Vocabulary | Backs |
|---|---|---|---|
| `ContinentColors` | **charts** (lines, bars, scatters, slope) — regions need strong colors to read as series | `OwidDistinctColors` | the `continents` scheme ("Continents" / "Continents (Lines)") and the **🌍 Regions** tab of the admin color picker |
| `MapContinentColors` | **maps** — muted colors, a deliberate design decision (maps look better light, charts need strong) | `OwidMapColors` | the `OwidCategoricalMap` scheme ("OWID Categorical Map") and the region hover tooltips |

Colors are looked up **by region name**, not by legend position (`ColorScale.ts` picks `colorScheme.colorMap[value]` before falling back to the positional palette), so the assignment is stable no matter how the `sort` order changes later. Precedence, highest first: a chart's/indicator's `customCategoryColors` → the scheme's `colorMap` → the positional palette. That's why Step 6b bans `customCategoryColors`: it silently defeats everything below it.

> **The two are not interchangeable, and neither one covers for the other.** `MapContinentColors` spreads `...ContinentColors`, so a region added to only `ContinentColors` still renders on maps — in the *strong chart color*, which is the thing the muted map vocabulary exists to avoid. And the admin color picker reads `Object.entries(ContinentColors)` directly, so a region added to only `MapContinentColors` never appears there. Always add both blocks, in the same region order, each headed `// <Provider> regions`.

**Pick the colors by analogy with what's already curated.** The hues below are the house convention, read off the providers already in the file — a region should get roughly the same color everywhere it appears:

| Region concept | Chart (`OwidDistinctColors`) | Map (`OwidMapColors`) |
|---|---|---|
| Americas / North(ern) America | `Peach` #e56e5a | `SoftOrange` #CC7641 |
| Latin America / South America / Caribbean | `Maroon` #883039 | `MutedCherry` #B04E74 |
| Europe (broad, incl. Northern Europe) | `Denim` #4c6a9c | `MutedDenim` #526F9B |
| Eastern Europe / CIS / Eurasia | `MidnightBlue` #00295b | `LightDenim` #92D3DE |
| Africa (whole) | `Mauve` #a2559c | `LightPurple` #A07AB8 |
| Sub-Saharan Africa | `DarkMauve` #8c4569 | `LightPurple` #A07AB8 |
| Northern Africa | `Purple` #6d3e91 | `SoftPurple` #77538F |
| Middle East / MENA / Arab States / Western Asia | `Camel` #bc8e5a | `Sand` #C3A27C |
| Asia (whole) / Asia-Pacific | `Teal` #00847e | `MutedTeal` #238A84 |
| East Asia | `TealishGreen` #00875e or `Lime` #3b8e1d | `LeafGreen` #6FA54F |
| South-East Asia / Asia Pacific (energy providers) | `Lime` #3b8e1d | `LeafGreen` #6FA54F |
| South Asia / Central and Southern Asia | `OliveGreen` #578145 | `Olive` #5B6D35 |
| Central Asia | `LightTeal` #58ac8c | `LightTeal` #4FB2AC |
| Oceania | `Turquoise` #38aaba | `SkyTurquoise` #5FB8C8 |
| Australia and New Zealand | `Teal` #00847e | `MutedTeal` #238A84 |

- **Mirror the nearest already-curated provider.** Before consulting the table, find the provider whose regions are closest in *composition and count* and copy its assignment wholesale, noting it in the comment — the file already does this (`// IEA regions (mirroring the Energy Institute regions)`, `// FAO SDG regions (mirroring the UN SDG regions)`). Two providers that carve the world the same way should look the same.
- **No two regions of one provider may share a color.** Where the table would collide, move to a neighboring hue or an extended map color, and prefer keeping the *broadest* region on the canonical hue. Precedents: UN M49 pushes `South-eastern Asia` to `DarkOrange`/`LightOrange` to clear `Eastern Asia`'s green; Maddison's `East Asia` is `Copper`/`LightCherry` because its greens are taken by `South and South East Asia`.
- **Multi-tier providers get pinned too.** Because colors are name-keyed, a region shared across tiers (e.g. `Arab States (ILO)`, tagged `ilo_1` but back-filled into the `ilo_2` map) carries *one* color that is correct in both tiers. Pin every region of every tier; check the tiers for collisions independently, since each tier is its own legend.
- Use the **named constants**, never raw hex — `"<Region> (<Provider>)": OwidDistinctColors.Peach` / `OwidMapColors.SoftOrange`. `OwidMapColors` is declared above both dictionaries, so it's available to each.

### Check the colors on the region-maps test page

The admin has a page that renders every provider's regions as a real map next to a fake line chart — i.e. `MapContinentColors` and `ContinentColors` side by side. Use it before showing the colors to anyone:

- **This branch:** `http://<container-name>/admin/test-region-maps` (owid-grapher staging, once the branch has built). Same naming rule as above — derive `<container-name>` from the *grapher* branch with `get_container_name()`, never by pasting the branch in raw.
- **Production:** <https://admin.owid.io/admin/test> → the **Region maps** bullet — direct link <https://admin.owid.io/admin/test-region-maps>

The page splits into *Providers with hard-coded region colors* and *Providers without*. **Your provider must land in the first section**; if it doesn't, a region is missing from one of the two dictionaries (the check requires the name in *both*). Then compare it against the provider you mirrored: same hues in the same places, no two regions of the tier reading as the same color, and the map muted where the line chart is strong.

### Verify & open the PR

```bash
yarn typecheck          # surfaces any missing label-record entries (the Record<…> types are exhaustive)
yarn fixLintChanged     # lint the changed files; yarn fixFormatChanged to format
```
Confirm the provider appears in `regionGroupLabels` and the relevant label record(s), and that typecheck is clean (a missing entry in a `Record<RegionDataProvider, …>` / `Record<TooltipKey, …>` registry is a compile error — that's your safety net). Open a PR in `owid-grapher` (title like `🔨 update regions file`), with the disclosure blockquote in the body.

### Get the colors signed off

Colors are a design call, and what the skill produces is a **first stab** — it always goes to a human. All of this happens on the **owid-grapher** PR, not the ETL one:

1. **Open the PR** (above) with the proposed colors in the body: a `region → chart color → map color` table (constant names *and* hex), the provider you mirrored, and the link to **this branch's** test page — `http://<container-name>/admin/test-region-maps`, with `<container-name>` derived as above. Wait for the staging server to build before sharing the link; open it yourself first.
2. **Ask Marwa** — draft a Slack message to `@mrwbkrm` with that same table and link, asking her to look at the rendered maps on the test page (point at the *"Providers with hard-coded region colors"* section). Don't request the code review yet.
3. **Apply her changes**, push, and only then **request review from Sophia** — `gh pr edit <n> --add-reviewer sophiamersmann`.
4. Both the PR body and the Slack message carry the attribution blockquote (`CLAUDE.md` → Team).

> **You can start this before the ETL PR merges** — the color review only needs `regions.data.ts` to know the new provider, and the `ETL_REGIONS_URL` preview gives you that from your ETL branch's staging catalog. What you must **not** do is merge the grapher PR on a staging-derived `regions.data.ts`: re-run `yarn runRegionsUpdater` against prod once the regions are live there, and push that regeneration as the last commit. The colors don't change in that regeneration — only the region data does.

### Renaming existing regions (not a fresh add)

Occasionally you're not adding a provider but **renaming** its already-published regions (e.g. WID's `MENA (WID)` → `Middle East and North Africa (WID)`, or `&` → `and`). The Step 9 flow is the same, with two wrinkles:

- **Finish with `runRegionsUpdater` from prod — never merge on a hand-edit.** Renaming a region's `name` shifts the derived name / `RegionDataProvider` union types, so it's tempting to hand-edit the `name`s directly in `regions.data.ts` to keep `yarn typecheck` green before the prod catalog has rebuilt. That stopgap is **always incomplete**: the updater also regenerates each region's `shortName` and `slug` *from* the name, which a hand-edit silently misses (on the WID rename the stopgap kept the stale `mena-wid` slug and dropped MENA's `shortName`). Once the rename is live on prod, run `runRegionsUpdater` and let it overwrite the file — the diff exposes any derived field the stopgap got wrong. Treat the regenerated file as the source of truth, not the hand-edit.
- **Ignore the "set up redirects" message for provider regions.** The updater unconditionally prints *"Be sure to set up redirects for any slugs that have changed"* on **any** change to `regions.data.ts`. But a `slug` only drives a URL for a **country** profile page (`/country/<slug>`, baked only for `regionType: "country"` — see `countries` in `regionsUtils.ts`). Provider regions are `regionType: "aggregate"` and have no country page, so their slug changes need **no** redirect. Only act on that warning if a real *country's* slug changed.

---

## Step 10 — Add the provider to the world-region-map-definitions article

The [world-region-map-definitions](https://ourworldindata.org/world-region-map-definitions) article is the public, human-readable home for every provider's regions — and the target of the hover `descriptions` links (Step 9) and indicator text. Add a section for the new provider there.

- It's an OWID **gdoc/article**, not in this repo — edit it in the OWID admin/gdocs (a manual editorial step), not via ETL.
- Add a section headed `<Provider full name> (<PROVIDER>)` — e.g. *"Maddison Project Database (Maddison)"* — usually with the provider's region map and a short description of the grouping.
- **The section anchor must match the link** used in `descriptions` (and any indicator text). The anchor is the heading slugged: lowercase, spaces → hyphens, parentheses dropped:
  - `Maddison Project Database (Maddison)` → `#maddison-project-database-maddison`
  - `World Inequality Database (WID)` → `#world-inequality-database-wid`
  - `International Labour Organization (ILO)` → `#international-labour-organization-ilo`
- For a multi-tier provider, both tiers' hovers can point at the **same** section (e.g. `ilo_1` and `ilo_2` both → `#international-labour-organization-ilo`).
- Open the link once the section is published to confirm the anchor resolves.

---

## Gotchas

- **`.venv/bin/`** for every `python` / `etl` / `etlr` call.
- **No `--force`** — `etlr` rebuilds on edited YAML/code automatically; `--force --only` only when nothing in the repo changed.
- **Grapher DB `%`-LIKE** needs `params={...}` (pymysql treats bare `%` as a format spec). Prefer `OWID_ENV.read_sql(...)` in Python over `make query` for `%`/quoted SQL.
- **`chart_configs.slug`**, not `charts.slug`.
- **Provider aggregates never go in `regions.codes.csv`.**
- If you write any helper that touches Tables, preserve metadata/origins: use `pr.*` (no `pd.concat`/`np.where`), and verify member lists with set-equality rather than eyeballing.
- **owid-grapher is a separate repo and a separate PR**, done *after* the ETL regions are merged & on the prod catalog. `regions.data.ts` is auto-generated (`yarn runRegionsUpdater`) — never hand-edit it; the hand-maintained parts are the label/description registries and the two color dictionaries.
- **Region colors are keyed by name, not by legend position.** A map without `baseColorScheme: OwidCategoricalMap` falls back to `BuGn`, and any `customCategoryColors` block overrides the name lookup entirely — both silently, with a chart that still renders.
- **`MapContinentColors` spreads `ContinentColors`**, so a region added to only one dictionary looks right on the test page's line chart and wrong on its map (strong chart color where a muted one belongs). Add both.
- **Colors always go to a human** (`@mrwbkrm` for the design call, `@sophiamersmann` for the code review) — the skill's proposal is a first stab, never the final word.
- The grapher `RegionDataProvider` / `RegionGroupKey` / `TooltipKey` types are exhaustive `Record<…>` unions, so a forgotten label is a **typecheck error**, not a silent gap — run `yarn typecheck` to find them.
