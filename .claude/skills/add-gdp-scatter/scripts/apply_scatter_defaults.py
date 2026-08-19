"""Apply scatter-view defaults to OWID Grapher charts via the admin API.

Reads a JSON list of `{chart_admin_url, target_chart_admin_url, gdp_source}` from
stdin and, for each row:

- Adds `ScatterPlot` to the target's `chartTypes` (preserving existing tabs).
- Appends x (GDP per capita), color, size dimensions if absent. If the source
  scatter uses a non-default color/size variable (e.g. WB income groups, or a
  historical population series), the source's variableId is used on the target
  instead of the admin default.
- Sets `matchingEntitiesOnly: true`.
- Sets `xAxis` to log scale with `canChangeScaleType: true`.
- Enables the y-axis log *toggle* (canChangeScaleType) if the source scatter is
  log — but leaves the default linear, since yAxis is shared with the line/bar views.
- Mirrors the source's explicit `yAxis` min/max bounds if set.
- Mirrors the source's manually-set y `display.name` if present.
- Emits warnings (no action) for: source `excludedEntityNames`; GDP-coverage
  mismatch vs the y-indicator's earliest year; few entities visible on scatter
  + source has a higher y-dim tolerance.

Output: two TSV-style tables to stdout. First, per-row actions/warnings. Then a
display-name comparison table (manual vs ETL `display.name` vs catalog
`variable.name`) so the caller can decide whether to drop redundant manual
overrides.
"""

import json
import math
import re
import sys
from collections.abc import Iterable
from typing import Any

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.grapher.io import variable_data_df_from_s3

GDP_SOURCES = {
    # World Bank: WDI 2026-07-27. Bumped from 1204826 (WDI 2026-02-27) on 2026-08-04 —
    # every published "X vs. GDP per capita" scatter already plots this id, so a target
    # pinned to the older one would disagree with the source it was migrated from.
    "world bank": 1294305,
    "wdi": 1294305,
    "maddison": 900793,
    "maddison project database": 900793,
    "pwt": 1108541,
    "penn world table": 1108541,
}
GDP_LABEL = {1294305: "World Bank", 900793: "Maddison", 1108541: "PWT"}
GDP_COVERAGE = {1294305: 1990, 1108541: 1950, 900793: 1}

# catalogPath patterns used to detect when a newer version of each GDP-per-capita
# indicator has landed in the catalog since this script was last updated.
GDP_CATALOG_PATTERNS = {
    1294305: "grapher/worldbank_wdi/%/wdi/wdi#ny_gdp_pcap_pp_kd",
    900793: "grapher/ggdc/%/maddison_project_database/maddison_project_database#gdp_per_capita",
    1108541: "grapher/ggdc/%/penn_world_table/penn_world_table#rgdpo_pc",
}

# Every GDP-per-capita variable we might meet on a source axis, current or superseded. Used
# to spot a REVERSED source (GDP on its y instead of its x): the target query admits those,
# but this script's y-oriented mirroring — y display.name, yAxis bounds, comparisonLines —
# assumes the source's y is the non-GDP indicator, so on a reversed source it would copy
# GDP-axis settings onto the target's unrelated indicator, and onto its other views too.
ANY_GDP_VARIABLE_IDS = {1294305, 1204826, 900793, 1108541}

CONTINENTS_ID = 900801

# An entity the source excluded is graded by how much including it would stretch each axis,
# in that axis's OWN units — because the two axes are drawn differently: the target's x is
# always log (step 4) while its y is always linear (step 5). So a very high GDP per capita
# costs a fraction of a decade and is usually invisible, while a y outlier costs a multiple
# of the whole span. Thresholds are deliberately about visible effect, not about statistical
# outlierness: a symmetric IQR fence gets positive skewed indicators badly wrong (chart 1131,
# cereal yield: Cape Verde at 40 kg/ha against a pack of 577-13,340 sits well inside a
# +/-3*IQR fence, yet it is 14x below the lowest country).
AGGREGATE_CODE_PREFIX = "OWID_"  # OWID-defined aggregates; real countries carry an ISO-3 code
Y_STRETCH_FACTOR = 1.2  # including it widens the linear y span by >= this
Y_PACK_FACTOR = 2.0  # ... or it sits this many times outside every other entity
X_MATERIAL_DECADES = 0.3  # extra log-x width that shows even on a log axis

# Which exclusion classes are a possible defect (a warning) rather than expected-and-handled
# (context). Owned here so `build_review.py` splits its two boxes on the same rule.
EXCLUSION_WARN_CLASSES = frozenset({"y-OUTLIER", "high-GDP-material", "aggregate", "unclear", "ungradeable"})
POPULATION_ID = 953899  # "Population" — the default sizing indicator (-10000..2100)

LINE_FAMILY = {"LineChart", "SlopeChart", "DiscreteBar", "Marimekko", "ScatterPlot"}
STACKED_FAMILY = {"StackedArea", "StackedBar", "StackedDiscreteBar"}
# Chart types that draw bars/areas from a baseline — they need a ZERO y-axis min,
# so a non-zero min tuned for the scatter must not be mirrored onto them.
# DiscreteBar is deliberately NOT here: `DiscreteBarChart.yAxisConfig` hardcodes
# `min: undefined` and anchors at `x0 = 0`, so it ignores `yAxis.min` outright ("the
# author-configured minimum is usually intended for the line chart"). Withholding the
# source's min from a DiscreteBar target protects nothing and costs the scatter a
# well-fitted axis. Of the rest, only these override nothing and so really do respect it.
BAR_AREA_FAMILY = {"Marimekko", "StackedArea", "StackedBar", "StackedDiscreteBar"}
# Tabs that can ONLY show a single time (grapher's checkOnlySingleTimeSelectionPossible).
# Dumbbell is deliberately excluded: it is single-time only with >=2 y indicators and needs
# a RANGE with one, so treat it as range-capable.
SINGLE_TIME_ONLY_FAMILY = {"DiscreteBar", "StackedDiscreteBar", "Marimekko"}
SCHEMA_DEFAULT_CHART_TYPES = ["LineChart", "DiscreteBar"]

CHART_ID_RE = re.compile(r"/charts/(\d+)")
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")


def short_admin_host() -> str:
    """Return OWID_ENV.admin_api with the tailscale suffix stripped."""
    return TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).rstrip("/").removesuffix("/api")


def edit_link(chart_id: int) -> str:
    return f"{short_admin_host()}/charts/{chart_id}/edit"


_data_cache: dict[int, Any] = {}
_pop_variant_cache: dict[int, bool] = {}


def is_population_variant(var_id: int) -> bool:
    """True if the variable is some flavour of a population count.

    Reference scatters size bubbles by various population series (regular,
    historical, UN WPP, etc.). For bubble sizing they're interchangeable, so we
    always collapse them to the default Population indicator. Detected by the
    variable's name starting with "Population" or its catalogPath living under a
    population dataset.
    """
    if var_id == POPULATION_ID:
        return True
    if var_id in _pop_variant_cache:
        return _pop_variant_cache[var_id]
    df = OWID_ENV.read_sql(
        "SELECT name, catalogPath FROM variables WHERE id = %(v)s",
        params={"v": int(var_id)},
    )
    if df.empty:
        result = False
    else:
        name = (df.iloc[0]["name"] or "").strip().lower()
        path = (df.iloc[0]["catalogPath"] or "").lower()
        result = name.startswith("population") or "/population/" in path
    _pop_variant_cache[var_id] = result
    return result


def chart_id_from_url(url: str) -> int:
    m = CHART_ID_RE.search(url)
    if not m:
        raise ValueError(f"Could not extract chart id from {url!r}")
    return int(m.group(1))


def resolve_gdp_source(label: str) -> int:
    needle = label.strip().lower()
    if needle in GDP_SOURCES:
        return GDP_SOURCES[needle]
    for k, v in GDP_SOURCES.items():
        if k in needle:
            return v
    raise ValueError(f"Unknown gdp_source {label!r}; pick World Bank / Maddison / PWT")


def find_dim(cfg: dict, prop: str) -> dict | None:
    for d in cfg.get("dimensions", []) or []:
        if d.get("property") == prop:
            return d
    return None


def load_var_data(var_id: int, engine):
    if var_id in _data_cache:
        return _data_cache[var_id]
    df = variable_data_df_from_s3(engine, variable_ids=[var_id], workers=1, value_as_str=False)
    _data_cache[var_id] = df
    return df


def year_range(var_id: int, engine) -> tuple[int, int] | None:
    df = load_var_data(var_id, engine)
    if df is None or df.empty:
        return None
    return int(df["year"].min()), int(df["year"].max())


def values_at_year(var_id: int, year: int, tolerance: int, engine) -> dict[str, float]:
    """Each entity's value at `year`, nearest year within tolerance winning.

    Nearest-wins is Grapher's own tolerance resolution, so this is the value the reader
    actually sees on the scatter rather than an arbitrary one from the window.
    """
    df = load_var_data(var_id, engine)
    if df is None or df.empty:
        return {}
    in_window = df[(df["year"] >= year - tolerance) & (df["year"] <= year + tolerance)].copy()
    if in_window.empty:
        return {}
    in_window["_gap"] = (in_window["year"] - year).abs()
    nearest = in_window.sort_values(["entityName", "_gap"]).groupby("entityName", observed=True).first()
    return {str(k): float(v) for k, v in nearest["value"].items() if v == v}


def entities_at_year(var_id: int, year: int, tolerance: int, engine) -> set[str]:
    return set(values_at_year(var_id, year, tolerance, engine))


def entity_codes(var_id: int, engine) -> dict[str, str]:
    """entityName -> entityCode, off the frame `load_var_data` already holds.

    `variable_data_df_from_s3` joins `entityCode` in alongside `entityName`, so telling an
    OWID aggregate from a country costs no query of its own.
    """
    df = load_var_data(var_id, engine)
    if df is None or df.empty or "entityCode" not in df.columns:
        return {}
    pairs = df[["entityName", "entityCode"]].drop_duplicates()
    return {str(r["entityName"]): str(r["entityCode"] or "") for r in pairs.to_dict("records")}


def grade_exclusion(
    y: float | None,
    x: float | None,
    others_y: list[float],
    others_x: list[float],
    code: str | None,
) -> tuple[str, str]:
    """(class, why) for one entity the source excluded. Pure, so it is checkable without a DB.

    The question is not whether the point is a statistical outlier but whether putting it back
    changes what the reader sees — so each axis is measured in the units it is drawn in.
    """
    # Missing data comes FIRST, including for an aggregate: `aggregate`'s claim is that the entity
    # "renders as one point among the countries", and an entity with no y/GDP pair renders nowhere
    # — `matchingEntitiesOnly` hides it. Testing the code first turned such an aggregate into a
    # warning, and so into a RECONSIDER row, on the strength of a sentence that was not true of it.
    if y is None or x is None:
        return "no data", "no year has both a y and a GDP value, so matchingEntitiesOnly hides it regardless"
    if (code or "").startswith(AGGREGATE_CODE_PREFIX):
        return "aggregate", f"{code} is an OWID aggregate — it renders as one point among the countries"
    if not others_y or not others_x:
        return "ungradeable", "no other entity has both values here, so there is no pack to compare against"

    lo_y, hi_y = min(others_y), max(others_y)
    span = hi_y - lo_y
    if span > 0:
        stretch = (max(hi_y, y) - min(lo_y, y)) / span
    else:
        # A pack with no spread at all — every other entity on the same value. Stretch is then a
        # ratio over zero, so it is only meaningful as a yes/no: an entity ON that value changes
        # nothing (1.0), any other value turns a zero-width axis into a real one (unbounded).
        # Without the first case an entity sitting exactly on the pack graded `y-OUTLIER`.
        stretch = 1.0 if y == hi_y else float("inf")
    # The two pack tests are RATIOS, so they only mean anything against a positive bound: on a
    # negative pack `hi_y * 2` sits BELOW the pack, which made an ordinary in-range value (y=-7
    # against -10..-5) read as "above the highest", and a pack topping out at exactly 0 divided
    # by zero in the factor below. Indicators with negative or zero values are graded on `stretch`
    # alone, which is a span ratio and so sign-agnostic.
    above = hi_y > 0 and y > hi_y * Y_PACK_FACTOR
    below = lo_y > 0 and 0 < y < lo_y / Y_PACK_FACTOR
    if stretch >= Y_STRETCH_FACTOR or above or below:
        why = f"y={y:,.4g} against the other {len(others_y)} at {lo_y:,.4g}-{hi_y:,.4g}"
        if above or below:
            factor = y / hi_y if above else lo_y / y
            why += f" — {factor:,.1f}x {'above the highest' if above else 'below the lowest'}"
        if math.isinf(stretch):
            why += (
                "; they all sit on one value, so it turns a zero-width y axis into a real range, and yAxis.max "
                "is global so the scatter cannot cap it alone"
            )
        elif stretch >= Y_STRETCH_FACTOR:
            why += f"; stretches the y axis {stretch:.1f}x, and yAxis.max is global so the scatter cannot cap it alone"
        return "y-OUTLIER", why

    positive = [v for v in others_x if v > 0]
    if not positive or x <= 0:
        return "unclear", f"y={y:,.4g} sits inside the pack and x=${x:,.0f} cannot be placed on a log axis"
    before = math.log10(max(positive) / min(positive))
    after = math.log10(max(max(positive), x) / min(min(positive), x))
    decades = after - before
    if decades <= 0:
        return (
            "unclear",
            f"y={y:,.4g} and x=${x:,.0f} sit with the other {len(others_y)} — putting it back stretches y "
            f"{stretch:.2f}x and widens log x by nothing, so the exclusion was editorial rather than about "
            f"the chart's shape",
        )
    if decades < X_MATERIAL_DECADES:
        # Benign is a claim about the AXIS, which is the reason a wide-GDP point normally gets
        # excluded and the reason a log x axis undoes it. It is not a claim that the figure is
        # sound: Ireland's GDP is inflated by profit shifting and a Gulf state's by its expat
        # denominator, so an author may have excluded one as bad data rather than as a bad shape.
        return (
            "high-GDP",
            f"x=${x:,.0f} vs the highest other ${max(positive):,.0f} — only +{decades:.2f} of a decade on the "
            f"target's log x axis (y stretch {stretch:.2f}x), so it costs the axis nothing; check the author did "
            f"not mean the GDP figure itself is distorted",
        )
    return (
        "high-GDP-material",
        f"x=${x:,.0f} vs the highest other ${max(positive):,.0f} — +{decades:.2f} of a decade, wide enough to "
        f"show even on a log x axis",
    )


def classify_exclusions(
    excluded: list[str],
    y_var_id: int,
    gdp_var_id: int,
    year: int,
    tolerance: int,
    engine,
) -> list[dict]:
    """Why each entity the source excluded was probably excluded, and whether it matters.

    The target never inherits `excludedEntityNames` (they are global, so they would also hide
    the entity from the line/bar/map views), which means every one of these entities reappears
    on the migrated scatter. Two of the reasons an author excludes one have opposite
    consequences here: a **y** outlier is weird non-GDP data and its return is a real defect,
    while a merely **very high GDP per capita** is harmless because the target's x axis is
    logarithmic — so grading them apart is what keeps the warning worth reading.

    Lives here, and is imported by the reviewer and the redirect script, for the same reason
    `log_y_axis_sources` does: getting it right in one script and forgetting it in another is
    exactly how this went wrong once.
    """
    codes = {**entity_codes(gdp_var_id, engine), **entity_codes(y_var_id, engine)}
    packs: dict[int, tuple[list[float], list[float]]] = {}

    def pack_at(yr: int) -> tuple[list[float], list[float]]:
        """The peer distribution as the chart draws it at `yr`, memoized per year.

        Rebuilt per year rather than computed once, because an entity graded at a fallback year
        has to be compared against the pack AT THAT YEAR: for a trending indicator, holding the
        pack at the default year while moving the point measures the trend instead of the
        entity, which flips the verdict either way. Cheap — `values_at_year` reads the frame
        `load_var_data` already holds, so no extra query.
        """
        if yr not in packs:
            ys = values_at_year(y_var_id, yr, tolerance, engine)
            xs = values_at_year(gdp_var_id, yr, tolerance, engine)
            names = sorted((set(ys) & set(xs)) - set(excluded))
            packs[yr] = ([ys[n] for n in names], [xs[n] for n in names])
        return packs[yr]

    y_at = values_at_year(y_var_id, year, tolerance, engine)
    x_at = values_at_year(gdp_var_id, year, tolerance, engine)

    # An entity absent at the default year may still be an outlier the reader meets by dragging
    # the timeline, so fall back to its latest year with both values rather than calling it moot.
    y_all = load_var_data(y_var_id, engine)
    x_all = load_var_data(gdp_var_id, engine)

    rows = []
    for name in excluded:
        y, x, used, exact = y_at.get(name), x_at.get(name), year, True
        if y is None or x is None:
            shared = _latest_shared_year(y_all, x_all, name, tolerance)
            if shared is not None:
                # Read with the same tolerance the year was found under: at a non-zero tolerance
                # `shared` can be a timeline year whose actual observation sits a year or two off,
                # and a tolerance-0 read would then come back empty.
                used, exact = shared, False
                y = values_at_year(y_var_id, shared, tolerance, engine).get(name)
                x = values_at_year(gdp_var_id, shared, tolerance, engine).get(name)
            else:
                y = x = None
        others_y, others_x = pack_at(used) if y is not None and x is not None else ([], [])
        cls, why = grade_exclusion(y, x, others_y, others_x, codes.get(name))
        rows.append({"entity": name, "cls": cls, "why": why, "year": used if y is not None else None, "exact": exact})
    return rows


def _latest_shared_year(y_df, x_df, entity: str, tolerance: int = 0) -> int | None:
    """Latest timeline year where BOTH indicators resolve for this entity, tolerance included.

    Not a raw-year intersection: with a non-zero tolerance Grapher pairs a y value with a GDP
    value from a neighbouring year, so demanding the same year understated which entities the
    reader can actually reach by dragging the timeline — and an entity wrongly found absent
    graded as a benign `no data` and dropped out of the warning entirely.
    """
    if y_df is None or x_df is None or y_df.empty or x_df.empty:
        return None
    ys = {int(v) for v in y_df.loc[y_df["entityName"] == entity, "year"].unique()}
    xs = {int(v) for v in x_df.loc[x_df["entityName"] == entity, "year"].unique()}
    if not ys or not xs:
        return None
    # Candidates are every year the two variables cover, not just this entity's own observation
    # years: the year that PAIRS them can be one where the entity itself has neither. y in 2000
    # and GDP in 2002 at tolerance 1 meet at 2001 — on the timeline because other entities have
    # data there, and a year the reader reaches by dragging the handle. Searching only {2000, 2002}
    # found nothing and sent the entity back as a benign `no data`.
    timeline = {int(v) for v in y_df["year"].unique()} | {int(v) for v in x_df["year"].unique()}
    for yr in sorted(timeline, reverse=True):
        if any(abs(y - yr) <= tolerance for y in ys) and any(abs(x - yr) <= tolerance for x in xs):
            return yr
    return None


def coverage_warning(y_min_year: int, gdp_var_id: int) -> str | None:
    gdp_start = GDP_COVERAGE.get(gdp_var_id)
    if gdp_start is None or y_min_year >= gdp_start:
        return None
    suggestions = [GDP_LABEL[v] for v, s in GDP_COVERAGE.items() if s <= y_min_year and v != gdp_var_id]
    if not suggestions:
        return None
    return (
        f"WARN: y starts {y_min_year}, but GDP={GDP_LABEL[gdp_var_id]} starts ~{gdp_start} "
        f"— consider {' or '.join(suggestions)}"
    )


def resolve_default_year(cfg: dict, y_var_id: int, gdp_var_id: int, engine) -> int | None:
    mx = cfg.get("maxTime")
    mn = cfg.get("minTime")
    candidate = mx if isinstance(mx, int) else mn if isinstance(mn, int) else None
    if candidate is not None:
        return candidate
    y_yr = year_range(y_var_id, engine)
    x_yr = year_range(gdp_var_id, engine)
    if y_yr is None or x_yr is None:
        return None
    return min(y_yr[1], x_yr[1])


def is_x_independent_line(line: dict) -> bool:
    """True if a comparison line means the same thing on every view.

    `ComparisonLineConfig` is a union: `{xEquals: number}` draws a vertical line at an x
    value, and `{yEquals?: string}` draws a formula that may reference x ("2*x^2",
    "sqrt(x)"). Only a constant `yEquals` is safe to mirror, because the target's other
    views do not share the scatter's x: on the scatter x is GDP per capita, on a LineChart
    it is time, so an x-dependent line becomes a meaningless year or curve there.

    Note `yEquals` **defaults to "x"** when omitted, so a bare `{label: ...}` line is
    x-dependent too — absence is not neutral.
    """
    if "xEquals" in line:
        return False
    y = line.get("yEquals")
    if not isinstance(y, str) or not y.strip():
        return False  # omitted/blank => "x"
    return "x" not in y.lower()


def log_y_axis_sources(chart_ids: Iterable[int]) -> set[int]:
    """Of these SOURCE scatters, those whose log y axis describes the NON-GDP indicator.

    Carrying a source's log y axis onto the target is only meaningful when the two y axes are
    the same indicator. A **reversed** source — GDP on its y — is therefore excluded: its
    `scaleType` describes the GDP axis, and the target's y is the non-GDP indicator, so
    proposing `yScale=log` there would make the wrong axis logarithmic. Same reason
    `process_row` skips the y-oriented mirrors for a reversed source.

    Lives here, and is imported by the handoff builder and the redirect script, because the
    exclusion is easy to get right in one place and forget in another.
    """
    return {i for i, loss in source_lossiness(chart_ids).items() if loss["log"]}


def source_lossiness(chart_ids: Iterable[int]) -> dict[int, dict]:
    """Per SOURCE chart, the two things the migration cannot carry onto the target.

    - `log`: a log y axis describing the non-GDP indicator. `yAxis` is global, so the target
      keeps a linear default and the log survives only on a URL carrying `yScale=log`.
    - `excluded`: `excludedEntityNames`, which the target never inherits (they are global and
      would hide the entity from its line/bar/map views too), so each one reappears on the
      migrated scatter.

    Both come off the one config read, so the redirect script can grade a retirement without
    fetching every source config a second time. `log_y_axis_sources` is the thin wrapper that
    keeps the reversed-source exclusion here rather than in each of its callers.
    """
    ids = tuple(sorted(set(chart_ids)))
    if not ids:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT c.id, cc.full ->> '$.yAxis.scaleType' AS scale_type, cc.full AS full_config "
        "FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN %(i)s",
        params={"i": ids},
    )
    out: dict[int, dict] = {}
    for row in df.to_dict("records"):
        cfg = row["full_config"] if isinstance(row["full_config"], dict) else json.loads(row["full_config"])
        y_dim = find_dim(cfg, "y") or {}
        # A reversed source (GDP on its y) is kept out of the log set: its `scaleType` describes
        # the GDP axis, while the target's y is the non-GDP indicator, so proposing log there
        # would make the wrong axis logarithmic. Its exclusions still apply.
        reversed_source = y_dim.get("variableId") in ANY_GDP_VARIABLE_IDS
        out[int(row["id"])] = {
            "log": row["scale_type"] == "log" and not reversed_source,
            "excluded": list(cfg.get("excludedEntityNames") or []),
            "y_var_id": y_dim.get("variableId"),
        }
    return out


def process_row(
    api: AdminAPI,
    engine,
    src_url: str,
    tgt_url: str,
    gdp_source: str,
) -> dict:
    notes: list[str] = []
    src_id = chart_id_from_url(src_url)
    tgt_id = chart_id_from_url(tgt_url)
    gdp_var_id = resolve_gdp_source(gdp_source)

    cfg = api.get_chart_config(tgt_id)
    src_cfg = api.get_chart_config(src_id)

    # A reversed source plots GDP on y; its y-side settings describe the GDP axis, not the
    # indicator the target charts, so they must not be mirrored.
    src_y_dim = find_dim(src_cfg, "y")
    src_y_is_gdp = (src_y_dim or {}).get("variableId") in ANY_GDP_VARIABLE_IDS
    if src_y_is_gdp:
        notes.append(
            "WARN: source plots GDP on its y axis — skipping the y display.name, yAxis and "
            "comparisonLines mirrors, which would otherwise describe the GDP axis"
        )

    # 1) chartTypes
    existing_types = cfg.get("chartTypes")
    if not existing_types:
        existing_types = SCHEMA_DEFAULT_CHART_TYPES.copy()
        notes.append("seeded default [LineChart, DiscreteBar]")
    else:
        notes.append(f"kept {existing_types}")

    if "ScatterPlot" not in existing_types:
        existing_set = set(existing_types)
        if existing_set & STACKED_FAMILY and not (existing_set & LINE_FAMILY):
            return {
                "chart": tgt_id,
                "src": src_id,
                "gdp_source": gdp_source,
                "status": "SKIPPED",
                "notes": f"chartTypes={existing_types} is stacked-family; ScatterPlot would replace, not add",
                "y_var_id": None,
            }
        existing_types.append("ScatterPlot")
    cfg["chartTypes"] = existing_types

    # 2) dimensions: x / color / size with source overrides for color & size
    dims = list(cfg.get("dimensions", []) or [])
    props = {d.get("property") for d in dims}

    src_color = find_dim(src_cfg, "color")
    src_size = find_dim(src_cfg, "size")
    color_target = (src_color or {}).get("variableId") or CONTINENTS_ID
    src_size_var = (src_size or {}).get("variableId") if src_size else None

    added: list[str] = []
    if "x" not in props:
        dims.append({"variableId": gdp_var_id, "property": "x"})
        added.append(f"x={gdp_var_id}")
    if "color" not in props:
        dims.append({"variableId": color_target, "property": "color"})
        added.append(f"color={color_target}" + (" (from source)" if color_target != CONTINENTS_ID else ""))
    if "size" not in props:
        if src_size is None:
            # Source scatter has no size dim — skip on target too.
            added.append("size=skipped (source has no size dim)")
        else:
            size_target = src_size_var or POPULATION_ID
            # Any population variant (regular, historical, WPP, …) collapses to the
            # default Population indicator — they're interchangeable for bubble sizing.
            # A genuinely non-population size (GDP, area, …) is mirrored as-is.
            if is_population_variant(size_target):
                dims.append({"variableId": POPULATION_ID, "property": "size"})
                if size_target == POPULATION_ID:
                    added.append(f"size={POPULATION_ID}")
                else:
                    added.append(f"size={POPULATION_ID} (normalized population variant {size_target}→default)")
            else:
                dims.append({"variableId": size_target, "property": "size"})
                added.append(
                    f"WARN: size={size_target} is non-population (mirrored from source) — review the bubble sizing"
                )
    cfg["dimensions"] = dims
    if added:
        notes.append("added " + ", ".join(added))

    # 3) matchingEntitiesOnly
    if not cfg.get("matchingEntitiesOnly"):
        cfg["matchingEntitiesOnly"] = True
        notes.append("matchingEntitiesOnly=true")

    # 4) xAxis log
    xa = dict(cfg.get("xAxis") or {})
    if xa.get("scaleType") != "log" or not xa.get("canChangeScaleType"):
        xa["scaleType"] = "log"
        xa["canChangeScaleType"] = True
        cfg["xAxis"] = xa
        notes.append("xAxis: log + canChangeScaleType")

    # 5) yAxis log option (NOT forced). yAxis is shared across all views, so forcing
    # scaleType=log would also flip the line/bar views to log. Instead, when the source
    # scatter is log, we only enable the toggle (canChangeScaleType=True) and leave the
    # default linear — users can switch the scatter to log, line/bar stay linear.
    src_ya = {} if src_y_is_gdp else (src_cfg.get("yAxis") or {})
    if src_ya.get("scaleType") == "log":
        ya = dict(cfg.get("yAxis") or {})
        if not ya.get("canChangeScaleType"):
            ya["canChangeScaleType"] = True
            cfg["yAxis"] = ya
            notes.append("yAxis: enabled log toggle (canChangeScaleType; default stays linear)")

    # 6) yAxis bounds mirror — copy each of min/max the source explicitly sets,
    # preserving other target yAxis keys. Note: affects ALL views, not just scatter.
    # A `max: 0` paired with `min: 0` is a degenerate (collapsed) axis — junk we
    # neither replicate from the source nor leave on the target.
    # Bar/area views need a ZERO baseline, so never mirror a non-zero `min` onto a
    # target that has one (the source's non-zero min is a scatter-only zoom and would
    # make bars start above zero — misleading).
    bar_area_present = bool(set(cfg.get("chartTypes") or []) & BAR_AREA_FAMILY)
    bound_changes = []
    ya = dict(cfg.get("yAxis") or {})
    for bound in ("min", "max"):
        if bound in src_ya and src_ya[bound] != ya.get(bound):
            if bound == "min" and src_ya[bound] != 0 and bar_area_present:
                bound_changes.append(f"min={src_ya[bound]} NOT mirrored (bar/area needs zero baseline)")
                continue
            prev = ya.get(bound, "unset")
            ya[bound] = src_ya[bound]
            bound_changes.append(f"{bound}: {prev}→{src_ya[bound]}")
    if ya.get("min") == 0 and ya.get("max") == 0:
        ya.pop("max", None)
        bound_changes.append("dropped degenerate max:0")
    if bound_changes:
        cfg["yAxis"] = ya
        notes.append("yAxis bounds (" + ", ".join(bound_changes) + ")")

    # 7) y display.name mirror
    src_y = find_dim(src_cfg, "y")
    tgt_y = find_dim(cfg, "y")
    src_name = None if src_y_is_gdp else ((src_y or {}).get("display") or {}).get("name")
    if src_name and tgt_y is not None:
        tgt_display = dict(tgt_y.get("display") or {})
        prev = tgt_display.get("name")
        if prev != src_name:
            tgt_display["name"] = src_name
            tgt_y["display"] = tgt_display
            notes.append(f"y.display.name: {prev!r} → {src_name!r}")

    # 7b) comparisonLines mirror. These are a scatter's reference lines (e.g. `yEquals: 1`
    # for a ratio-to-a-benchmark indicator) and are usually the whole point of the source
    # chart's framing, so losing them silently makes the migrated view say less than the
    # chart it replaces. Only added when the target has none — never overwrite an existing
    # set, same rule as the dimensions. `comparisonLines` is global config, but a reference
    # line that is meaningful for the y indicator is meaningful on the line/bar views too.
    src_lines = None if src_y_is_gdp else src_cfg.get("comparisonLines")
    if src_lines and not cfg.get("comparisonLines"):
        safe = [ln for ln in src_lines if is_x_independent_line(ln)]
        skipped = [ln for ln in src_lines if ln not in safe]
        if safe:
            cfg["comparisonLines"] = safe
            notes.append(f"comparisonLines mirrored from source: {json.dumps(safe)}")
        if skipped:
            notes.append(
                f"WARN: {len(skipped)} x-dependent comparisonLine(s) NOT mirrored "
                f"({json.dumps(skipped)}) — x is GDP on the scatter but time on the "
                f"line/bar views, so they would render meaningless there; re-author by hand "
                f"if the scatter needs them"
            )

    # 8) Warnings (no action)
    if not cfg.get("selectedEntityNames"):
        notes.append(
            "WARN: target has no selectedEntityNames — line/bar/slope views will fall back to Grapher defaults"
        )

    # On scatter, relative mode renders as "Display average annual change". We want
    # the toggle available but OFF by default, i.e. stackMode must not be "relative".
    if cfg.get("stackMode") == "relative":
        notes.append(
            "WARN: stackMode=relative — scatter defaults to 'average annual change'; set to absolute to disable the default"
        )

    # Resolved once, because two checks below need the same view: the year and tolerance the
    # target's scatter actually opens on.
    y_var_id = (tgt_y or {}).get("variableId")
    tgt_tol = int((tgt_y or {}).get("display", {}).get("tolerance") or 0)
    src_tol = int((src_y or {}).get("display", {}).get("tolerance") or 0)
    default_year = None
    if y_var_id is not None:
        try:
            default_year = resolve_default_year(cfg, int(y_var_id), gdp_var_id, engine)
        except Exception as e:
            notes.append(f"(default-year resolution failed: {e!s:.80})")

    # A log y axis is the other thing the migration cannot carry (see step 5): `yAxis` is
    # global, so the target stays linear and the log survives only on a URL that says
    # `yScale=log`. Part 2's redirect and a hand-updated link carry it; a reader who clicks the
    # scatter tab, and a key-chart slot (which has no query string at all — `chart_tags` has
    # nowhere to put one), do not.
    if not src_y_is_gdp and (src_cfg.get("yAxis") or {}).get("scaleType") == "log":
        notes.append(
            "WARN: source y axis is LOG — the target's scatter tab opens LINEAR, and only a URL carrying "
            "yScale=log restores it. Weigh the retirement against how the old chart is referenced"
        )

    excluded = src_cfg.get("excludedEntityNames")
    exclusions: list[dict] = []
    if excluded:
        try:
            if y_var_id is None or default_year is None:
                raise ValueError("no y variable or default year to grade against")
            exclusions = classify_exclusions(list(excluded), int(y_var_id), gdp_var_id, default_year, tgt_tol, engine)
            worst = [r for r in exclusions if r["cls"] in EXCLUSION_WARN_CLASSES]
            summary = ", ".join(f"{r['entity']} {r['cls']}" for r in exclusions)
            lead = "WARN: " if worst else ""
            notes.append(f"{lead}source excludes {len(exclusions)}, none applied — {summary}; see EXCLUDED ENTITIES")
        except Exception as e:
            notes.append(f"WARN: source excludes {excluded} (not applied on target); grading failed: {e!s:.80}")

    # A hidden timeline defeats Grapher's automatic single-year scatter. Normally
    # `checkSingleTimeSelectionPreferred` collapses the two time handles when the scatter
    # is a secondary tab, so the scatter shows one year while the other views keep their
    # full range — no config needed. But with `hideTimeline`,
    # `GrapherState.timelineHandleTimeBounds` reads the AUTHORED minTime/maxTime on every
    # chart tab and ignores the runtime handles, so the collapse never takes effect and
    # the reader has no slider to fix it. Authored `minTime == maxTime` is then the only
    # way to get a single-year scatter — but that is safe only when every other view is
    # single-time anyway; a LineChart/SlopeChart/single-indicator Dumbbell needs a range,
    # and one global time cannot serve both.
    if cfg.get("hideTimeline") and cfg.get("minTime") != cfg.get("maxTime"):
        other_tabs = set(cfg.get("chartTypes") or []) - {"ScatterPlot"}
        if other_tabs and other_tabs <= SINGLE_TIME_ONLY_FAMILY:
            notes.append(
                f"WARN: hideTimeline with minTime != maxTime — scatter will show a time RANGE and the reader "
                f"has no timeline to collapse it. Other views {sorted(other_tabs)} are single-time anyway, "
                f"so setting minTime=maxTime='latest' is safe"
            )
        else:
            notes.append(
                f"WARN: hideTimeline with minTime != maxTime — scatter will show a time RANGE with no timeline "
                f"to collapse it, and {sorted(other_tabs)} needs a range, so no single global time serves both. "
                f"Un-hide the timeline or leave the scatter as a range"
            )

    if y_var_id is not None:
        try:
            yr = year_range(int(y_var_id), engine)
            if yr is not None:
                w = coverage_warning(yr[0], gdp_var_id)
                if w:
                    notes.append(w)
        except Exception as e:
            notes.append(f"(coverage check failed: {e!s:.80})")

    # 9) Tolerance recommendation
    try:
        if src_tol > tgt_tol and y_var_id is not None and default_year is not None:
            y_ents = entities_at_year(int(y_var_id), default_year, tgt_tol, engine)
            x_ents = entities_at_year(gdp_var_id, default_year, tgt_tol, engine)
            visible = y_ents & x_ents
            if len(visible) < 15:
                notes.append(
                    f"WARN: ~{len(visible)} entities would render on scatter at {default_year}; "
                    f"source tolerance={src_tol}, target={tgt_tol} — consider raising target tolerance"
                )
    except Exception as e:
        notes.append(f"(tolerance check failed: {e!s:.80})")

    # 10) Push
    try:
        res = api.update_chart(tgt_id, cfg)
        status = "OK" if res.get("success") else "FAIL"
    except Exception as e:
        status = "ERR_PUT"
        notes.append(f"{e!s:.140}")

    return {
        "chart": tgt_id,
        "src": src_id,
        "gdp_source": gdp_source,
        "status": status,
        "notes": "; ".join(notes) or "(no changes)",
        "y_var_id": y_var_id,
        "exclusions": exclusions,
    }


def print_action_table(results: list[dict]) -> None:
    print()
    print("PER-ROW ACTIONS")
    print(f"{'chart':>6}  {'src':>6}  {'gdp_source':<13}  {'status':<8}  {'edit link':<60}  notes")
    print("-" * 180)
    for r in results:
        link = edit_link(r["chart"]) if isinstance(r["chart"], int) else ""
        print(f"{r['chart']:>6}  {r['src']:>6}  {r['gdp_source']:<13}  {r['status']:<8}  {link:<60}  {r['notes']}")


def print_exclusion_table(results: list[dict]) -> None:
    """The graded exclusions, with the numbers the one-line note has no room for.

    Its own table for the same reason the display names get one: the `notes` column is a single
    joined line, and the evidence for "this entity's return is a defect" is what makes the call
    decidable — a reader who only sees the class has to re-derive it.
    """
    rows = [(r, e) for r in results for e in r.get("exclusions") or []]
    if not rows:
        return
    print()
    print("EXCLUDED ENTITIES (on the source, never applied to the target — so they reappear on the scatter)")
    print(f"{'chart':>6}  {'entity':<26}  {'class':<18}  why")
    print("-" * 190)
    for r, e in rows:
        year = "" if e["exact"] or e["year"] is None else f" [at {e['year']}, its latest year with both]"
        print(f"{r['chart']:>6}  {e['entity'][:26]:<26}  {e['cls']:<18}  {e['why']}{year}")
    print()
    print(
        f"  {' / '.join(sorted(EXCLUSION_WARN_CLASSES))} want a decision; high-GDP and no data are benign "
        "(the target's x axis is log, and matchingEntitiesOnly hides an entity with no pair)."
    )


def print_display_name_table(api: AdminAPI, results: list[dict]) -> None:
    rows = []
    for r in results:
        if r["status"] not in ("OK",):
            continue
        y_var_id = r["y_var_id"]
        if not y_var_id:
            continue
        cfg = api.get_chart_config(r["chart"])
        ydim = find_dim(cfg, "y") or {}
        manual = (ydim.get("display") or {}).get("name") or ""

        var_row = OWID_ENV.read_sql(
            "SELECT name, display FROM variables WHERE id = %(v)s",
            params={"v": int(y_var_id)},
        )
        if var_row.empty:
            continue
        v = var_row.iloc[0]
        etl_disp = json.loads(v["display"]) if v["display"] else {}
        rows.append((r["chart"], y_var_id, manual, etl_disp.get("name", ""), v["name"]))

    if not rows:
        return

    print()
    print("Y-DIM DISPLAY NAMES (manual vs ETL)")
    hdrs = ("chart", "varId", "manual (on chart)", "ETL display.name", "variable.name")
    widths = [max(len(str(r[i])) for r in [hdrs] + rows) for i in range(5)]

    def line(r):
        return "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(r))

    print(line(hdrs))
    print("-" * (sum(widths) + 8))
    for r in rows:
        print(line(r))


def check_gdp_versions() -> None:
    print("GDP-PER-CAPITA VERSION CHECK")
    for hardcoded_id, pattern in GDP_CATALOG_PATTERNS.items():
        latest = OWID_ENV.read_sql(
            "SELECT id, catalogPath FROM variables WHERE catalogPath LIKE %(p)s ORDER BY id DESC LIMIT 1",
            params={"p": pattern},
        )
        label = GDP_LABEL[hardcoded_id]
        if latest.empty:
            print(f"  {label:<11} (id {hardcoded_id}): no match for {pattern} — cannot verify")
            continue
        latest_id = int(latest.iloc[0]["id"])
        latest_path = latest.iloc[0]["catalogPath"]
        if latest_id == hardcoded_id:
            print(f"  {label:<11} (id {hardcoded_id}): up-to-date ({latest_path})")
        else:
            print(
                f"  {label:<11} WARN: hardcoded id={hardcoded_id} but newer id={latest_id} "
                f"exists at {latest_path}. Update GDP_SOURCES in this script if you want to use it."
            )
    print()


def main() -> int:
    payload = json.load(sys.stdin)
    if not isinstance(payload, list):
        print("ERROR: stdin must be a JSON list", file=sys.stderr)
        return 2

    api = AdminAPI(OWID_ENV)
    engine = get_engine()
    check_gdp_versions()
    results: list[dict] = []
    for row in payload:
        try:
            results.append(
                process_row(
                    api,
                    engine,
                    row["chart_admin_url"],
                    row["target_chart_admin_url"],
                    row["gdp_source"],
                )
            )
        except Exception as e:
            results.append(
                {
                    "chart": "-",
                    "src": "-",
                    "gdp_source": row.get("gdp_source", "?"),
                    "status": "ERROR",
                    "notes": f"{type(e).__name__}: {e!s:.180}",
                    "y_var_id": None,
                }
            )

    print(f"Target admin: {short_admin_host()}")
    print_action_table(results)
    print_exclusion_table(results)
    print_display_name_table(api, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
