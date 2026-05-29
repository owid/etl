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
- Mirrors the source's `yAxis.scaleType: log` if present.
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
import re
import sys
from typing import Any

from etl.config import OWID_ENV
from etl.db import get_engine
from etl.grapher.io import variable_data_df_from_s3
from apps.chart_sync.admin_api import AdminAPI

GDP_SOURCES = {
    "world bank": 1204826,
    "wdi": 1204826,
    "maddison": 900793,
    "maddison project database": 900793,
    "pwt": 1108541,
    "penn world table": 1108541,
}
GDP_LABEL = {1204826: "World Bank", 900793: "Maddison", 1108541: "PWT"}
GDP_COVERAGE = {1204826: 1990, 1108541: 1950, 900793: 1}

# catalogPath patterns used to detect when a newer version of each GDP-per-capita
# indicator has landed in the catalog since this script was last updated.
GDP_CATALOG_PATTERNS = {
    1204826: "grapher/worldbank_wdi/%/wdi/wdi#ny_gdp_pcap_pp_kd",
    900793: "grapher/ggdc/%/maddison_project_database/maddison_project_database#gdp_per_capita",
    1108541: "grapher/ggdc/%/penn_world_table/penn_world_table#rgdpo_pc",
}

CONTINENTS_ID = 900801
POPULATION_ID = 953899  # "Population" — the default sizing indicator (-10000..2100)

LINE_FAMILY = {"LineChart", "SlopeChart", "DiscreteBar", "Marimekko", "ScatterPlot"}
STACKED_FAMILY = {"StackedArea", "StackedBar", "StackedDiscreteBar"}
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


def entities_at_year(var_id: int, year: int, tolerance: int, engine) -> set[str]:
    df = load_var_data(var_id, engine)
    if df is None or df.empty:
        return set()
    in_window = df[(df["year"] >= year - tolerance) & (df["year"] <= year + tolerance)]
    return set(in_window["entityName"].unique())


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
        added.append(
            f"color={color_target}"
            + (" (from source)" if color_target != CONTINENTS_ID else "")
        )
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

    # 5) yAxis log mirror
    src_ya = src_cfg.get("yAxis") or {}
    if src_ya.get("scaleType") == "log":
        ya = dict(cfg.get("yAxis") or {})
        if ya.get("scaleType") != "log":
            ya["scaleType"] = "log"
            ya["canChangeScaleType"] = True
            cfg["yAxis"] = ya
            notes.append("yAxis: log + canChangeScaleType (mirrored from source)")

    # 6) yAxis bounds mirror — copy each of min/max the source explicitly sets,
    # preserving other target yAxis keys. Note: affects ALL views, not just scatter.
    # A `max: 0` paired with `min: 0` is a degenerate (collapsed) axis — junk we
    # neither replicate from the source nor leave on the target.
    bound_changes = []
    ya = dict(cfg.get("yAxis") or {})
    for bound in ("min", "max"):
        if bound in src_ya and src_ya[bound] != ya.get(bound):
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
    src_name = ((src_y or {}).get("display") or {}).get("name")
    if src_name and tgt_y is not None:
        tgt_display = dict(tgt_y.get("display") or {})
        prev = tgt_display.get("name")
        if prev != src_name:
            tgt_display["name"] = src_name
            tgt_y["display"] = tgt_display
            notes.append(f"y.display.name: {prev!r} → {src_name!r}")

    # 8) Warnings (no action)
    if not cfg.get("selectedEntityNames"):
        notes.append("WARN: target has no selectedEntityNames — line/bar/slope views will fall back to Grapher defaults")

    # On scatter, relative mode renders as "Display average annual change". We want
    # the toggle available but OFF by default, i.e. stackMode must not be "relative".
    if cfg.get("stackMode") == "relative":
        notes.append("WARN: stackMode=relative — scatter defaults to 'average annual change'; set to absolute to disable the default")

    excluded = src_cfg.get("excludedEntityNames")
    if excluded:
        notes.append(f"WARN: source excludes {excluded} (not applied on target)")

    y_var_id = (tgt_y or {}).get("variableId")
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
        tgt_tol = int((tgt_y or {}).get("display", {}).get("tolerance") or 0)
        src_tol = int((src_y or {}).get("display", {}).get("tolerance") or 0)
        if src_tol > tgt_tol and y_var_id is not None:
            year = resolve_default_year(cfg, int(y_var_id), gdp_var_id, engine)
            if year is not None:
                y_ents = entities_at_year(int(y_var_id), year, tgt_tol, engine)
                x_ents = entities_at_year(gdp_var_id, year, tgt_tol, engine)
                visible = y_ents & x_ents
                if len(visible) < 15:
                    notes.append(
                        f"WARN: ~{len(visible)} entities would render on scatter at {year}; "
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
    }


def print_action_table(results: list[dict]) -> None:
    print()
    print("PER-ROW ACTIONS")
    print(f"{'chart':>6}  {'src':>6}  {'gdp_source':<13}  {'status':<8}  {'edit link':<60}  notes")
    print("-" * 180)
    for r in results:
        link = edit_link(r["chart"]) if isinstance(r["chart"], int) else ""
        print(
            f"{r['chart']:>6}  {r['src']:>6}  {r['gdp_source']:<13}  {r['status']:<8}  "
            f"{link:<60}  {r['notes']}"
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
    def line(r): return "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(r))
    print(line(hdrs))
    print("-" * (sum(widths) + 8))
    for r in rows:
        print(line(r))


def check_gdp_versions() -> None:
    print("GDP-PER-CAPITA VERSION CHECK")
    for hardcoded_id, pattern in GDP_CATALOG_PATTERNS.items():
        latest = OWID_ENV.read_sql(
            "SELECT id, catalogPath FROM variables "
            "WHERE catalogPath LIKE %(p)s ORDER BY id DESC LIMIT 1",
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
    print_display_name_table(api, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
