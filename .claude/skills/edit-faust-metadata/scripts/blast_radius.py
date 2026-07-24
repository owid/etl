"""Report every surface affected by an indicator/metadata edit, for the edit-faust-metadata skill.

Given variable ids, indicator catalogPaths, a garden meta.yml anchor, or a chart id, sweeps the
branch's STAGING database (a production clone at branch-creation time) for:

- charts using the variables (with `--field`, charts shielded by their own patch override of
  that field are listed separately — an inherited-text change does NOT reach them; for the
  chart-text fields title/subtitle/note, charts with no inheritance path — variable not a y
  series, several y series, or inheritance disabled — are also listed separately, since
  grapher only inherits chart config from a single-y parent);
- MDim views carrying the variables (via multi_dim_x_chart_configs, unioned with a client-side
  scan of the MDim configs' y catalogPaths);
- explorer views rendering the variables (legacy CSV-backed explorers are invisible to these
  tables — a caveat is printed);
- narrative charts whose parent chart or parent MDim view is affected;
- published gdoc article references to the affected slugs (informational — embeds don't break,
  but the displayed text changes).

Usage:
    .venv/bin/python .claude/skills/edit-faust-metadata/scripts/blast_radius.py --branch <b> \
        (--variable-id N ... | --catalog-path 'grapher/<ns>/<ver>/<ds>/<table>#<col>' ... \
         | --anchor NAME --meta-file PATH | --chart-id N) \
        [--field subtitle] [--exclude-chart-id N ...] [--json]

Output: a markdown summary with per-surface tables and staging links, or `--json` for the
machine-readable form (used by the skill to decide whether to ask the user before applying).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from etl.config import OWIDEnv, get_container_name  # noqa: E402
from etl.files import ruamel_load  # noqa: E402

FIELD_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.]*$")

# Chart-text fields that reach a chart only via grapher config inheritance. Grapher's
# inheritance parent exists only for charts with exactly one y dimension and inheritance
# enabled (owid-grapher getParentVariableIdFromChartConfig) — other usages of the variable
# (x/color/size, one of several y series) have no inheritance path for these fields.
TEXT_FIELDS = ("title", "subtitle", "note")


def current_branch() -> str:
    return subprocess.check_output(["git", "branch", "--show-current"], cwd=REPO_ROOT, text=True).strip()


def staging_env(branch: str) -> OWIDEnv:
    if branch in ("master", "main", ""):
        raise SystemExit("Refusing to run against master/main — create the work branch first (`etl pr ...`).")
    env = OWIDEnv.from_staging(branch)
    try:
        env.read_sql("SELECT 1")
    except Exception as e:
        raise SystemExit(
            f"Staging server for branch '{branch}' is not reachable ({type(e).__name__}). "
            "Run `etl pr` first or wait for the staging build to finish."
        ) from e
    return env


# ---------------------------------------------------------------------------
# Input expansion → variable ids
# ---------------------------------------------------------------------------


def ids_from_catalog_paths(env: OWIDEnv, catalog_paths: list[str]) -> tuple[list[int], list[str]]:
    """Resolve catalogPaths to variable ids: exact match, plus '<col>__<dims>' flattened variants."""
    ids: list[int] = []
    catalog: list[str] = []
    for cp in catalog_paths:
        df = env.read_sql(
            r"""
            SELECT id, catalogPath FROM variables
            WHERE catalogPath = %(cp)s OR catalogPath LIKE CONCAT(%(cp)s, '\_\_%%')
            """,
            params={"cp": cp},
        )
        if df.empty:
            print(f"! No variables found for catalogPath '{cp}'")
        for _, row in df.iterrows():
            if int(row["id"]) not in ids:
                ids.append(int(row["id"]))
                catalog.append(row["catalogPath"])
    return ids, catalog


def catalog_paths_from_anchor(anchor: str, meta_file: Path) -> list[str]:
    """Find which variables' metadata consumes a definitions anchor, and derive grapher catalogPaths.

    Detects YAML alias references (`*NAME`, `<<: *NAME`) and dynamic-YAML interpolations
    (`{definitions.NAME}`) inside each variable block. An anchor consumed by
    `definitions.common` (or a `common:` block) means every variable in the file is affected.
    This is a text-level scan — for gnarly files, pass explicit --catalog-path instead.
    """
    text = meta_file.read_text()
    ref_re = re.compile(rf"(\*{re.escape(anchor)}\b|\{{definitions\.{re.escape(anchor)}\}})")
    if not ref_re.search(text):
        raise SystemExit(f"Anchor '{anchor}' is not referenced anywhere in {meta_file}")

    data = ruamel_load(meta_file)
    # <ns>/<ver>/<ds> from etl/steps/data/garden/<ns>/<ver>/<ds>.meta.yml
    parts = meta_file.resolve().parts
    try:
        i = parts.index("garden")
    except ValueError:
        raise SystemExit(f"--meta-file must be a garden .meta.yml (got {meta_file})")
    ns, ver = parts[i + 1], parts[i + 2]
    ds = meta_file.name.removesuffix(".meta.yml")

    # If the anchor is consumed inside definitions.common, every variable inherits it.
    common_block = re.search(r"^definitions:.*?(?=^\S)", text, re.S | re.M)
    all_variables = False
    if common_block:
        m = re.search(r"^  common:.*?(?=^  \S|\Z)", common_block.group(0), re.S | re.M)
        if m and ref_re.search(m.group(0)):
            all_variables = True

    tables = (data.get("tables") or {}) if isinstance(data, dict) else {}
    catalog_paths: list[str] = []
    for table_name, table in tables.items():
        variables = (table or {}).get("variables") or {}
        for var_name in variables:
            if all_variables or _variable_block_references(text, table_name, var_name, ref_re):
                catalog_paths.append(f"grapher/{ns}/{ver}/{ds}/{table_name}#{var_name}")
    if not catalog_paths:
        raise SystemExit(
            f"Anchor '{anchor}' is referenced but no consuming variable blocks were identified in "
            f"{meta_file} — pass explicit --catalog-path arguments instead."
        )
    return catalog_paths


def _variable_block_references(text: str, table: str, var: str, ref_re: re.Pattern) -> bool:
    """Check whether the `<var>:` block under `tables.<table>.variables` references the anchor."""
    var_m = re.search(rf"^(\s+){re.escape(var)}:\s*$(.*?)(?=^\1\S|\Z)", text, re.S | re.M)
    return bool(var_m and ref_re.search(var_m.group(2)))


def _in_clause(ids: list[int], prefix: str) -> tuple[str, dict[str, int]]:
    params = {f"{prefix}{i}": vid for i, vid in enumerate(ids)}
    clause = ", ".join(f"%({k})s" for k in params)
    return clause, params


# ---------------------------------------------------------------------------
# Surface sweeps
# ---------------------------------------------------------------------------


def sweep_charts(env: OWIDEnv, variable_ids: list[int], field: str | None) -> list[dict]:
    clause, params = _in_clause(variable_ids, "v")
    shielded_col = ""
    if field:
        if not FIELD_RE.match(field):
            raise SystemExit(f"Invalid --field '{field}'")
        json_path = "$." + ".".join(f'"{p}"' for p in field.split("."))
        shielded_col = f", MAX(JSON_CONTAINS_PATH(cc.patch, 'one', '{json_path}')) AS shielded"
    df = env.read_sql(
        f"""
        SELECT c.id AS chart_id, cc.slug, c.publishedAt IS NOT NULL AS published,
               c.isInheritanceEnabled AS inheritance_enabled,
               MAX(cd.property = 'y') AS affected_var_in_y,
               (SELECT COUNT(*) FROM chart_dimensions cd2
                WHERE cd2.chartId = c.id AND cd2.property = 'y') AS n_y_dims
               {shielded_col}
        FROM chart_dimensions cd
        JOIN charts c ON c.id = cd.chartId
        JOIN chart_configs cc ON cc.id = c.configId
        WHERE cd.variableId IN ({clause})
        GROUP BY c.id, cc.slug, c.publishedAt, c.isInheritanceEnabled
        ORDER BY published DESC, cc.slug
        """,
        params=params,
    )
    charts = df.to_dict(orient="records")
    if field and field.split(".")[0] in TEXT_FIELDS:
        for c in charts:
            if not c["affected_var_in_y"]:
                c["no_inherit_reason"] = "variable is not a y series (x/color/size only)"
            elif int(c["n_y_dims"]) != 1:
                c["no_inherit_reason"] = "several y series — grapher has no inheritance parent"
            elif not c["inheritance_enabled"]:
                c["no_inherit_reason"] = "inheritance disabled on the chart"
    return charts


def sweep_mdim_views(env: OWIDEnv, variable_ids: list[int], catalog_paths: list[str]) -> list[dict]:
    clause, params = _in_clause(variable_ids, "v")
    df = env.read_sql(
        f"""
        SELECT md.catalogPath AS mdim_catalog_path, md.slug, md.published, mx.viewId, mx.id AS mx_id
        FROM multi_dim_x_chart_configs mx
        JOIN multi_dim_data_pages md ON md.id = mx.multiDimId
        WHERE mx.variableId IN ({clause})
        """,
        params=params,
    )
    views = df.to_dict(orient="records")
    seen = {(v["mdim_catalog_path"], v["viewId"]) for v in views}

    # Client-side scan: mx.variableId only records one variable per view; multi-indicator views
    # carrying an affected variable in other y slots are found by scanning the configs.
    all_mdims = env.read_sql("SELECT catalogPath, slug, published, config FROM multi_dim_data_pages")
    for _, row in all_mdims.iterrows():
        config = json.loads(row["config"]) if isinstance(row["config"], str) else row["config"]
        for view in config.get("views", []):
            y = view.get("indicators", {}).get("y", [])
            if not isinstance(y, list):
                y = [y]
            y_paths = [i.get("catalogPath") if isinstance(i, dict) else i for i in y]
            y_ids = [i.get("id") for i in y if isinstance(i, dict)]
            if any(cp in y_paths for cp in catalog_paths) or any(vid in variable_ids for vid in y_ids):
                key = (row["catalogPath"], json.dumps(view.get("dimensions", {}), sort_keys=True))
                if key not in seen and not any(
                    v["mdim_catalog_path"] == row["catalogPath"] and _dims_match(v["viewId"], view) for v in views
                ):
                    seen.add(key)
                    views.append(
                        {
                            "mdim_catalog_path": row["catalogPath"],
                            "slug": row["slug"],
                            "published": row["published"],
                            "viewId": json.dumps(view.get("dimensions", {})),
                            "mx_id": None,
                        }
                    )
    return views


def _dims_match(view_id: Any, view: dict) -> bool:
    """viewId is a serialized form of the view's dimensions — compare loosely by choice values."""
    dims = view.get("dimensions", {})
    return all(str(v) in str(view_id) for v in dims.values())


def sweep_explorer_views(env: OWIDEnv, variable_ids: list[int]) -> list[dict]:
    conditions = " OR ".join(
        f"JSON_CONTAINS(cc.full->'$.dimensions', JSON_OBJECT('variableId', %(e{i})s))" for i in range(len(variable_ids))
    )
    params = {f"e{i}": vid for i, vid in enumerate(variable_ids)}
    df = env.read_sql(
        f"""
        SELECT ev.explorerSlug, e.isPublished, COUNT(*) AS n_views
        FROM explorer_views ev
        JOIN chart_configs cc ON cc.id = ev.chartConfigId
        JOIN explorers e ON e.slug = ev.explorerSlug
        WHERE {conditions}
        GROUP BY ev.explorerSlug, e.isPublished
        """,
        params=params,
    )
    return df.to_dict(orient="records")


def sweep_narrative_charts(env: OWIDEnv, chart_ids: list[int], mx_ids: list[int], field: str | None) -> list[dict]:
    rows: list[dict] = []
    if chart_ids:
        clause, params = _in_clause(chart_ids, "c")
        df = env.read_sql(
            f"""
            SELECT nc.id, nc.name, nc.parentChartId, cc.patch
            FROM narrative_charts nc JOIN chart_configs cc ON cc.id = nc.chartConfigId
            WHERE nc.parentChartId IN ({clause})
            """,
            params=params,
        )
        rows.extend(df.to_dict(orient="records"))
    if mx_ids:
        clause, params = _in_clause(mx_ids, "m")
        df = env.read_sql(
            f"""
            SELECT nc.id, nc.name, nc.parentMultiDimXChartConfigId AS parent_view, cc.patch
            FROM narrative_charts nc JOIN chart_configs cc ON cc.id = nc.chartConfigId
            WHERE nc.parentMultiDimXChartConfigId IN ({clause})
            """,
            params=params,
        )
        rows.extend(df.to_dict(orient="records"))
    for row in rows:
        patch = json.loads(row["patch"]) if isinstance(row["patch"], str) else (row["patch"] or {})
        row["shielded"] = bool(field and field.split(".")[0] in patch)
        row.pop("patch", None)
    return rows


def sweep_gdoc_refs(env: OWIDEnv, slugs: list[str]) -> list[dict]:
    if not slugs:
        return []
    # Include old slugs that redirect to the affected charts — embeds often use them.
    clause = ", ".join(f"%(s{i})s" for i in range(len(slugs)))
    params = {f"s{i}": s for i, s in enumerate(slugs)}
    redirects = env.read_sql(
        f"""
        SELECT csr.slug FROM chart_slug_redirects csr
        JOIN charts c ON csr.chart_id = c.id
        JOIN chart_configs cc ON c.configId = cc.id
        WHERE cc.slug IN ({clause})
        """,
        params=params,
    )
    all_slugs = sorted(set(slugs) | set(redirects["slug"]))
    clause = ", ".join(f"%(s{i})s" for i in range(len(all_slugs)))
    params = {f"s{i}": s for i, s in enumerate(all_slugs)}
    df = env.read_sql(
        f"""
        SELECT DISTINCT pg.slug AS gdoc_slug, pgl.target, pgl.linkType
        FROM posts_gdocs_links pgl
        JOIN posts_gdocs pg ON pg.id = pgl.sourceId
        WHERE pg.published = 1 AND pgl.linkType IN ('grapher', 'guided-chart') AND pgl.target IN ({clause})
        ORDER BY pg.slug
        """,
        params=params,
    )
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def render_markdown(result: dict[str, Any], branch: str) -> str:
    site = f"http://{get_container_name(branch)}"
    lines: list[str] = []
    charts = result["charts"]
    affected_charts = [c for c in charts if not c.get("shielded") and not c.get("no_inherit_reason")]
    shielded = [c for c in charts if c.get("shielded")]
    no_inherit = [c for c in charts if c.get("no_inherit_reason") and not c.get("shielded")]
    lines.append(
        f"**Blast radius:** {len(affected_charts)} charts, {len(result['mdim_views'])} MDim views, "
        f"{sum(e['n_views'] for e in result['explorers'])} explorer views "
        f"(in {len(result['explorers'])} explorers), {len(result['narrative_charts'])} narrative charts, "
        f"{len(result['gdoc_refs'])} article references."
    )
    lines.append("")
    if affected_charts:
        lines.append("### Charts")
        for c in affected_charts:
            pub = "" if c["published"] else " (unpublished)"
            lines.append(
                f"- [{c['slug']}]({site}/grapher/{c['slug']}){pub} — [edit]({site}/admin/charts/{c['chart_id']}/edit)"
            )
        lines.append("")
    if shielded:
        lines.append("### Charts shielded by their own override (NOT affected by an inherited-text change)")
        for c in shielded:
            lines.append(
                f"- [{c['slug']}]({site}/grapher/{c['slug']}) — [edit]({site}/admin/charts/{c['chart_id']}/edit)"
            )
        lines.append("")
    if no_inherit:
        lines.append("### Charts with no inheritance path for this field (NOT affected)")
        for c in no_inherit:
            lines.append(
                f"- [{c['slug']}]({site}/grapher/{c['slug']}) — {c['no_inherit_reason']} — "
                f"[edit]({site}/admin/charts/{c['chart_id']}/edit)"
            )
        lines.append("")
    if result["mdim_views"]:
        lines.append("### MDim views")
        for v in result["mdim_views"]:
            lines.append(f"- `{v['mdim_catalog_path']}` view `{v['viewId']}` (published: {bool(v['published'])})")
        lines.append("")
    if result["explorers"]:
        lines.append("### Explorers")
        for e in result["explorers"]:
            pub = "published" if e["isPublished"] else "unpublished"
            lines.append(
                f"- [{e['explorerSlug']}]({site}/explorers/{e['explorerSlug']}) — {e['n_views']} views ({pub})"
            )
        lines.append("")
    lines.append("_Caveat: legacy CSV-backed explorers don't appear in explorer_views — not covered here._")
    lines.append("")
    if result["narrative_charts"]:
        lines.append("### Narrative charts")
        for n in result["narrative_charts"]:
            shield = " (shielded by own override)" if n.get("shielded") else ""
            lines.append(f"- {n['name']} (id {n['id']}){shield}")
        lines.append("")
    if result["gdoc_refs"]:
        lines.append("### Article references (informational — displayed text changes, embeds don't break)")
        for g in result["gdoc_refs"]:
            lines.append(f"- https://ourworldindata.org/{g['gdoc_slug']} → `{g['target']}` ({g['linkType']})")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--branch", default=None, help="Work branch (defaults to current git branch)")
    parser.add_argument("--variable-id", type=int, action="append", default=[], help="Variable id (repeatable)")
    parser.add_argument("--catalog-path", action="append", default=[], help="Indicator catalogPath (repeatable)")
    parser.add_argument("--anchor", help="Garden meta.yml definitions anchor name")
    parser.add_argument("--meta-file", type=Path, help="Garden .meta.yml file (required with --anchor)")
    parser.add_argument("--chart-id", type=int, help="Sweep the surfaces of one chart's variables")
    parser.add_argument(
        "--field", default=None, help="Edited field, e.g. 'subtitle' — enables shielded-chart detection"
    )
    parser.add_argument(
        "--exclude-chart-id", type=int, action="append", default=[], help="The chart the user pointed at"
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of markdown")
    args = parser.parse_args()

    branch = args.branch or current_branch()
    env = staging_env(branch)

    catalog_paths = list(args.catalog_path)
    if args.anchor:
        if not args.meta_file:
            raise SystemExit("--anchor requires --meta-file")
        catalog_paths.extend(catalog_paths_from_anchor(args.anchor, args.meta_file))

    variable_ids = list(args.variable_id)
    resolved_paths: list[str] = []
    if catalog_paths:
        ids, resolved_paths = ids_from_catalog_paths(env, catalog_paths)
        variable_ids.extend(i for i in ids if i not in variable_ids)
    if args.chart_id:
        df = env.read_sql(
            "SELECT variableId FROM chart_dimensions WHERE chartId = %(cid)s", params={"cid": args.chart_id}
        )
        variable_ids.extend(int(v) for v in df["variableId"] if int(v) not in variable_ids)
        args.exclude_chart_id.append(args.chart_id)
    if not variable_ids:
        raise SystemExit("Nothing to sweep — pass --variable-id / --catalog-path / --anchor / --chart-id.")
    if resolved_paths and not args.catalog_path:
        print(f"Expanded to {len(variable_ids)} variables from anchor '{args.anchor}'.")

    all_charts = sweep_charts(env, variable_ids, args.field)
    charts = [c for c in all_charts if c["chart_id"] not in args.exclude_chart_id]
    mdim_views = sweep_mdim_views(env, variable_ids, resolved_paths or catalog_paths)
    explorers = sweep_explorer_views(env, variable_ids)
    chart_ids = [c["chart_id"] for c in charts] + args.exclude_chart_id
    mx_ids = [v["mx_id"] for v in mdim_views if v.get("mx_id")]
    narrative = sweep_narrative_charts(env, chart_ids, mx_ids, args.field)
    # The gdoc sweep includes the target chart's slug: articles embedding the chart being
    # edited display the changed text too (same rationale as passing exclude ids to the
    # narrative sweep above). Only the beyond-target *chart list* filters out the target.
    gdocs = sweep_gdoc_refs(env, [c["slug"] for c in all_charts if c["slug"]])

    result = {
        "branch": branch,
        "variable_ids": variable_ids,
        "charts": charts,
        "mdim_views": mdim_views,
        "explorers": explorers,
        "narrative_charts": narrative,
        "gdoc_refs": gdocs,
        "beyond_target_count": len([c for c in charts if not c.get("shielded") and not c.get("no_inherit_reason")])
        + len(mdim_views)
        + sum(e["n_views"] for e in explorers)
        + len(narrative),
    }
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(render_markdown(result, branch))


if __name__ == "__main__":
    main()
