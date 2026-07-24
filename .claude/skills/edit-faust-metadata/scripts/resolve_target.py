"""Resolve a chart/MDim/indicator reference into an editing target for the edit-faust-metadata skill.

Accepts whatever the user pasted — a live ourworldindata.org/grapher URL, a staging URL, an
admin chart-edit URL, an admin collection (MDim) preview URL with dimension query params, a
bare slug, a numeric chart id, or an indicator catalogPath — and reports everything the skill
needs to route the edit: kind, chart id/slug/published state, isInheritanceEnabled, which
top-level keys sit in the chart's patch (explicit chart-level overrides), the chart's variables
(with whether each has an ETL grapher config), the matched MDim view and its overrides, the
candidate ETL files to edit, and ready-made staging/admin URLs.

Usage:
    .venv/bin/python .claude/skills/edit-faust-metadata/scripts/resolve_target.py <reference> \
        [--branch <branch>] [--json] [--no-db]

The DB lookups run against the branch's STAGING server (never production). `--no-db` does
parse-only identification — useful before the staging server exists.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlparse

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from etl.config import OWIDEnv, get_container_name  # noqa: E402


def current_branch() -> str:
    return subprocess.check_output(["git", "branch", "--show-current"], cwd=REPO_ROOT, text=True).strip()


# ---------------------------------------------------------------------------
# Parse layer (no DB)
# ---------------------------------------------------------------------------


def parse_reference(ref: str) -> dict[str, Any]:
    """Normalize a pasted reference into {ref_type, value, query} without touching the DB."""
    ref = ref.strip()

    # Numeric chart id.
    if re.fullmatch(r"\d+", ref):
        return {"ref_type": "chart_id", "value": int(ref), "query": {}}

    # Indicator or MDim catalogPath (contains '#', not a URL).
    if "#" in ref and "://" not in ref and not ref.startswith("http"):
        path_part = ref.split("#", 1)[0]
        n_parts = len(path_part.split("/"))
        if n_parts >= 5:
            return {"ref_type": "indicator_path", "value": ref, "query": {}}
        # e.g. 'wb/latest/incomes_pip#incomes_pip' or 'grapher/wb/latest/incomes_pip#incomes_pip'
        return {"ref_type": "mdim_path", "value": ref, "query": {}}

    if "://" not in ref and not ref.startswith("http") and "/" not in ref:
        return {"ref_type": "slug", "value": ref, "query": {}}

    url = urlparse(ref if "://" in ref else f"http://{ref}")
    query = dict(parse_qsl(url.query))
    path = url.path.rstrip("/")

    m = re.search(r"/admin/charts/(\d+)", path)
    if m:
        return {"ref_type": "chart_id", "value": int(m.group(1)), "query": query}

    m = re.search(r"/admin/grapher/(.+)$", path)
    if m:
        decoded = unquote(m.group(1))
        if "/" in decoded:
            return {"ref_type": "mdim_path", "value": decoded, "query": query}
        return {"ref_type": "slug", "value": decoded, "query": query}

    m = re.search(r"/explorers/([^/]+)$", path)
    if m:
        return {"ref_type": "explorer_slug", "value": m.group(1), "query": query}

    m = re.search(r"/grapher/([^/]+)$", path)
    if m:
        slug = m.group(1).removesuffix(".svg").removesuffix(".png")
        return {"ref_type": "slug", "value": slug, "query": query}

    raise SystemExit(f"Could not parse reference: {ref}")


# ---------------------------------------------------------------------------
# DB layer (branch staging server only)
# ---------------------------------------------------------------------------


def staging_env(branch: str) -> OWIDEnv:
    if branch in ("master", "main", ""):
        raise SystemExit(
            "Refusing to resolve against master/main. Create the work branch first "
            "(`etl pr ...`) or pass --branch, or use --no-db for parse-only output."
        )
    env = OWIDEnv.from_staging(branch)
    try:
        env.read_sql("SELECT 1")
    except Exception as e:
        raise SystemExit(
            f"Staging server for branch '{branch}' is not reachable ({type(e).__name__}). "
            "Run `etl pr` first or wait for the staging build to finish."
        ) from e
    return env


def load_chart(env: OWIDEnv, *, chart_id: int | None = None, slug: str | None = None) -> dict | None:
    where = "c.id = %(chart_id)s" if chart_id is not None else "cc.slug = %(slug)s"
    df = env.read_sql(
        f"""
        SELECT c.id, cc.slug, c.publishedAt, c.isInheritanceEnabled, cc.patch
        FROM charts c JOIN chart_configs cc ON c.configId = cc.id
        WHERE {where}
        ORDER BY (c.publishedAt IS NULL), c.id
        """,
        params={"chart_id": chart_id, "slug": slug},
    )
    if df.empty:
        return None
    if len(df) > 1:
        print(f"! {len(df)} charts share slug '{slug}' — using the published one (id={df.iloc[0]['id']})")
    row = df.iloc[0]
    patch = json.loads(row["patch"]) if isinstance(row["patch"], str) else (row["patch"] or {})
    variables = env.read_sql(
        """
        SELECT cd.property, cd.`order`, v.id AS variable_id, v.catalogPath,
               v.grapherConfigIdETL IS NOT NULL AS has_etl_config,
               v.grapherConfigIdAdmin IS NOT NULL AS has_admin_config
        FROM chart_dimensions cd JOIN variables v ON cd.variableId = v.id
        WHERE cd.chartId = %(chart_id)s
        ORDER BY cd.property, cd.`order`
        """,
        params={"chart_id": int(row["id"])},
    )
    return {
        "chart_id": int(row["id"]),
        "slug": row["slug"],
        "published": row["publishedAt"] is not None,
        "is_inheritance_enabled": bool(row["isInheritanceEnabled"]),
        "patch_keys": sorted(patch.keys()),
        "variables": variables.to_dict(orient="records"),
    }


def load_mdim(env: OWIDEnv, *, catalog_path: str | None = None, slug: str | None = None) -> dict | None:
    where = "catalogPath = %(cp)s" if catalog_path is not None else "slug = %(slug)s"
    df = env.read_sql(
        f"SELECT id, catalogPath, slug, published, config FROM multi_dim_data_pages WHERE {where}",
        params={"cp": catalog_path, "slug": slug},
    )
    if df.empty and catalog_path is not None:
        # DB catalogPaths may or may not carry a 'grapher/' channel prefix — try the other form.
        alt = (
            catalog_path.removeprefix("grapher/") if catalog_path.startswith("grapher/") else f"grapher/{catalog_path}"
        )
        df = env.read_sql(
            "SELECT id, catalogPath, slug, published, config FROM multi_dim_data_pages WHERE catalogPath = %(cp)s",
            params={"cp": alt},
        )
    if df.empty:
        return None
    row = df.iloc[0]
    config = json.loads(row["config"]) if isinstance(row["config"], str) else row["config"]
    return {
        "mdim_id": int(row["id"]),
        "catalog_path": row["catalogPath"],
        "slug": row["slug"],
        "published": bool(row["published"]),
        "config": config,
    }


def match_mdim_view(config: dict, query: dict[str, str]) -> dict[str, Any]:
    """Match URL query params against the MDim's views.

    Choice values are compared stripped (trailing-space choices are deliberate and must be
    preserved when writing back — see memory reference_mdim_choice_name_trailing_space).
    """
    dim_slugs = [d["slug"] for d in config.get("dimensions", [])]
    given = {k: v for k, v in query.items() if k in dim_slugs}
    views = config.get("views", [])
    if not given:
        return {"kind": "mdim", "given_dims": {}, "matched_views": len(views)}

    def norm(v: Any) -> str:
        return str(v).strip()

    matches = [
        v for v in views if all(norm(v.get("dimensions", {}).get(k, "")) == norm(val) for k, val in given.items())
    ]
    result: dict[str, Any] = {"given_dims": given, "matched_views": len(matches)}
    if len(matches) == 1:
        view = matches[0]
        y = view.get("indicators", {}).get("y", [])
        y_paths = [i["catalogPath"] if isinstance(i, dict) else i for i in y]
        result.update(
            {
                "kind": "mdim-view",
                "view_dimensions": view.get("dimensions", {}),
                "config_overrides": sorted((view.get("config") or {}).keys()),
                "metadata_overrides": sorted((view.get("metadata") or {}).keys()),
                "y_indicators": y_paths,
            }
        )
    else:
        result["kind"] = "mdim"
    return result


def load_indicator(env: OWIDEnv, catalog_path: str) -> dict | None:
    df = env.read_sql(
        "SELECT id, catalogPath, grapherConfigIdETL IS NOT NULL AS has_etl_config FROM variables "
        "WHERE catalogPath = %(cp)s",
        params={"cp": catalog_path},
    )
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "variable_id": int(row["id"]),
        "catalog_path": row["catalogPath"],
        "has_etl_config": bool(row["has_etl_config"]),
    }


# ---------------------------------------------------------------------------
# ETL file mapping and URLs
# ---------------------------------------------------------------------------


def candidate_files_for_indicator(catalog_path: str) -> list[str]:
    """From 'grapher/<ns>/<ver>/<ds>/<table>#<col>' derive the edit files that exist on disk.

    The grapher catalogPath version can differ from the garden version (and an old catalogPath
    can point at a version no longer on disk), so when the exact version is absent the latest
    on-disk version of the dataset is returned instead — the caller must confirm it's the step
    that actually feeds the indicator before editing.

    When a `<ds>.meta.override.yml` sits next to the meta.yml, it is listed FIRST: the ETL
    merges it on top of the built metadata (etl/steps/__init__.py), and datasets that carry one
    (e.g. WDI) auto-generate their main meta.yml — manual curation must go into the override
    file or it is lost on the next regeneration.
    """
    path_part = catalog_path.split("#", 1)[0]
    parts = path_part.split("/")
    if len(parts) < 4:
        return []
    if parts[0] in ("grapher", "garden"):
        parts = parts[1:]
    ns, ver, ds = parts[0], parts[1], parts[2]
    files: list[str] = []
    for channel in ("garden", "grapher"):
        exact = REPO_ROOT / "etl/steps/data" / channel / ns / ver / f"{ds}.meta.yml"
        if exact.exists():
            override = exact.with_name(f"{ds}.meta.override.yml")
            if override.exists():
                files.append(
                    f"{override.relative_to(REPO_ROOT)} (manual-curation override — edit THIS file; the main meta.yml is likely auto-generated)"
                )
            files.append(str(exact.relative_to(REPO_ROOT)))
            continue
        versions = sorted((REPO_ROOT / "etl/steps/data" / channel / ns).glob(f"*/{ds}.meta.yml"))
        if versions:
            override = versions[-1].with_name(f"{ds}.meta.override.yml")
            if override.exists():
                files.append(
                    f"{override.relative_to(REPO_ROOT)} (manual-curation override — edit THIS file; the main meta.yml is likely auto-generated)"
                )
            files.append(f"{versions[-1].relative_to(REPO_ROOT)} (latest on disk — catalogPath says {ver})")
    return files


def candidate_files_for_mdim(catalog_path: str) -> list[str]:
    """From '<ns>/<ver>/<short>#<short>' (with or without channel prefix) find the MDim step files."""
    path_part = catalog_path.split("#", 1)[0]
    parts = path_part.split("/")
    if parts and parts[0] in ("grapher", "multidim", "export"):
        parts = parts[1:]
    if len(parts) < 3:
        return []
    ns, ver, short = parts[0], parts[1], parts[2]
    step_dir = REPO_ROOT / "etl/steps/export/multidim" / ns / ver
    if not step_dir.exists():
        return []
    return sorted(str(p.relative_to(REPO_ROOT)) for p in step_dir.glob(f"{short}*") if p.suffix in (".py", ".yml"))


def build_urls(branch: str, result: dict[str, Any]) -> dict[str, str]:
    container = get_container_name(branch)
    site = f"http://{container}"
    urls: dict[str, str] = {}
    chart = result.get("chart")
    if chart and chart.get("slug"):
        urls["staging_site"] = f"{site}/grapher/{chart['slug']}"
        urls["staging_svg"] = f"{site}/grapher/{chart['slug']}.svg"
    if chart:
        urls["staging_admin_edit"] = f"{site}/admin/charts/{chart['chart_id']}/edit"
    mdim = result.get("mdim")
    if mdim:
        base = f"{site}/admin/grapher/{quote(mdim['catalog_path'], safe='')}"
        given = (result.get("view") or {}).get("given_dims") or {}
        if given:
            base += "?" + "&".join(f"{k}={quote(str(v))}" for k, v in given.items())
        urls["staging_admin_preview"] = base
        if mdim.get("slug") and mdim.get("published"):
            urls["staging_site"] = f"{site}/grapher/{mdim['slug']}"
    indicator = result.get("indicator")
    if indicator:
        urls["staging_admin_variable"] = f"{site}/admin/variables/{indicator['variable_id']}/"
        urls["staging_metadata_json"] = (
            f"https://api-staging.owid.io/{container}/v1/indicators/{indicator['variable_id']}.metadata.json"
        )
    return urls


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def resolve(ref: str, branch: str, use_db: bool) -> dict[str, Any]:
    parsed = parse_reference(ref)
    result: dict[str, Any] = {"reference": ref, "branch": branch, **parsed}

    if parsed["ref_type"] == "explorer_slug":
        result["kind"] = "explorer"
        result["note"] = "Explorers are out of scope for edit-faust-metadata — use /create-explorer."
        return result

    if not use_db:
        result["kind"] = {
            "chart_id": "chart",
            "slug": "chart-or-mdim (needs DB)",
            "mdim_path": "mdim",
            "indicator_path": "indicator",
        }[parsed["ref_type"]]
        if parsed["ref_type"] == "indicator_path":
            result["files"] = candidate_files_for_indicator(parsed["value"])
        if parsed["ref_type"] == "mdim_path":
            result["files"] = candidate_files_for_mdim(parsed["value"])
        return result

    env = staging_env(branch)

    if parsed["ref_type"] == "chart_id":
        chart = load_chart(env, chart_id=parsed["value"])
        if not chart:
            raise SystemExit(f"No chart with id {parsed['value']} on staging-site-{branch}.")
        result["kind"] = "chart"
        result["chart"] = chart

    elif parsed["ref_type"] == "slug":
        chart = load_chart(env, slug=parsed["value"])
        if chart:
            result["kind"] = "chart"
            result["chart"] = chart
        else:
            mdim = load_mdim(env, slug=parsed["value"])
            if mdim:
                result["mdim"] = {k: v for k, v in mdim.items() if k != "config"}
                result["view"] = match_mdim_view(mdim["config"], parsed["query"])
                result["kind"] = result["view"]["kind"]
                result["files"] = candidate_files_for_mdim(mdim["catalog_path"])
            else:
                redirect = env.read_sql(
                    "SELECT chart_id FROM chart_slug_redirects WHERE slug = %(slug)s",
                    params={"slug": parsed["value"]},
                )
                if redirect.empty:
                    raise SystemExit(f"Slug '{parsed['value']}' not found in charts, MDims, or redirects.")
                chart = load_chart(env, chart_id=int(redirect.iloc[0]["chart_id"]))
                result["kind"] = "chart"
                result["chart"] = chart
                result["note"] = f"Slug '{parsed['value']}' is a redirect — canonical slug is '{chart['slug']}'."

    elif parsed["ref_type"] == "mdim_path":
        mdim = load_mdim(env, catalog_path=parsed["value"])
        if not mdim:
            raise SystemExit(f"No MDim with catalogPath '{parsed['value']}' on staging-site-{branch}.")
        result["mdim"] = {k: v for k, v in mdim.items() if k != "config"}
        result["view"] = match_mdim_view(mdim["config"], parsed["query"])
        result["kind"] = result["view"]["kind"]
        result["files"] = candidate_files_for_mdim(mdim["catalog_path"])

    elif parsed["ref_type"] == "indicator_path":
        indicator = load_indicator(env, parsed["value"])
        if not indicator:
            raise SystemExit(f"No variable with catalogPath '{parsed['value']}' on staging-site-{branch}.")
        result["kind"] = "indicator"
        result["indicator"] = indicator
        result["files"] = candidate_files_for_indicator(parsed["value"])

    if result.get("kind") == "chart" and result.get("chart"):
        y_paths = [v["catalogPath"] for v in result["chart"]["variables"] if v["property"] == "y" and v["catalogPath"]]
        files: list[str] = []
        for p in y_paths:
            files.extend(f for f in candidate_files_for_indicator(p) if f not in files)
        result["files"] = files
        if not result["chart"]["published"]:
            result.setdefault("note", "")
            result["note"] = (result["note"] + " Chart is UNPUBLISHED on staging.").strip()

    result["urls"] = build_urls(branch, result)
    return result


def print_human(result: dict[str, Any]) -> None:
    print(f"Kind: {result.get('kind')}")
    if result.get("note"):
        print(f"Note: {result['note']}")
    chart = result.get("chart")
    if chart:
        print(f"Chart id: {chart['chart_id']}  slug: {chart['slug']}  published: {chart['published']}")
        print(f"isInheritanceEnabled: {chart['is_inheritance_enabled']}")
        print(f"Patch keys (explicit chart-level overrides): {chart['patch_keys']}")
        print("Variables:")
        for v in chart["variables"]:
            print(
                f"  [{v['property']}] id={v['variable_id']} etl_config={bool(v['has_etl_config'])} "
                f"admin_config={bool(v['has_admin_config'])} {v['catalogPath']}"
            )
    mdim = result.get("mdim")
    if mdim:
        print(f"MDim: {mdim['catalog_path']}  slug: {mdim.get('slug')}  published: {mdim.get('published')}")
    view = result.get("view")
    if view:
        print(f"View match: {view.get('matched_views')} view(s) for dims {view.get('given_dims')}")
        if view.get("kind") == "mdim-view":
            print(f"  view dims: {view['view_dimensions']}")
            print(f"  config overrides: {view['config_overrides']}")
            print(f"  metadata overrides: {view['metadata_overrides']}")
            print(f"  y indicators: {view['y_indicators']}")
    indicator = result.get("indicator")
    if indicator:
        print(f"Variable id: {indicator['variable_id']}  etl_config={indicator['has_etl_config']}")
        print(f"  {indicator['catalog_path']}")
    if result.get("files"):
        print("Candidate ETL files:")
        for f in result["files"]:
            print(f"  {f}")
    if result.get("urls"):
        print("URLs:")
        for k, v in result["urls"].items():
            print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("reference", help="URL, slug, chart id, or catalogPath to resolve")
    parser.add_argument("--branch", default=None, help="Work branch (defaults to current git branch)")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of the human-readable block")
    parser.add_argument("--no-db", action="store_true", help="Parse-only (no staging DB lookups)")
    args = parser.parse_args()

    branch = args.branch or current_branch()
    result = resolve(args.reference, branch, use_db=not args.no_db)
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print_human(result)


if __name__ == "__main__":
    main()
