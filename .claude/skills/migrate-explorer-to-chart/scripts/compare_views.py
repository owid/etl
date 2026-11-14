"""Check that a staging multidim matches the production explorer it replaces, view by view.

For every explorer view and the multidim view it maps to (from `map-explorer-to-mdim`'s
`mapping.json`), it compares:

- indicators: the y indicators of each view, in order (by catalog path), and their display names;
- data and metadata: each indicator's `dataChecksum` and `metadataChecksum`, production vs staging;
- config: the explorer view's grapher config (production `/explorers/<slug>.config.json`) against
  the multidim view's full config (staging DB), field by field, after dropping schema defaults;
- what readers see: header (title, time, subtitle), footer (sources, note), tabs, active tab and
  map legend, read from both pages rendered in a headless browser, on the default tab and on the
  other main tab. Screenshots of each are saved for the review page.

Rendered checks run on every view up to `--max-rendered`; past that, on a sample that covers every
choice of every dimension. The DB and config checks always run on every view. Read-only.

Writes `<out>/eval.json` and `<out>/shots/*.jpg`; `build_review_page.py` turns them into the page.

Usage:
    .venv/bin/python .claude/skills/migrate-explorer-to-chart/scripts/compare_views.py \
        --mapping-dir ai/<slug>-mdim-mapping --staging <branch> --prod-env-file <prod creds> \
        --out ai/<slug>-review
"""

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlencode

from etl.config import OWIDEnv
from etl.http import session

PROD_SITE = "https://ourworldindata.org"
CHROMIUM_GLOB = "~/.cache/ms-playwright/chromium-*/chrome-linux64/chrome"
GRAPHER_SCHEMA = "https://files.ourworldindata.org/schemas/grapher-schema.012.json"
# Config keys that say nothing about what the reader sees, or that are compared separately.
CONFIG_IGNORE = {"$schema", "adminBaseUrl", "bakedGrapherURL", "dimensions", "isPublished", "id", "version", "slug"}
# Read the same parts of the grapher figure on both pages (they share the grapher component).
FACTS_JS = """() => {
  const g = document.querySelector('.GrapherComponent');
  // Read text the way it reads, not the way it wraps. Grapher breaks long text into one element per
  // line and drops the space at each break, and it sets the time span after the title with a margin;
  // so put a space back after every line and block, and before every margin-spaced element. Otherwise the same
  // text wrapped one word apart (the two pages differ slightly in width) reads as a difference.
  const t = e => {
    if (!e) return null;
    const c = e.cloneNode(true);
    c.querySelectorAll('button, [role=button]').forEach(x => x.remove());
    c.querySelectorAll('.markdown-text-wrap__line, p, div').forEach(x => x.append(' '));
    c.querySelectorAll('[style*="inline-block"]').forEach(x => x.prepend(' '));
    return c.textContent.replace(/\\s+/g, ' ').trim();
  };
  return {
    header: t(g.querySelector('.HeaderHTML')),
    // Footer content without its action buttons, whose labels show only when the footer wraps.
    footer: t(g.querySelector('.SourcesFooterHTML')),
    tabs: Array.from(g.querySelectorAll('.Tabs__Tab')).map(x => x.innerText.trim()),
    activeTab: t(g.querySelector('.Tabs__Tab[aria-selected="true"]')),
    legend: Array.from(g.querySelectorAll('.numericColorLegend text, .categoricalColorLegend text'))
      .map(x => x.textContent.trim()).filter(Boolean),
    legendColors: Array.from(g.querySelectorAll('.numericColorLegend rect, .categoricalColorLegend rect'))
      .map(x => (x.getAttribute('fill') || '').toLowerCase().replace(/nodatapattern-\\d+/, 'nodatapattern'))
      .filter(c => c && c !== 'none'),
  };
}"""


def parse_tsv_section(tsv: str, name: str) -> list[dict]:
    """Rows of one tab-indented section (`graphers`, `columns`) of an explorer TSV."""
    lines = tsv.splitlines()
    start = next(i for i, line in enumerate(lines) if line.split("\t")[0] == name)
    block = []
    for line in lines[start + 1 :]:
        if not line.startswith("\t"):
            break
        block.append(line.split("\t")[1:])
    header, rows = block[0], block[1:]
    return [dict(zip(header, r + [""] * (len(header) - len(r)))) for r in rows]


def dim_columns(header: list[str]) -> dict[str, str]:
    """Explorer TSV column -> dimension name (`Measure Dropdown` -> `Measure`)."""
    out = {}
    for col in header:
        m = re.match(r"^(.*) (Dropdown|Radio|Checkbox)$", col)
        if m:
            out[col] = m.group(1)
    return out


def full_catalog_path(path: str) -> str:
    """Catalog paths in a TSV are usually full (`grapher/ns/v/ds/table#col`); keep them as is."""
    return path.strip()


def strip_trailing_nulls(value):
    if isinstance(value, list):
        value = [strip_trailing_nulls(v) for v in value]
        while value and value[-1] is None:
            value.pop()
        return value
    if isinstance(value, dict):
        return {k: strip_trailing_nulls(v) for k, v in value.items()}
    if isinstance(value, str) and re.fullmatch(r"#[0-9a-fA-F]{6}", value):
        return value.lower()
    return value


def load_schema_defaults() -> dict:
    """Schema defaults as a nested dict (`{"map": {"time": "latest", ...}, "tab": "chart", ...}`)."""
    schema = session.get(GRAPHER_SCHEMA, timeout=60).json()

    def resolve(node: dict) -> dict:
        while "$ref" in node:
            node = schema["$defs"][node["$ref"].split("/")[-1]]
        return node

    def walk(node: dict, depth: int = 0) -> dict:
        out = {}
        for k, p in resolve(node).get("properties", {}).items():
            p = resolve(p)
            if "default" in p:
                out[k] = p["default"]
            elif p.get("type") == "object" and depth < 3:
                sub = walk(p, depth + 1)
                if sub:
                    out[k] = sub
        return out

    return walk(schema)


def normalize_config(config: dict, defaults: dict, top: bool = True) -> dict:
    """Drop ignored keys, nulls and values equal to the schema default, at every level."""
    out = {}
    for k, v in config.items():
        if (top and k in CONFIG_IGNORE) or v is None:
            continue
        d = defaults.get(k)
        if isinstance(v, dict) and isinstance(d, dict):
            v = normalize_config(v, d, top=False)
            if not v:
                continue
        elif d is not None and d == v:
            continue
        out[k] = strip_trailing_nulls(v)
    return out


def diff_configs(a: dict, b: dict, prefix: str = "") -> list[dict]:
    """Field-level differences between two normalized configs (nested dicts are walked)."""
    diffs = []
    for k in sorted(set(a) | set(b)):
        va, vb = a.get(k), b.get(k)
        if isinstance(va, dict) and isinstance(vb, dict):
            diffs += diff_configs(va, vb, f"{prefix}{k}.")
        elif va != vb:
            diffs.append({"field": f"{prefix}{k}", "explorer": va, "multidim": vb})
    return diffs


def sample_views(pairs: list[dict], dims: list[str], max_rendered: int) -> list[int]:
    """All views, or a sample that covers every choice of every dimension."""
    if len(pairs) <= max_rendered:
        return [p["id"] for p in pairs]
    chosen, seen = [], set()
    for p in pairs:
        new = {(d, p["explorer_dims"].get(d, "")) for d in dims} - seen
        if new:
            chosen.append(p["id"])
            seen |= new
    for p in pairs:
        if len(chosen) >= max_rendered:
            break
        if p["id"] not in chosen:
            chosen.append(p["id"])
    return sorted(chosen[:max_rendered])


def render(pairs: list[dict], ids: list[int], shots: Path) -> None:
    import glob

    from playwright.sync_api import sync_playwright

    exe = sorted(glob.glob(str(Path(CHROMIUM_GLOB).expanduser())))
    shots.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=exe[-1] if exe else None)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        def capture(url: str, path: Path) -> dict:
            page.goto(url, wait_until="load", timeout=120_000)
            page.wait_for_selector(".GrapherComponent .HeaderHTML h1", timeout=60_000)
            page.wait_for_timeout(2500)
            # The cookie notice would cover the bottom of the figure in every screenshot.
            page.add_style_tag(content=".cookie-notice{display:none!important}")
            facts = page.evaluate(FACTS_JS)
            page.locator(".GrapherComponent").first.screenshot(path=str(path), type="jpeg", quality=72)
            return facts

        for p_ in pairs:
            if p_["id"] not in ids:
                continue
            p_["rendered"] = {}
            # Default tab first; then the other main tab, so map and chart both get checked.
            for tab in [None, "other"]:
                key = "default" if tab is None else "other"
                urls = {"explorer": p_["explorer_url"], "multidim": p_["multidim_url"]}
                if tab == "other":
                    default_tab = (p_["rendered"]["default"]["explorer"]["facts"]["activeTab"] or "").lower()
                    if "map" in default_tab:
                        other = "chart"
                    elif any("map" in t.lower() for t in p_["rendered"]["default"]["explorer"]["facts"]["tabs"]):
                        other = "map"
                    else:
                        break
                    urls = {k: u + ("&" if "?" in u else "?") + f"tab={other}" for k, u in urls.items()}
                res = {}
                for side, url in urls.items():
                    shot = shots / f"v{p_['id']:03d}_{key}_{side}.jpg"
                    try:
                        res[side] = {"url": url, "facts": capture(url, shot), "shot": f"shots/{shot.name}"}
                    except Exception as e:  # a page that fails to render is a finding, not a crash
                        res[side] = {"url": url, "error": str(e).splitlines()[0][:300]}
                p_["rendered"][key] = res
            print(f"  rendered view {p_['id']}")
        browser.close()


def compare_rendered(pair: dict) -> list[dict]:
    out = []
    for key, sides in pair.get("rendered", {}).items():
        e, m = sides.get("explorer", {}), sides.get("multidim", {})
        if "error" in e or "error" in m:
            out.append(
                {"check": f"rendered:{key}", "field": "page", "explorer": e.get("error"), "multidim": m.get("error")}
            )
            continue
        for field in ["header", "footer", "tabs", "activeTab", "legend", "legendColors"]:
            if e["facts"].get(field) != m["facts"].get(field):
                out.append(
                    {
                        "check": f"rendered:{key}",
                        "field": field,
                        "explorer": e["facts"].get(field),
                        "multidim": m["facts"].get(field),
                    }
                )
    return out


def main(mapping_dir: Path, staging: str, prod_env_file: str, out: Path, max_rendered: int) -> None:
    mapping = json.loads((mapping_dir / "mapping.json").read_text())
    sources = json.loads((mapping_dir / "_sources.json").read_text())
    slug = sources["explorer"]["slug"]
    dims = sources["explorer"]["dimensions"]
    mdim = sources["mdims"][0]
    if len(sources["mdims"]) > 1:
        raise SystemExit("compare_views.py handles one target multidim; run it per multidim.")
    prod = OWIDEnv.from_env_file(prod_env_file)
    # After the merge the multidim is on production too: `--staging production` checks it there.
    stg = prod if staging == "production" else OWIDEnv.from_staging(staging)
    stg_site = PROD_SITE if staging == "production" else stg.site.rstrip("/")

    # Explorer side (production): view rows, indicators and column display names from the TSV.
    tsv = prod.read_sql("SELECT tsv FROM explorers WHERE slug = %s", params=(slug,)).iloc[0, 0]
    graphers = parse_tsv_section(tsv, "graphers")
    columns = {c.get("catalogPath") or c.get("slug"): c for c in parse_tsv_section(tsv, "columns")}
    dcols = dim_columns(list(graphers[0].keys()))

    # Multidim side (staging): views, indicators and full view configs.
    cfg_raw = stg.read_sql(
        "SELECT config FROM multi_dim_data_pages WHERE catalogPath = %s", params=(mdim["catalogPath"],)
    ).iloc[0, 0]
    mcfg = json.loads(cfg_raw)
    full_ids = [v["fullConfigId"] for v in mcfg["views"]]
    full = stg.read_sql("SELECT id, config FROM chart_configs WHERE id IN %s", params=(tuple(full_ids),)).set_index(
        "id"
    )["config"]
    mviews = {tuple(sorted(v["dimensions"].items())): v for v in mcfg["views"]}

    defaults = load_schema_defaults()
    pairs = []
    for r in mapping["redirects"]:
        src, tgt = r["source"]["dimensions"], r["target"]
        row = next(g for g in graphers if all(g[c].strip() == src.get(name, "") for c, name in dcols.items()))
        e_paths = [full_catalog_path(x) for x in row.get("yVariableIds", "").split()]
        e_names = [columns.get(x, {}).get("name") or None for x in e_paths]
        mv = mviews.get(tuple(sorted((tgt or {}).get("dimensions", {}).items()))) if tgt else None
        m_full = json.loads(full[mv["fullConfigId"]]) if mv else {}
        m_paths = [y.get("catalogPath") for y in (mv or {}).get("indicators", {}).get("y", [])]
        m_names = [((d.get("display") or {}).get("name")) or None for d in m_full.get("dimensions", [])]
        e_params = {k: v for k, v in src.items() if v}
        e_url = f"{PROD_SITE}/explorers/{slug}" + (f"?{urlencode(e_params)}" if e_params else "")
        m_url = f"{stg_site}/grapher/{mdim['slug']}" + (
            f"?{urlencode(tgt['dimensions'])}" if tgt and tgt.get("dimensions") else ""
        )
        e_cfg = session.get(f"{PROD_SITE}/explorers/{slug}.config.json", params=e_params, timeout=60).json()
        pairs.append(
            {
                "id": r["sourceViewId"],
                "explorer_dims": src,
                "multidim_dims": (tgt or {}).get("dimensions"),
                "multidim_view": (tgt or {}).get("viewId"),
                "explorer_url": e_url,
                "multidim_url": m_url,
                "explorer_indicators": e_paths,
                "multidim_indicators": m_paths,
                "explorer_names": e_names,
                "multidim_names": m_names,
                "config_diffs": diff_configs(normalize_config(e_cfg, defaults), normalize_config(m_full, defaults)),
            }
        )

    # Data and metadata checksums for every indicator involved, production vs staging.
    paths = sorted({p for x in pairs for p in x["explorer_indicators"] + x["multidim_indicators"] if p})
    q = "SELECT catalogPath, dataChecksum, metadataChecksum FROM variables WHERE catalogPath IN %s"
    ck_prod = prod.read_sql(q, params=(tuple(paths),)).set_index("catalogPath").to_dict("index")
    ck_stg = stg.read_sql(q, params=(tuple(paths),)).set_index("catalogPath").to_dict("index")

    # A multidim published moments ago 404s until the site is baked; every capture would then
    # wait out its timeout. Stop early instead.
    code = session.get(pairs[0]["multidim_url"], timeout=60).status_code
    if code != 200:
        raise SystemExit(f"{pairs[0]['multidim_url']} returns {code}: wait for the bake (or publish it), then re-run.")
    # The page also fetches each view's config by id; an upload made while a fresh staging server
    # was still starting leaves them missing, and every chart then times out.
    cfg_url = f"{stg_site}/grapher/by-uuid/{mcfg['views'][0]['fullConfigId']}.config.json"
    if session.get(cfg_url, timeout=60).status_code != 200:
        raise SystemExit(f"{cfg_url} is missing: re-run the upload with `--force --only`, then re-run this.")
    ids = sample_views(pairs, dims, max_rendered)
    print(f"rendering {len(ids)} of {len(pairs)} views (2 tabs each, both pages)")
    render(pairs, ids, out / "shots")

    for p in pairs:
        checks = []
        if p["multidim_view"] is None:
            checks.append({"check": "mapping", "field": "target", "explorer": "view", "multidim": None})
        if p["explorer_indicators"] != p["multidim_indicators"]:
            checks.append(
                {
                    "check": "indicators",
                    "field": "y",
                    "explorer": p["explorer_indicators"],
                    "multidim": p["multidim_indicators"],
                }
            )
        if p["explorer_names"] != p["multidim_names"]:
            checks.append(
                {
                    "check": "indicators",
                    "field": "display.name",
                    "explorer": p["explorer_names"],
                    "multidim": p["multidim_names"],
                }
            )
        for path in p["multidim_indicators"]:
            a, b = ck_prod.get(path), ck_stg.get(path)
            for field in ["dataChecksum", "metadataChecksum"]:
                if not a or not b or a[field] != b[field]:
                    checks.append(
                        {
                            "check": "data" if field == "dataChecksum" else "metadata",
                            "field": f"{path.split('#')[-1]}.{field}",
                            "explorer": (a or {}).get(field),
                            "multidim": (b or {}).get(field),
                        }
                    )
        checks += [{"check": "config", **d} for d in p["config_diffs"]]
        checks += compare_rendered(p)
        p["checks"] = checks

    summary = {}
    for p in pairs:
        for c in p["checks"]:
            summary.setdefault(c["check"].split(":")[0], set()).add(p["id"])
    result = {
        "explorer": {"slug": slug, "dimensions": dims, "url": f"{PROD_SITE}/explorers/{slug}"},
        "multidim": {
            "slug": mdim["slug"],
            "catalogPath": mdim["catalogPath"],
            "dimensions": mcfg["dimensions"],
            "staging_url": f"{stg_site}/grapher/{mdim['slug']}",
            "staging_admin": f"{stg_site}/admin/grapher/{mdim['catalogPath'].replace('/', '%2F').replace('#', '%23')}/",
        },
        "staging": staging,
        "rendered_ids": ids,
        "summary": {k: sorted(v) for k, v in summary.items()},
        "pairs": pairs,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "eval.json").write_text(json.dumps(result, indent=1, default=str))
    n_ok = sum(1 for p in pairs if not p["checks"])
    print(f"{n_ok}/{len(pairs)} views identical on every check -> {out / 'eval.json'}")
    for k, v in result["summary"].items():
        print(f"  {k}: differs in {len(v)} view(s): {v}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mapping-dir", type=Path, required=True)
    ap.add_argument("--staging", required=True, help="Branch whose staging server holds the multidim, or `production`.")
    ap.add_argument("--prod-env-file", required=True, help="Env file with production DB credentials (read-only use).")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--max-rendered", type=int, default=60)
    a = ap.parse_args()
    main(a.mapping_dir, a.staging, a.prod_env_file, a.out, a.max_rendered)
