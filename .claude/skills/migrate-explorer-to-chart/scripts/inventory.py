"""Inventory of published explorers: size, references and redirects, read from the grapher DB.

Writes `inventory.csv` (one row per explorer) and `inventory.json` (same rows plus the list of
referencing posts, the site redirects that end on each explorer, and its featured metrics).

Usage (production, read-only):
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/migrate-explorer-to-chart/scripts/inventory.py --out ai/explorer_migration [--slug fish-stocks]
"""

import argparse
import json
import re
from pathlib import Path

import pandas as pd

from etl.config import OWID_ENV
from etl.paths import BASE_DIR

SLUG_RE = r"/explorers/([^/?#.]+)"
EXPLORER_STEPS_DIR = BASE_DIR / "etl/steps/viz/explorer"


def _slug_col(s: pd.Series) -> pd.Series:
    return s.str.extract(SLUG_RE)[0]


def find_step(slug: str) -> Path | None:
    """Find the viz://explorer step that publishes this slug.

    Tried in order: file name (`fish-stocks` -> `fish_stocks.py`), an explicit `short_name="<slug>"`
    in the script, then `subNavCurrentId: <slug>` in the config.
    """
    stem = slug.replace("-", "_")
    for f in EXPLORER_STEPS_DIR.glob(f"*/latest/{stem}.*"):
        return f.with_name(stem)
    for py in EXPLORER_STEPS_DIR.glob("*/latest/*.py"):
        if re.search(rf"short_name\s*=\s*[\"']{re.escape(slug)}[\"']", py.read_text()):
            return py.with_suffix("")
    for cfg in EXPLORER_STEPS_DIR.glob("*/latest/*.config.yml"):
        if re.search(rf"subNavCurrentId:\s*{re.escape(slug)}\s*$", cfg.read_text(), re.M):
            return cfg.with_name(cfg.name.split(".")[0])
    return None


def code_features(base: Path) -> dict:
    py = base.with_suffix(".py")
    cfgs = list(base.parent.glob(f"{base.name}*.config.yml"))
    text = (py.read_text() if py.exists() else "") + "".join(c.read_text() for c in cfgs)
    return {
        "step": str(base.relative_to(EXPLORER_STEPS_DIR)),
        "py_lines": len(py.read_text().splitlines()) if py.exists() else 0,
        "cfg_lines": sum(len(c.read_text().splitlines()) for c in cfgs),
        "checkbox": bool(re.search("checkbox", text, re.I)),
        "picker": "pickerColumnSlugs" in text,
        "download": "downloadDataLink" in text,
        "related": "wpBlockId" in text,
    }


def main(out: Path, slug: str | None) -> None:
    q = OWID_ENV.read_sql
    ex = q("SELECT slug, isPublished AS published FROM explorers")
    views = q("SELECT explorerSlug AS slug, dimensions FROM explorer_views")
    views["ndims"] = views["dimensions"].map(lambda d: len(json.loads(d)) if d else 0)
    views = views.groupby("slug").agg(views=("ndims", "size"), ndims=("ndims", "max")).reset_index()

    pv = q(
        "SELECT url, views_7d, views_365d FROM analytics_pageviews WHERE url LIKE 'https://ourworldindata.org/explorers/%%'"
    )
    pv["slug"] = _slug_col(pv["url"])
    pv = pv.groupby("slug")[["views_7d", "views_365d"]].sum().reset_index()

    links = q(
        """SELECT l.target AS slug, l.componentType AS component, g.slug AS post,
                  JSON_UNQUOTE(JSON_EXTRACT(g.content, '$.type')) AS type
           FROM posts_gdocs_links l JOIN posts_gdocs g ON g.id = l.sourceId
           WHERE l.linkType = 'explorer' AND g.published = 1"""
    )
    redirects = q("SELECT source, target FROM redirects WHERE target LIKE '%%/explorers/%%'")
    redirects["slug"] = _slug_col(redirects["target"])
    featured = q(
        """SELECT f.url, t.name AS tag, f.incomeGroup FROM featured_metrics f
           JOIN tags t ON t.id = f.parentTagId WHERE f.url LIKE '%%/explorers/%%'"""
    )
    featured["slug"] = _slug_col(featured["url"])
    tags = q(
        "SELECT et.explorerSlug AS slug, GROUP_CONCAT(t.name) AS tags FROM explorer_tags et JOIN tags t ON t.id = et.tagId GROUP BY 1"
    )

    df = ex.merge(views, on="slug", how="left").merge(pv, on="slug", how="left").merge(tags, on="slug", how="left")
    if slug:
        df = df[df["slug"] == slug]
    rows = []
    for r in df.fillna({"views": 0, "ndims": 0, "views_7d": 0, "views_365d": 0, "tags": ""}).to_dict("records"):
        s = r["slug"]
        refs = [
            {
                "post": p,
                "type": t,
                "embed": bool((g["component"] == "chart").any()),
                "components": sorted(set(g["component"].dropna())),
            }
            for (p, t), g in links[links["slug"] == s].groupby(["post", "type"])
        ]
        row = {
            **{k: (int(v) if isinstance(v, float) else v) for k, v in r.items()},
            "posts": len(refs),
            "embeds": sum(x["embed"] for x in refs),
            "redirects_in": int((redirects["slug"] == s).sum()),
            "featured": int((featured["slug"] == s).sum()),
        }
        base = find_step(s)
        if base:
            row.update(code_features(base))
        row["refs"] = refs
        row["redirect_sources"] = redirects.loc[redirects["slug"] == s, "source"].tolist()
        row["featured_list"] = featured.loc[featured["slug"] == s, ["tag", "incomeGroup"]].to_dict("records")
        rows.append(row)

    out.mkdir(parents=True, exist_ok=True)
    (out / "inventory.json").write_text(json.dumps(rows, indent=1, default=str))
    flat = pd.DataFrame(
        [{k: v for k, v in r.items() if k not in ("refs", "redirect_sources", "featured_list")} for r in rows]
    )
    flat.sort_values("views_365d").to_csv(out / "inventory.csv", index=False)
    print(flat.sort_values("views_365d").drop(columns=["tags"], errors="ignore").to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--slug", help="Only this explorer.")
    a = ap.parse_args()
    main(a.out, a.slug)
