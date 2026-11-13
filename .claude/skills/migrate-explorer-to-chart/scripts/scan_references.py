"""Find every place that names an explorer's URL, with its replacement URL and where to edit it.

Wider than `map-explorer-to-mdim/scripts/audit_references.py`, which reads the gdoc link index: this
scans every text and JSON column of the production DB for `/explorers/<slug>` (except history,
analytics and the explorer's own bookkeeping, listed in the output), and greps the ETL repo and
owid-grapher's `origin/master`. Each hit gets its surface, how the URL is used there (embed,
card, link), an edit link, and the URL it should become, resolved with the same rules the
redirects use (`map-explorer-to-mdim/scripts/redirect_rules.py`). Read-only.

Writes `<out>/references_full.json`; `build_review_page.py` renders it.

Usage:
    .venv/bin/python .claude/skills/migrate-explorer-to-chart/scripts/scan_references.py \
        --mapping-dir ai/<slug>-mdim-mapping --prod-env-file <prod creds> --out ai/<slug>-review \
        [--grapher-repo ~/repos/owid-grapher]
"""

import argparse
import html
import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from etl.config import OWIDEnv
from etl.paths import BASE_DIR

sys.path.insert(0, str(BASE_DIR / ".claude/skills/map-explorer-to-mdim/scripts"))
from redirect_rules import build_source_rules, choice_values, resolve_explorer_url  # noqa: E402

SITE = "https://ourworldindata.org"
ADMIN = "https://admin.owid.io/admin"
# History, analytics and the explorer's own bookkeeping: an old revision or a pageview row is not a
# reference anyone has to edit. `redirects` and `featured_metrics` are scanned (they do need edits).
SKIP_TABLES = {
    "chart_revisions",
    "jobs",
    "migrations",
    "users",
    "sessions",
    "explorer_views",
    "explorer_view_dimensions",
    "explorer_variables",
    "multi_dim_redirects",
    "chart_diff_conflicts",
    "chart_diff_approvals",
}
SKIP_PREFIXES = ("analytics_", "archived_")
TEXT_TYPES = ("text", "mediumtext", "longtext", "tinytext", "json", "varchar")


def url_pattern(slug: str) -> re.Pattern:
    # Stop at the slug's end so `/explorers/co2` doesn't match `/explorers/co2-and-more`.
    return re.compile(r"(?:https?://[^\s\"'<>()\[\]]*)?/explorers/" + re.escape(slug) + r"(?![\w-])[^\s\"'<>()\[\]\\]*")


def plain_text(node) -> str:
    """The text a reader sees in a gdoc node (spans, children, values), joined."""
    if isinstance(node, dict):
        if "text" in node and isinstance(node["text"], str):
            return node["text"]
        return "".join(plain_text(node.get(k)) for k in ("children", "value", "text") if k in node)
    if isinstance(node, list):
        return "".join(plain_text(v) for v in node)
    return ""


def gdoc_uses(content: dict, pat: re.Pattern) -> list[dict]:
    """Every URL in a gdoc's content: how it's used, and text to search for in the doc.

    A link in a doc shows only its text, so `link_text` is what to search for: the linked words
    for a text link, the title for a card. `context` is the text just before it (for an embed, the
    end of the paragraph above it), to tell repeated link texts apart.
    """
    found = []
    last_text = ""

    def walk(node, kind):
        nonlocal last_text
        if isinstance(node, dict):
            kind = node.get("type") or node.get("spanType") or kind
            url = node.get("url")
            if isinstance(url, str) and pat.search(url):
                if node.get("spanType") == "span-link":
                    link_text = plain_text(node.get("children"))
                else:
                    link_text = node.get("title") or ""
                for m in pat.finditer(url):
                    found.append(
                        {"url": m.group(0), "how": kind or "text", "link_text": link_text, "context": last_text[-90:]}
                    )
            if node.get("type") == "text" and "value" in node:
                # Walk the spans one by one so a link's context is the text right before it.
                for span in node["value"]:
                    walk(span, kind)
                    last_text = (last_text + plain_text(span))[-300:]
                return
            for k, v in node.items():
                if k != "url":
                    walk(v, kind)
        elif isinstance(node, list):
            for v in node:
                walk(v, kind)
        elif isinstance(node, str):
            for m in pat.finditer(node):
                found.append({"url": m.group(0), "how": kind or "text", "link_text": "", "context": last_text[-90:]})

    walk(content, None)
    return found


def breaks_on_redirect(how: str) -> bool:
    # Embeds and cards render by fetching the explorer page itself; a redirect breaks them at once.
    return how in {"chart", "prominent-link", "all-charts", "key-indicator", "explorer-tiles"}


def main(mapping_dir: Path, prod_env_file: str, out: Path, grapher_repo: Path | None) -> None:
    mapping = json.loads((mapping_dir / "mapping.json").read_text())
    slug = mapping["explorer"]["slug"]
    pat = url_pattern(slug)
    rules = build_source_rules(mapping)
    dim_names, choices = choice_values(mapping_dir)
    prod = OWIDEnv.from_env_file(prod_env_file)

    def repair(q: dict) -> tuple[dict, list[str]]:
        """Fix the usual damage in old explorer links, so they can land on the view they meant.

        A value glued to the next param by a missing `&` ("…affordhideControls=false") is cut back
        to the choice it starts with; a param named after a renamed dimension ("Metric") is moved to
        the one dimension that offers its value.
        """
        out, notes = {}, []
        for k, v in q.items():
            if k in choices and v not in choices[k]:
                head = next((c for c in sorted(choices[k], key=len, reverse=True) if c and v.startswith(c)), None)
                if head:
                    out[k] = head
                    rest = v[len(head) :]
                    if "=" in rest:
                        rk, rv = rest.split("=", 1)
                        out[rk] = rv
                    notes.append(f"cut “{v}” to “{head}”")
                    continue
            if k not in choices and k not in {
                "country",
                "time",
                "tab",
                "facet",
                "hideControls",
                "region",
                "yScale",
                "stackMode",
            }:
                owners = [d for d, cs in choices.items() if v in cs and d not in q]
                if len(owners) == 1:
                    out[owners[0]] = v
                    notes.append(f"renamed “{k}” to “{owners[0]}”")
                    continue
            out.setdefault(k, v)
        return out, notes

    def new_url(old: str) -> dict:
        q = dict(parse_qsl(urlsplit(old if old.startswith("http") else SITE + old).query))
        r = resolve_explorer_url(rules, q, SITE, choices, dim_names)
        if r.match != "view" and q:
            fixed, notes = repair(q)
            r2 = resolve_explorer_url(rules, fixed, SITE, choices, dim_names) if notes else None
            if r2 and r2.match == "view":
                # The redirect still sends the broken link to the default view; the doc edit fixes it.
                return {
                    "new_url": r2.url,
                    "match": "view, after repairing the old link",
                    "repairs": notes,
                    "stale_params": r.stale_params,
                }
        return {"new_url": r.url, "match": r.match, "stale_params": r.stale_params}

    cols = prod.read_sql(
        """SELECT c.TABLE_NAME AS t, c.COLUMN_NAME AS c FROM information_schema.COLUMNS c
           JOIN information_schema.TABLES t ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
           WHERE c.TABLE_SCHEMA = DATABASE() AND t.TABLE_TYPE = 'BASE TABLE' AND c.DATA_TYPE IN %s""",
        params=(TEXT_TYPES,),
    )
    scanned, skipped, hits = [], sorted({t for t in cols.t if t in SKIP_TABLES or t.startswith(SKIP_PREFIXES)}), []
    like = f"%/explorers/{slug}%"
    for t, group in cols.groupby("t"):
        if t in skipped:
            continue
        for c in group.c:
            n = prod.read_sql(
                f"SELECT /*+ MAX_EXECUTION_TIME(120000) */ COUNT(*) FROM `{t}` WHERE `{c}` LIKE %s", params=(like,)
            ).iloc[0, 0]
            scanned.append(f"{t}.{c}")
            if n:
                hits.append((t, c, int(n)))
    print(f"scanned {len(scanned)} columns in {len(set(s.split('.')[0] for s in scanned))} tables; hits: {hits}")

    refs = []

    seen = set()

    def add(surface, where, edit_url, old, how, **extra):
        # One entry per place and URL: a doc keeps a copy in its markdown, a line may repeat a URL.
        key = (surface, re.sub(r":\d+$", "", where), old, how, extra.get("link_text"), extra.get("context"))
        if key in seen:
            return
        seen.add(key)
        refs.append(
            {
                "surface": surface,
                "where": where,
                "edit_url": edit_url,
                "old_url": old,
                "how": how,
                "breaks_on_redirect": breaks_on_redirect(how),
                **new_url(old),
                **extra,
            }
        )

    for t, c, _ in hits:
        if t == "posts_gdocs" and c == "content":
            rows = prod.read_sql(
                "SELECT id, slug, published, JSON_UNQUOTE(content->'$.type') AS type, JSON_UNQUOTE(content->'$.title') AS title,"
                " content FROM posts_gdocs WHERE content LIKE %s",
                params=(like,),
            )
            for r in rows.to_dict("records"):
                for use in gdoc_uses(json.loads(r["content"]), pat):
                    old, how = use["url"], use["how"]
                    add(
                        "Google Doc" if r["published"] else "Google Doc (unpublished draft)",
                        f"{r['slug'] or r['title']} ({r['type']})",
                        f"https://docs.google.com/document/d/{r['id']}/edit",
                        old,
                        how,
                        page_url=f"{SITE}/{r['slug']}" if r["published"] else None,
                        link_text=use["link_text"],
                        context=use["context"],
                        preview_url=f"{ADMIN}/gdocs/{r['id']}/preview",
                    )
        elif t in {"posts_gdocs", "posts_gdocs_components", "posts_gdocs_links", "posts_links"}:
            continue  # same docs as `posts_gdocs.content` / `posts.content` (copies and derived indexes)
        elif t == "posts":
            if c != "content":
                continue  # archieml, markdown and the API snapshot are copies of `content`
            rows = prod.read_sql(
                "SELECT id, slug, type, status, content FROM posts WHERE content LIKE %s", params=(like,)
            )
            # A reusable block renders only on the published explorers whose `wpBlockId` names it.
            shown_on: dict[int, list[str]] = {}
            for e in prod.read_sql(
                "SELECT slug, JSON_UNQUOTE(JSON_EXTRACT(config, '$.wpBlockId')) AS b FROM explorers "
                "WHERE isPublished = 1 AND JSON_EXTRACT(config, '$.wpBlockId') IS NOT NULL"
            ).to_dict("records"):
                shown_on.setdefault(int(e["b"]), []).append(e["slug"])
            for r in rows.to_dict("records"):
                # Legacy WordPress: posts are private copies of pages now served from gdocs.
                if r["type"] == "wp_block" and shown_on.get(r["id"]):
                    pages = ", ".join(f"/explorers/{s}" for s in shown_on[r["id"]])
                    surface, how = f"WordPress block (shown on {pages})", "link in block"
                elif r["type"] == "wp_block":
                    surface, how = "WordPress block (unused, not on the site)", "legacy copy"
                else:
                    surface, how = f"Legacy WordPress {r['type']} ({r['status']}, not on the site)", "legacy copy"
                for m in pat.finditer(r["content"] or ""):
                    add(surface, f"{r['slug']} (wp {r['id']})", None, html.unescape(m.group(0)), how)
        elif t == "chart_configs":
            rows = prod.read_sql(
                f"""SELECT cc.id, cc.slug, ch.id AS chart_id, nc.id AS narrative_id, nc.name AS narrative_name,
                           mx.multiDimId AS mdim_id, cc.`{c}` AS txt
                    FROM chart_configs cc
                    LEFT JOIN charts ch ON ch.configId = cc.id
                    LEFT JOIN narrative_charts nc ON nc.chartConfigId = cc.id
                    LEFT JOIN multi_dim_x_chart_configs mx ON mx.chartConfigId = cc.id
                    WHERE cc.`{c}` LIKE %s""",
                params=(like,),
            )
            for r in rows.to_dict("records"):
                for m in pat.finditer(r["txt"]):
                    if r["chart_id"]:
                        add(
                            "Chart config",
                            f"chart {r['chart_id']} ({r['slug']})",
                            f"{ADMIN}/charts/{r['chart_id']}/edit",
                            m.group(0),
                            "config text",
                        )
                    elif r["narrative_id"]:
                        add(
                            "Narrative chart",
                            r["narrative_name"],
                            f"{ADMIN}/narrative-charts/{r['narrative_id']}/edit",
                            m.group(0),
                            "config text",
                        )
                    elif r["mdim_id"]:
                        add(
                            "Multidim view config",
                            f"multidim {r['mdim_id']}",
                            f"{ADMIN}/multi-dims/{r['mdim_id']}",
                            m.group(0),
                            "config text",
                        )
                    else:
                        add("Chart config (unattached)", r["id"], None, m.group(0), "config text")
        elif t == "variables":
            rows = prod.read_sql(
                f"SELECT id, catalogPath, `{c}` AS txt FROM variables WHERE `{c}` LIKE %s", params=(like,)
            )
            for r in rows.to_dict("records"):
                for m in pat.finditer(r["txt"]):
                    add(
                        "Indicator metadata (fix in ETL)",
                        f"{r['catalogPath']} · {c}",
                        f"{ADMIN}/variables/{r['id']}/",
                        m.group(0),
                        "metadata text",
                    )
        elif t == "featured_metrics":
            rows = prod.read_sql("SELECT id, url FROM featured_metrics WHERE url LIKE %s", params=(like,))
            for r in rows.to_dict("records"):
                add(
                    "Featured metric",
                    f"featured metric {r['id']}",
                    f"{ADMIN}/featured-metrics",
                    r["url"],
                    "featured metric",
                )
        elif t == "redirects":
            rows = prod.read_sql(
                "SELECT id, source, target FROM redirects WHERE source LIKE %s OR target LIKE %s", params=(like, like)
            )
            for r in rows.to_dict("records"):
                if pat.search(r["target"] or ""):
                    add(
                        "Site redirect (repoint its target)",
                        r["source"],
                        f"{ADMIN}/redirects",
                        r["target"],
                        "redirect target",
                    )
        else:
            key = "slug" if t == "explorers" else "id"
            rows = prod.read_sql(f"SELECT `{key}` AS k, `{c}` AS txt FROM `{t}` WHERE `{c}` LIKE %s", params=(like,))
            for r in rows.to_dict("records"):
                if t == "explorers" and r["k"] == slug:
                    continue
                for m in pat.finditer(str(r["txt"])):
                    add(f"DB: {t}.{c}", f"{t} {r['k']}", None, m.group(0), "text")

    # Repos: hardcoded links in ETL metadata or in the site's own code. Both are read at
    # `origin/master`, so a fix merged elsewhere shows as swapped whatever branch is checked out.
    subprocess.run(["git", "-C", str(BASE_DIR), "fetch", "-q", "origin", "master"], check=False)
    repos = {
        "etl": (
            BASE_DIR,
            ["git", "grep", "-n", "-I", f"/explorers/{slug}", "origin/master", "--", ".", ":!ai", ":!data", ":!dag/archive"],
        )
    }
    if grapher_repo:
        subprocess.run(["git", "-C", str(grapher_repo), "fetch", "-q", "origin", "master"], check=False)
        repos["owid-grapher"] = (grapher_repo, ["git", "grep", "-n", "-I", f"/explorers/{slug}", "origin/master", "--"])
    for name, (root, cmd) in repos.items():
        res = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
        for line in res.stdout.splitlines():
            path, lineno, text = line.removeprefix("origin/master:").split(":", 2)
            if name == "etl" and path.startswith("etl/steps/viz/explorer/"):
                continue  # the explorer step itself, retired after the redirects
            # Grapher's ArchieML component docs and our own skills use real explorers as examples;
            # no reader sees them.
            docs = (
                "/gdocTypes/archieMLComponents/" in path
                or path.endswith(".generated.json")
                or path.startswith(".claude/")
            )
            how = "docs example" if docs else "code"
            for m in pat.finditer(text):
                add(f"Repo: {name}", f"{path}:{lineno}", None, m.group(0), how)

    out.mkdir(parents=True, exist_ok=True)
    result = {
        "explorer": slug,
        "scanned_columns": len(scanned),
        "scanned_tables": sorted({s.split(".")[0] for s in scanned}),
        "skipped_tables": skipped,
        "repos": list(repos),
        "references": refs,
    }
    (out / "references_full.json").write_text(json.dumps(result, indent=1, default=str))
    n_break = sum(r["breaks_on_redirect"] for r in refs)
    print(f"{len(refs)} reference(s), {n_break} break on redirect -> {out / 'references_full.json'}")
    for r in refs:
        print(f"  [{r['surface']}] {r['where']} · {r['how']} · {r['match']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mapping-dir", type=Path, required=True)
    ap.add_argument("--prod-env-file", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument(
        "--grapher-repo", type=Path, default=None, help="A local owid-grapher clone (grepped at origin/master)."
    )
    a = ap.parse_args()
    main(a.mapping_dir, a.prod_env_file, a.out, a.grapher_repo)
