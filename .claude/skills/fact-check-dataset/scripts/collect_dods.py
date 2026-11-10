"""List every DoD (glossary entry) a dataset's readers see, with where it appears and its current text.

DoDs are reader-facing text (shown on hover) that live in the grapher DB, not in the repo, so a PR diff never
shows them and a metadata review that reads only the `.meta.yml` misses them. This script collects every
`#dod:<name>` reference from the surfaces that render the dataset, then fetches each DoD's text from production.

Surfaces scanned:
  - indicator metadata: the local grapher build (new text, if built) and production variables (any version)
  - chart configs of production charts that use the dataset's indicators (any version), and of the narrative charts
    built on those charts or on views of published MDims that use those indicators
  - ETL viz configs (charts, MDims, explorers) of the viz steps that depend on the dataset (any version) in the DAG
  - articles and data insights that embed or link those charts (by current or redirected slug, as a chart, a guided
    chart or a pasted URL) or their narrative charts (their own DoD links), including data insights that show one of
    those charts or narrative charts in their `grapher-url` or `narrative-chart` front matter

References and DoD text are read from the public Datasette mirror of production (no DB credentials needed; the
mirror can lag production by a few days). A DoD edited on the branch's staging server reaches production only on
merge, so pass `--staging <branch>` to read the DoD text from that staging server's MySQL instead (needs the
staging network); entries whose staging text differs from the production mirror are flagged.

Usage:
    .venv/bin/python .claude/skills/fact-check-dataset/scripts/collect_dods.py <namespace>/<version>/<short_name>
    .venv/bin/python .claude/skills/fact-check-dataset/scripts/collect_dods.py <ns>/<version>/<short_name> --staging <branch>
"""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from etl.http import session

DATASETTE = "https://datasette-public.owid.io/owid.json"
REPO = Path(__file__).resolve().parents[4]
# A DoD link is `[text](#dod:<name>)`; names can contain colons, so match up to the closing parenthesis.
DOD_RE = re.compile(r"#dod:\s*([^\s)\]\"'\\]+)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", help="<namespace>/<version>/<short_name> (a garden/ or grapher/ prefix is ignored)")
    parser.add_argument(
        "--staging",
        metavar="BRANCH",
        help="read the DoD text from this branch's staging server instead of production",
    )
    args = parser.parse_args()
    parts = args.step.replace("data://", "").strip("/").split("/")
    namespace, version, short_name = parts[-3:]

    # DoD name -> set of surfaces that reference it.
    refs: dict[str, set[str]] = defaultdict(set)

    # Indicator metadata in the local grapher build (holds the PR's new text).
    local = REPO / "data" / "grapher" / namespace / version / short_name
    if local.exists():
        for name in _dods_in_local_dataset(local):
            refs[name].add(f"indicator metadata (local build of {namespace}/{version}/{short_name})")
    else:
        print(f"Note: no local grapher build at {local}; only published text is scanned.\n")

    # Indicator metadata and charts on production, for any version of the dataset.
    variables = _query(
        "SELECT v.id, v.catalogPath, v.name, v.titlePublic, v.descriptionShort, v.descriptionKey, "
        "v.descriptionProcessing, v.descriptionFromProducer, v.display "
        "FROM variables v JOIN datasets d ON d.id = v.datasetId "
        f"WHERE d.catalogPath LIKE '{namespace}/%/{short_name}'"
    )
    for row in variables:
        text = " ".join(str(v) for k, v in row.items() if k not in ("id", "catalogPath") and v)
        for name in DOD_RE.findall(text):
            refs[name].add(f"indicator metadata (production, {row['catalogPath'].split('/')[2]})")

    # Published charts using those indicators (selected in SQL: long ID or slug lists overflow the URL).
    dataset_charts = (
        "SELECT DISTINCT c.id FROM charts c JOIN chart_dimensions cd ON cd.chartId = c.id "
        "JOIN variables v ON v.id = cd.variableId JOIN datasets d ON d.id = v.datasetId "
        f"WHERE c.isPublished AND d.catalogPath LIKE '{namespace}/%/{short_name}'"
    )
    charts = _query(f"SELECT c.slug, c.config FROM charts c WHERE c.id IN ({dataset_charts})")
    for row in charts:
        for name in DOD_RE.findall(str(row["config"])):
            refs[name].add(f"chart {row['slug']}")

    # Narrative charts built on those charts render their own config, which can add DoD links.
    narratives = _query(
        "SELECT nc.id, nc.name, cc.config FROM narrative_charts nc JOIN chart_configs cc ON cc.id = nc.chartConfigId "
        f"WHERE nc.parentChartId IN ({dataset_charts})"
    )
    # So do narrative charts built on views of published MDims. `multi_dim_x_chart_configs.variableId` holds only a
    # view's first indicator, so match on the indicators listed in the view's own chart config.
    variable_ids = {row["id"] for row in variables}
    for row in _query(
        "SELECT nc.id, nc.name, cc.config, vc.config AS view_config FROM narrative_charts nc "
        "JOIN chart_configs cc ON cc.id = nc.chartConfigId "
        "JOIN multi_dim_x_chart_configs mx ON mx.id = nc.parentMultiDimXChartConfigId "
        "JOIN multi_dim_data_pages m ON m.id = mx.multiDimId JOIN chart_configs vc ON vc.id = mx.chartConfigId "
        "WHERE m.published"
    ):
        if variable_ids & {d.get("variableId") for d in _json(row["view_config"]).get("dimensions") or []}:
            narratives.append(row)
    for row in narratives:
        for name in DOD_RE.findall(str(row["config"])):
            refs[name].add(f"narrative chart {row['name']}")
    narrative_names = {row["name"] for row in narratives}
    narrative_ids = {str(row["id"]) for row in narratives}

    # Slugs posts can use for those charts: the current one, plus old slugs that redirect to it.
    dataset_slugs = (
        f"SELECT c.slug FROM charts c WHERE c.id IN ({dataset_charts}) "
        f"UNION SELECT r.slug FROM chart_slug_redirects r WHERE r.chart_id IN ({dataset_charts})"
    )
    slugs = {row["slug"] for row in _query(dataset_slugs)}

    # ETL viz configs (MDims, explorers, ETL-authored charts) whose step depends on the dataset in the DAG.
    for path in _viz_configs_using(namespace, short_name):
        for name in DOD_RE.findall(path.read_text()):
            refs[name].add(f"viz config {path.relative_to(REPO)}")

    # Posts that show those charts as a pasted URL, possibly behind a Google redirect wrapper.
    post_ids = {
        row["sourceId"]
        for row in _query(
            "SELECT sourceId, target FROM posts_gdocs_links WHERE linkType = 'url' "
            "AND (target LIKE '%/grapher/%' OR target LIKE '%google.%/url?%')"
        )
        if _grapher_slug(row["target"]) in slugs
    }
    # Data insights that show one of those narrative charts in their `narrative-chart` front matter (its name or
    # its admin URL), which creates no link row.
    for row in _query(
        "SELECT id, content->>'$.\"narrative-chart\"' AS target FROM posts_gdocs "
        "WHERE type = 'data-insight' AND published AND coalesce(content->>'$.\"narrative-chart\"', '') <> ''"
    ):
        target = row["target"].strip()
        admin_id = re.search(r"/narrative-charts/(\d+)", target)
        if target in narrative_names or (admin_id and admin_id.group(1) in narrative_ids):
            post_ids.add(row["id"])

    # Articles and data insights that embed or link those charts (as a chart, a guided chart or a pasted URL) or
    # their narrative charts.
    rows = _query(
        "SELECT DISTINCT g.slug AS gdoc, g.type, l2.target AS dod "
        "FROM posts_gdocs_links l1 JOIN posts_gdocs g ON g.id = l1.sourceId "
        "JOIN posts_gdocs_links l2 ON l2.sourceId = g.id AND l2.linkType = 'dod' "
        "WHERE g.published AND ("
        f"(l1.linkType IN ('grapher', 'guided-chart') AND l1.target IN ({dataset_slugs})) "
        f"OR (l1.linkType = 'narrative-chart' AND l1.target IN ({_sql_list(narrative_names)})) "
        f"OR g.id IN ({_sql_list(post_ids)}))"
    )
    for row in rows:
        refs[row["dod"].strip()].add(f"{row['type']} {row['gdoc']}")

    # Data insights hold their chart in the `grapher-url` front matter, which creates no `grapher` link row.
    rows = _query(
        "SELECT DISTINCT g.slug AS gdoc, l.target AS dod "
        "FROM posts_gdocs g JOIN posts_gdocs_links l ON l.sourceId = g.id AND l.linkType = 'dod' "
        "WHERE g.type = 'data-insight' AND g.published "
        "AND split_part(split_part(g.content->>'$.\"grapher-url\"', '/grapher/', 2), '?', 1) IN "
        f"({dataset_slugs})"
    )
    for row in rows:
        refs[row["dod"].strip()].add(f"data-insight {row['gdoc']}")

    if not refs:
        print("No DoD references found.")
        return

    # Fetch the DoD text from production, and from the staging server when asked.
    names = ",".join(f"'{n}'" for n in sorted(refs))
    dods = {r["name"]: r for r in _query(f"SELECT name, updatedAt, content FROM dods WHERE name IN ({names})")}
    source = "production"
    if args.staging:
        from etl.config import OWIDEnv

        production = dods
        staging = OWIDEnv.from_staging(args.staging)
        df = staging.read_sql(
            "SELECT name, updatedAt, content FROM dods WHERE name IN %(n)s", params={"n": tuple(refs)}
        )
        dods = {r["name"]: r for r in df.to_dict("records")}
        for name, row in dods.items():
            if production.get(name, {}).get("content", "").strip() != (row["content"] or "").strip():
                row["note"] = "staging text differs from the production mirror"
        source = f"staging server {staging.conf.DB_HOST}"

    print(f"# DoDs referenced by {namespace}/{short_name} ({len(refs)} names, {len(charts)} published charts)\n")
    print(f"DoD text from {source}.\n")
    missing = sorted(set(refs) - set(dods))
    if missing:
        print("## Referenced but missing from the dods table (broken links)\n")
        for name in missing:
            print(f"- {name}: {', '.join(sorted(refs[name])[:5])}")
        print()
    for name in sorted(dods):
        surfaces = sorted(refs[name])
        shown = ", ".join(surfaces[:5]) + (f", and {len(surfaces) - 5} more" if len(surfaces) > 5 else "")
        note = f"; {dods[name]['note']}" if dods[name].get("note") else ""
        print(f"## {name} (last edited {dods[name]['updatedAt']}{note})\n")
        print(f"Referenced from: {shown}\n")
        print(dods[name]["content"].strip() + "\n")


def _dods_in_local_dataset(path: Path) -> set[str]:
    """Collect DoD names from every metadata field of a locally built dataset."""
    from owid.catalog import Dataset

    ds = Dataset(path)
    found = set(DOD_RE.findall(str(ds.metadata.to_dict())))
    for table_name in ds.table_names:
        tb = ds.read(table_name, load_data=False)
        for col in tb.columns:
            found |= set(DOD_RE.findall(str(tb[col].metadata.to_dict())))
    return found


def _viz_configs_using(namespace: str, short_name: str) -> list[Path]:
    """Config files of the viz steps that list any version of the dataset as a direct DAG dependency.

    A viz step `viz://<type>/<ns>/<version>/<name>` keeps its configs in `etl/steps/viz/<type>/<ns>/<version>/` as
    `<name>.yml` or `<name>.<part>.yml`.
    """
    from etl.dag_helpers import load_dag

    dep_re = re.compile(rf"data(-private)?://(garden|grapher)/{re.escape(namespace)}/[^/]+/{re.escape(short_name)}")
    paths: list[Path] = []
    for step, deps in load_dag().items():
        if not step.startswith("viz://") or not any(dep_re.fullmatch(dep) for dep in deps):
            continue
        folder = REPO / "etl" / "steps" / "viz" / Path(step.removeprefix("viz://")).parent
        name = step.rsplit("/", 1)[-1]
        paths += sorted({*folder.glob(f"{name}.yml"), *folder.glob(f"{name}.*.yml")})
    return paths


def _grapher_slug(url: str) -> str:
    """Chart slug of a pasted grapher URL, looking through a `google.com/url?` redirect wrapper (`q=` or `url=`)."""
    parts = urlsplit(url)
    if "google." in parts.netloc and parts.path == "/url":
        params = parse_qs(parts.query)
        url = next((params[k][0] for k in ("q", "url") if params.get(k, [""])[0].startswith("http")), url)
    path = urlsplit(url).path
    return path.split("/grapher/", 1)[1].rstrip("/") if "/grapher/" in path else ""


def _json(value) -> dict:
    """A JSON column as a dict (Datasette returns some JSON columns as text)."""
    return json.loads(value) if isinstance(value, str) else (value or {})


def _sql_list(values) -> str:
    """Quoted SQL list for `IN (...)`; `NULL` when empty, which matches nothing."""
    quoted = ["'" + str(v).replace("'", "''") + "'" for v in sorted(values)]
    return ",".join(quoted) or "NULL"


def _query(sql: str) -> list[dict]:
    """Run a read-only query against the public Datasette mirror of production.

    Datasette returns at most 1,000 rows per query (and flags the result as truncated), so page through it.
    """
    rows: list[dict] = []
    page_size = 1000
    while True:
        paged = f"SELECT * FROM ({sql}) AS q ORDER BY 1 LIMIT {page_size} OFFSET {len(rows)}"
        response = session.get(DATASETTE, params={"sql": paged, "_shape": "objects"}, timeout=120)
        response.raise_for_status()
        data = response.json()
        # A failed query still returns `rows: []`, so check the error flag, not just the key.
        if data.get("ok") is False or data.get("error") or "rows" not in data:
            raise RuntimeError(f"Datasette query failed: {data.get('error', data)}")
        rows.extend(data["rows"])
        if len(data["rows"]) < page_size:
            return rows


if __name__ == "__main__":
    main()
