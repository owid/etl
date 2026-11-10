"""List every DoD (glossary entry) a dataset's readers see, with where it appears and its current text.

DoDs are reader-facing text (shown on hover) that live in the grapher DB, not in the repo, so a PR diff never
shows them and a metadata review that reads only the `.meta.yml` misses them. This script collects every
`#dod:<name>` reference from the surfaces that render the dataset, then fetches each DoD's text from production.

Surfaces scanned:
  - indicator metadata: the local grapher build (new text, if built) and production variables (any version)
  - chart configs of production charts that use the dataset's indicators (any version)
  - ETL viz configs (charts, MDims, explorers) in this repo that mention the dataset's short name
  - articles and data insights that embed or link those charts (their own DoD links)

DoD text is read from the public Datasette mirror of production (no DB credentials needed; the mirror can lag
production by a few days). The DoD extension's script (vscode_extensions/dod-syntax/scripts/fetch_dod.py) reads
the same `dods` table straight from MySQL, preferring the branch's staging server when `.env` sets STAGING.

Usage:
    .venv/bin/python .claude/skills/fact-check-dataset/scripts/collect_dods.py <namespace>/<version>/<short_name>
"""

import argparse
import re
from collections import defaultdict
from pathlib import Path

from etl.http import session

DATASETTE = "https://datasette-public.owid.io/owid.json"
REPO = Path(__file__).resolve().parents[4]
DOD_RE = re.compile(r"#dod:\s*([A-Za-z0-9_\-]+)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", help="<namespace>/<version>/<short_name> (a garden/ or grapher/ prefix is ignored)")
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

    # ETL viz configs (MDims, explorers, ETL-authored charts) that mention the dataset.
    for path in (REPO / "etl" / "steps" / "viz").rglob("*.yml"):
        text = path.read_text()
        if short_name in text:
            for name in DOD_RE.findall(text):
                refs[name].add(f"viz config {path.relative_to(REPO)}")

    # Articles and data insights that embed or link those charts.
    rows = _query(
        "SELECT DISTINCT g.slug AS gdoc, g.type, l2.target AS dod "
        "FROM posts_gdocs_links l1 JOIN posts_gdocs g ON g.id = l1.sourceId "
        "JOIN posts_gdocs_links l2 ON l2.sourceId = g.id AND l2.linkType = 'dod' "
        f"WHERE l1.linkType = 'grapher' AND g.published AND l1.target IN "
        f"(SELECT c.slug FROM charts c WHERE c.id IN ({dataset_charts}))"
    )
    for row in rows:
        refs[row["dod"].strip()].add(f"{row['type']} {row['gdoc']}")

    if not refs:
        print("No DoD references found.")
        return

    # Fetch the DoD text from production.
    names = ",".join(f"'{n}'" for n in sorted(refs))
    dods = {r["name"]: r for r in _query(f"SELECT name, updatedAt, content FROM dods WHERE name IN ({names})")}

    print(f"# DoDs referenced by {namespace}/{short_name} ({len(refs)} names, {len(charts)} published charts)\n")
    missing = sorted(set(refs) - set(dods))
    if missing:
        print("## Referenced but missing from the dods table (broken links)\n")
        for name in missing:
            print(f"- {name}: {', '.join(sorted(refs[name])[:5])}")
        print()
    for name in sorted(dods):
        surfaces = sorted(refs[name])
        shown = ", ".join(surfaces[:5]) + (f", and {len(surfaces) - 5} more" if len(surfaces) > 5 else "")
        print(f"## {name} (last edited {dods[name]['updatedAt']})\n")
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
        if "rows" not in data:
            raise RuntimeError(f"Datasette query failed: {data.get('error', data)}")
        rows.extend(data["rows"])
        if len(data["rows"]) < page_size:
            return rows


if __name__ == "__main__":
    main()
