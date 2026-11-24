"""Prepare a fact check in one command: everything mechanical, done at once, written to a dossier.

A fact check spends most of its time finding things: the steps behind a chart, the snapshot's origin, the
metadata as readers see it, the producer's documentation, which indicators matter, which values look odd. This
script does all of that concurrently and prints a compact dossier, so the review starts at the first judgement.

Targets (one of):
    prepare.py <namespace>/<version>/<short_name>     a dataset (a `data://` or `garden/` prefix is ignored;
                                                      a bare <short_name> resolves to its latest active version)
    prepare.py --chart <slug | grapher URL | chart id>  a chart: its indicators, its strings, its displayed data
    prepare.py --brief <check_facts.md>               a static chart's brief from `/owid-staff:create-figma-chart`

Options:
    --context update|create|standalone|claims|snapshot   what the caller needs (default: standalone)
    --top N / --full / --indicators a,b    which indicators get the deep review (default: top 10 by chart views)
    --data <csv> / --strings <file>        a chart's displayed data and the strings to check (chart targets)
    --staging <branch>                     read a draft chart, or DoD text, from that branch's staging server
    --refresh                              fetch the producer's documentation again, ignoring saved copies

What it writes, under `ai/fact-check/<namespace>/<version>/<short_name>/` (a chart also gets
`ai/fact-check/charts/<slug>/`):
    dossier.md              what this command prints
    docs/, docs/index.json  the producer's documentation as plain text, with status and fetch date per link
    metadata.md             every indicator's metadata as readers see it
    metadata_changes.diff   metadata text that changed against master or the previous version
    ranking.csv             every indicator: chart views, rank, anomaly flags, selected or skipped (and why)
    anomalies.csv           the anomaly scan's candidates
    dods.md                 the Details on Demand the dataset's readers see
    lane_claims.md, lane_values.md   the two briefs of a full review

Saved documentation is reused for 7 days within a dataset version; data files are hashed live every run.
"""

import argparse
import datetime as dt
import difflib
import importlib.util
import json
import re
import subprocess
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pandas as pd
import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import anomaly_scan  # noqa: E402
import collect_dods  # noqa: E402
import fetch_docs  # noqa: E402

from etl import paths  # noqa: E402
from etl.http import session  # noqa: E402

CONTEXTS = ("update", "create", "standalone", "claims", "snapshot")
FULL_CONTEXTS = ("update", "create", "standalone")
SITE = "https://ourworldindata.org/grapher"
INDICATOR_API = "https://api.ourworldindata.org/v1/indicators"
SMALL_DATASET = 20
# File types a download URL can end in; any other suffix is part of a name (an API path such as `NY.GDP.ZS.AD`).
FILE_TYPES = {
    *fetch_docs.DATA_SUFFIXES,
    ".tsv",
    ".txt",
    ".json",
    ".xml",
    ".pdf",
    ".ods",
    ".xlsm",
    ".xlsb",
    ".tar",
    ".tgz",
    ".bz2",
    ".xz",
    ".sas7bdat",
}
MAX_SERIES_SHOWN = 30
MAX_FLAGGED = 20
MAX_TEXT_LINKS = 25
ANOMALY_ORDER = ("UNIT", "SIGN", "WORLD_SUM", "COVERAGE", "EMPTY", "BREAK", "OUTLIER", "CONSTANT")
# Where each kind of text lives in a rendered indicator, in the order a reader meets it.
META_FIELDS = (
    "title",
    "title_public",
    "unit",
    "short_unit",
    "description_short",
    "description_key",
    "description_processing",
    "description_from_producer",
    "chart_title",
    "chart_subtitle",
    "chart_note",
)
SCOPE = {
    "update": "new or changed metadata text, and the newly added data (latest wave or year) plus the anchors",
    "create": "all indicators when they are few, otherwise the main ones, with a few value cross-checks each",
    "standalone": "the selected indicators and every anomaly candidate",
    "claims": "only the metadata text that was added or edited, and the DoDs it links; no data checks",
    "snapshot": "only the `.dvc` origin against the producer's documentation; no dataset is built yet",
}


# ----------------------------------------------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", nargs="?", help="<namespace>/<version>/<short_name>, or a bare <short_name>")
    parser.add_argument("--chart", help="a chart: slug, grapher URL or numeric id")
    parser.add_argument("--brief", type=Path, help="a static chart's check_facts.md brief")
    parser.add_argument("--context", choices=CONTEXTS, default="standalone")
    parser.add_argument("--top", type=int, default=10, help="indicators to deep-review, by chart views")
    parser.add_argument("--full", action="store_true", help="deep-review every indicator")
    parser.add_argument("--indicators", help="comma-separated columns to deep-review, whatever their views")
    parser.add_argument(
        "--max-flagged",
        type=int,
        default=MAX_FLAGGED,
        help="anomaly-flagged indicators to deep-review beyond the top N (the rest are listed as SKIPPED)",
    )
    parser.add_argument("--data", type=Path, help="the chart's displayed data (CSV)")
    parser.add_argument("--strings", type=Path, help="the strings to check, one per line")
    parser.add_argument("--staging", metavar="BRANCH", help="a branch's staging server (draft charts, DoD text)")
    parser.add_argument("--refresh", action="store_true", help="fetch the documentation again")
    args = parser.parse_args()

    if sum(bool(x) for x in (args.step, args.chart, args.brief)) != 1:
        parser.error("give exactly one target: a dataset step, --chart or --brief")
    if args.step:
        print(prepare_dataset(args))
    else:
        print(prepare_chart(args))


# ----------------------------------------------------------------------------------------------------------------
# Dataset target
# ----------------------------------------------------------------------------------------------------------------


def prepare_dataset(args: argparse.Namespace) -> str:
    from etl.dag_helpers import load_dag

    dag = load_dag()
    namespace, version, short_name = parse_step(args.step, dag)
    dossier = dossier_dir(namespace, version, short_name)
    context = args.context
    full_review = context in FULL_CONTEXTS

    chain, siblings = pipeline(dag, namespace, version, short_name)
    files = step_files(chain + siblings, namespace, version, short_name)
    # The metadata comes first: the origins its indicators carry say which upstream snapshots are this dataset's.
    metadata = outcome_of(rendered_metadata, namespace, version, short_name)
    indicator_origins = (
        unique_origins([o for item in metadata[1] for o in item.get("origins", [])]) if ok(metadata) else []
    )
    origins = all_origins(dag, chain, files, indicator_origins)
    links = collect_links(origins, files["text"])
    garden = paths.DATA_DIR / "garden" / namespace / version / short_name

    # Everything that waits on the network or on a large table runs at once.
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs: dict[str, Future] = {
            "docs": pool.submit(fetch_docs.fetch_all, links, dossier, args.refresh),
            "freshness": pool.submit(check_sources, origins),
        }
        if context != "snapshot":
            jobs["dods"] = pool.submit(collect_dods.collect, namespace, version, short_name, args.staging)
        if full_review:
            jobs["ranking"] = pool.submit(chart_ranking, namespace, short_name)
            if garden.exists():
                jobs["anomalies"] = pool.submit(anomaly_scan.scan_dataset, garden)
    done = {name: outcome(job) for name, job in jobs.items()}
    done["metadata"] = metadata

    out = [f"# Fact-check dossier: `{namespace}/{version}/{short_name}`", ""]
    out += [f"- context: **{context}**. Scope: {SCOPE[context]}."]
    out += [f"- dossier directory: `{rel(dossier)}` (write scratch files and results here)", ""]
    out += section_pipeline(chain, siblings, files)
    out += section_origins(origins)
    out += section_docs(done["docs"], dossier)
    out += section_freshness(done["freshness"])
    out += section_corrections(files["corrections"])

    source, indicators = done["metadata"] if ok(done["metadata"]) else ("", [])
    out += section_metadata(done["metadata"], dossier, files, namespace, version, short_name)

    selected: list[dict] = []
    if full_review:
        findings, notes = done.get("anomalies", ([], [])) if ok(done.get("anomalies")) else ([], [])
        if "anomalies" not in done:
            notes = [
                f"NOT RUN: the garden dataset is not built. Build it with "
                f"`.venv/bin/etlr data://garden/{namespace}/{version}/{short_name}` and run this command again."
            ]
        elif not ok(done["anomalies"]):
            notes = [f"FAILED ({done['anomalies']}): run `scripts/anomaly_scan.py` by hand."]
        anomaly_scan.write_csv(findings, dossier / "anomalies.csv")
        rows, ranking_note = select_indicators(
            indicators, done["ranking"], findings, args.top, args.full, split_list(args.indicators), args.max_flagged
        )
        pd.DataFrame(rows).to_csv(dossier / "ranking.csv", index=False)
        selected = [row for row in rows if row["selected"]]
        out += section_selection(rows, ranking_note, dossier)
        out += section_anomalies(findings, notes, dossier, {row["identity"] for row in selected})
    if context != "snapshot":
        out += section_dods(done["dods"], dossier)
    out += section_earlier(short_name, dossier)
    out += section_next(context, dossier, namespace, version, short_name, selected, origins)

    text = "\n".join(out)
    (dossier / "dossier.md").write_text(text)
    return text


def parse_step(text: str, dag: dict) -> tuple[str, str, str]:
    """(namespace, version, short_name) from a step path, a `data://` URI or a bare short name."""
    parts = re.sub(r"^[a-z-]+://", "", text.strip()).strip("/").split("/")
    if parts[0] in ("meadow", "garden", "grapher"):
        parts = parts[1:]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) != 1:
        sys.exit(f"Cannot read `{text}` as <namespace>/<version>/<short_name>.")
    found = set()
    for step in set(dag) | {dep for deps in dag.values() for dep in deps}:
        match = re.fullmatch(r"data(?:-private)?://garden/([^/]+)/([^/]+)/([^/]+)", step)
        if match and match.group(3) == parts[0]:
            found.add(match.groups())
    namespaces = {key[0] for key in found}
    if not found:
        sys.exit(f"No active garden step is named `{parts[0]}`.")
    if len(namespaces) > 1:
        listing = ", ".join(sorted("/".join(key) for key in found))
        sys.exit(f"`{parts[0]}` is ambiguous ({listing}): ask the user which one, then pass the full path.")
    return max(found, key=lambda key: key[1])


def dossier_dir(namespace: str, version: str, short_name: str) -> Path:
    path = paths.BASE_DIR / "ai" / "fact-check" / namespace / version / short_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def pipeline(dag: dict, namespace: str, version: str, short_name: str) -> tuple[list[str], list[str]]:
    """The dataset's chain (snapshot to grapher) and the same-version siblings it depends on, from the DAG."""
    overview = load_skill_script("update-dataset/scripts/pipeline_overview.py")
    seeds = [
        step
        for step in overview.graph_nodes(dag)
        if step.partition("://")[0] in overview.CHAIN_SCHEMES
        and overview.step_key(step) == (namespace, version, short_name)
    ]
    if not seeds:
        return [], []
    members = overview.find_upstream_members(dag, seeds, namespace, version, short_name)
    chain = sorted(
        (s for s in members if (key := overview.step_key(s)) and key[2] == short_name), key=overview.stage_order
    )
    return chain, sorted(members - set(chain), key=overview.stage_order)


def step_files(steps: list[str], namespace: str, version: str, short_name: str) -> dict[str, list[Path]]:
    """Files of the chain, by use: `.dvc` origins, text to read for links, corrections, metadata."""
    overview = load_skill_script("update-dataset/scripts/pipeline_overview.py")
    found: list[Path] = []
    for step in steps:
        for file in overview.step_files(step):
            found += sorted(p for p in file.rglob("*") if p.is_file()) if file.is_dir() else [file]
    if not steps:
        # Not in the DAG yet (a snapshot being created): take what is on disk for this path.
        found += sorted((paths.SNAPSHOTS_DIR / namespace / version).glob(f"{short_name}*"))
        for channel in ("meadow", "garden", "grapher"):
            found += sorted((paths.STEPS_DATA_DIR / channel / namespace / version).glob(f"{short_name}*"))
    found = [f for f in dict.fromkeys(found) if f.is_file() and "__pycache__" not in f.parts]
    return {
        "all": found,
        "dvc": [f for f in found if f.suffix == ".dvc"],
        "text": [f for f in found if f.suffix in (".yml", ".py", ".md")],
        "meta": [f for f in found if f.name.endswith(".meta.yml") or f.name.endswith(".meta.override.yml")],
        "corrections": [f for f in found if f.name.endswith(".corrections.yml")],
    }


def read_origins(dvc_files: list[Path]) -> list[dict]:
    """The origin of each snapshot, with the md5 and size of the file it holds."""
    origins = []
    for file in dvc_files:
        content = yaml.safe_load(file.read_text()) or {}
        meta = content.get("meta") or {}
        origin = dict(meta.get("origin") or {})
        outs = (content.get("outs") or [{}])[0]
        origin.update(file=rel(file), md5=outs.get("md5", ""), size=outs.get("size"), stored=file.name[: -len(".dvc")])
        if not meta.get("origin"):
            origin["note"] = "no `meta.origin` block (an old `source`-style snapshot)"
        origins.append(origin)
    return origins


def collect_links(
    origins: list[dict], text_files: list[Path], first_texts: list[str] | None = None
) -> list[tuple[str, str]]:
    """Every producer link, with where it came from; the first role seen wins.

    Origin links always count. Links found in metadata and code are capped at `MAX_TEXT_LINKS` (a dataset like WDI
    carries one per series); `first_texts` (a chart's own indicator texts) are read before the files, so the cap
    never drops the links that matter to the chart. Links past the cap are returned with the role `not-fetched`.
    """
    links: dict[str, str] = {}
    for origin in origins:
        for key in ("url_main", "url_download"):
            if origin.get(key):
                links.setdefault(str(origin[key]), key)
        license_url = (origin.get("license") or {}).get("url") if isinstance(origin.get("license"), dict) else None
        if license_url:
            links.setdefault(str(license_url), "license")
        for key in ("description", "citation_full", "description_snapshot"):
            for url in fetch_docs.extract_links(str(origin.get(key) or "")):
                links.setdefault(url, "metadata")
    from_text = 0
    sources = [("metadata", text) for text in first_texts or []]
    sources += [
        ("code" if file.suffix == ".py" else "metadata", file.read_text(errors="replace")) for file in text_files
    ]
    for role, text in sources:
        for url in fetch_docs.extract_links(text):
            if url in links:
                continue
            from_text += 1
            links[url] = role if from_text <= MAX_TEXT_LINKS else "not-fetched"
    return list(links.items())


def check_sources(origins: list[dict]) -> list[dict]:
    """Hash each snapshot's source file again (never cached): an in-place revision is the top find of Phase 0."""
    converted = [o for o in origins if o.get("url_download") and o.get("md5") and converted_download(o)]
    todo = [o for o in origins if o.get("url_download") and o.get("md5") and o not in converted]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(lambda o: fetch_docs.check_freshness(str(o["url_download"]), o["md5"], o.get("size")), todo)
        )
    for origin, result in zip(todo, results):
        result["file"] = origin["file"]
        result["date_accessed"] = str(origin.get("date_accessed", ""))
    skipped = [o for o in origins if not (o.get("url_download") and o.get("md5"))]
    results += [
        {
            "file": o["file"],
            "status": "by-hand",
            "detail": "no `url_download`: the snapshot is fetched by a script or by hand",
            "date_accessed": str(o.get("date_accessed", "")),
        }
        for o in skipped
    ]
    results += [
        {
            "file": o["file"],
            "status": "by-hand",
            "detail": f"{converted_download(o)}: the download's bytes cannot match the snapshot's",
            "date_accessed": str(o.get("date_accessed", "")),
        }
        for o in converted
    ]
    return results


def converted_download(origin: dict) -> str:
    """Why the snapshot stores something other than the downloaded file (an archive extracted, a format converted).

    Empty when the two file types agree, or when the URL names no known file type (an API endpoint, or a path
    ending in a dotted identifier such as `NY.GDP.DEFL.ZS.AD`): those are compared.
    """
    downloaded = Path(urlsplit(str(origin["url_download"])).path).suffix.lower()
    if downloaded not in FILE_TYPES:
        return ""
    stored = Path(str(origin.get("stored") or "")).suffix.lower()
    if downloaded and stored and downloaded != stored:
        return f"the source serves a `{downloaded}` file and the snapshot stores a `{stored}`"
    return ""


def rendered_metadata(namespace: str, version: str, short_name: str) -> tuple[str, list[dict]]:
    """Each indicator's text as readers see it: from the local build, else from production."""
    from owid.catalog import Dataset

    for channel in ("grapher", "garden"):
        path = paths.DATA_DIR / channel / namespace / version / short_name
        if not path.exists():
            continue
        dataset = Dataset(path)
        indicators = []
        for table_name in dataset.table_names:
            tb = dataset.read(table_name, load_data=False)
            for column in tb.columns:
                meta = tb[column].metadata
                if not (meta.title or meta.unit is not None or meta.description_short):
                    continue  # a key column (country, year) or an unlabeled helper
                config = (meta.presentation.grapher_config if meta.presentation else None) or {}
                indicators.append(
                    {
                        "identity": f"{table_name}#{column}",
                        "column": column,
                        "title": meta.title,
                        "title_public": meta.presentation.title_public if meta.presentation else None,
                        "unit": meta.unit,
                        "short_unit": meta.short_unit,
                        "description_short": meta.description_short,
                        "description_key": as_text(meta.description_key),
                        "description_processing": meta.description_processing,
                        "description_from_producer": meta.description_from_producer,
                        "chart_title": config.get("title"),
                        "chart_subtitle": config.get("subtitle"),
                        "chart_note": config.get("note"),
                        "origins": [origin.to_dict() for origin in meta.origins or []],
                    }
                )
        return f"the local {channel} build (`data/{channel}/{namespace}/{version}/{short_name}`)", indicators

    dataset_variables = (
        "SELECT v.id FROM variables v JOIN datasets d ON d.id = v.datasetId "
        f"WHERE d.catalogPath = '{namespace}/{version}/{short_name}'"
    )
    rows = collect_dods._query(
        "SELECT v.id, v.catalogPath, v.name, v.unit, v.shortUnit, v.titlePublic, v.descriptionShort, v.descriptionKey, "
        f"v.descriptionProcessing, v.descriptionFromProducer FROM variables v WHERE v.id IN ({dataset_variables})"
    )
    # The origins each indicator carries: a derived dataset has no `.dvc` of its own, so they are its sources.
    carried: dict[int, list[dict]] = {}
    for origin in collect_dods._query(
        "SELECT ov.variableId, o.producer, o.title, o.urlMain, o.urlDownload, o.datePublished, o.dateAccessed, "
        "o.versionProducer, o.citationFull FROM origins o JOIN origins_variables ov ON ov.originId = o.id "
        f"WHERE ov.variableId IN ({dataset_variables})"
    ):
        variable_id = origin.pop("variableId")
        carried.setdefault(variable_id, []).append({k: v for k, v in origin.items() if v is not None})
    indicators = [
        {
            "identity": row["catalogPath"].split("/", 4)[-1] if row["catalogPath"] else row["name"],
            "column": (row["catalogPath"] or "").split("#")[-1],
            "title": row["name"],
            "title_public": row["titlePublic"],
            "unit": row["unit"],
            "short_unit": row["shortUnit"],
            "description_short": row["descriptionShort"],
            "description_key": as_text(row["descriptionKey"]),
            "description_processing": row["descriptionProcessing"],
            "description_from_producer": row["descriptionFromProducer"],
            "origins": carried.get(row["id"], []),
        }
        for row in rows
    ]
    return "production (the internal Datasette's copy; no local build of this version)", indicators


ORIGIN_KEYS = {
    "urlMain": "url_main",
    "urlDownload": "url_download",
    "datePublished": "date_published",
    "dateAccessed": "date_accessed",
    "citationFull": "citation_full",
    "versionProducer": "version_producer",
    "attributionShort": "attribution_short",
    "titleSnapshot": "title_snapshot",
    "descriptionSnapshot": "description_snapshot",
}


def origin_key(origin: dict) -> tuple[str, str]:
    """What identifies an origin across a `.dvc` file and an indicator's metadata: producer and title."""
    return tuple(re.sub(r"\W+", " ", str(origin.get(k) or "").lower()).strip() for k in ("producer", "title"))  # type: ignore[return-value]


def unique_origins(origins: list[dict]) -> list[dict]:
    """Origins as the indicators carry them (snake_case keys), one per producer and title."""
    found: dict[tuple[str, str], dict] = {}
    for origin in origins:
        origin = {ORIGIN_KEYS.get(key, key): value for key, value in origin.items() if value not in (None, "", [])}
        found.setdefault(origin_key(origin), origin)
    return list(found.values())


def upstream_snapshots(dag: dict, seeds: list[str]) -> list[Path]:
    """The `.dvc` file of every snapshot upstream of the given steps, however far up the DAG."""
    seen, todo, found = set(seeds), list(seeds), []
    while todo:
        for dep in dag.get(todo.pop(), ()):
            if dep in seen:
                continue
            seen.add(dep)
            if dep.partition("://")[0].startswith("snapshot"):
                found.append(paths.SNAPSHOTS_DIR / (dep.partition("://")[2] + ".dvc"))
            else:
                todo.append(dep)
    return sorted(path for path in found if path.exists())


def all_origins(dag: dict, chain: list[str], files: dict, indicator_origins: list[dict]) -> list[dict]:
    """The dataset's origins: its own snapshots or, for a derived dataset, the upstream ones its indicators carry.

    A derived dataset (one that combines other datasets) has no snapshot in its own chain: its indicators carry
    the origins of the datasets it reads. Each one is matched to its `.dvc` upstream in the DAG, which gives the
    md5 for the live check of the source file; an origin with no `.dvc` match is kept as the indicator states it.
    A dataset with snapshots of its own keeps just those: the origins its indicators pick up from the datasets it
    merges in (population, regions) are other datasets' sources, not this one's, and on a large dataset they would
    each be fetched and hashed.
    """
    origins = read_origins(files["dvc"])
    if origins:
        return origins
    known = {origin_key(o) for o in origins}
    wanted = {origin_key(o): o for o in indicator_origins if origin_key(o) not in known}
    if not wanted:
        return origins
    own = set(files["dvc"])
    for origin in read_origins([p for p in upstream_snapshots(dag, chain) if p not in own]):
        if origin_key(origin) in wanted:
            wanted.pop(origin_key(origin))
            origin["note"] = "an upstream snapshot whose origin this dataset's indicators carry"
            origins.append(origin)
    for origin in wanted.values():
        origins.append({**origin, "file": "(indicator metadata)", "note": "no upstream `.dvc` matched this origin"})
    return origins


def as_text(value) -> str | None:
    """`description_key` as one text: it is a list in some datasets and a markdown string in others."""
    if value is None or value == [] or value == "":
        return None
    if isinstance(value, (list, tuple)):
        return "\n".join(f"- {item}" for item in value)
    text = str(value)
    if text.startswith("["):
        try:
            return "\n".join(f"- {item}" for item in json.loads(text))
        except ValueError:
            pass
    return text


def chart_ranking(namespace: str, short_name: str) -> tuple[pd.DataFrame, str]:
    """Chart use of every version of the dataset's indicators, by identity (`<table>#<column>`), views first.

    Before a PR merges, live charts still point at the old version, so the match is on namespace and short name
    and the identity leaves the version out.
    """
    try:
        from etl.analytics.config import SEMANTIC_LAYER_SCHEMA as schema
        from etl.analytics.data import read_analytics

        frame = read_analytics(
            # LEFT JOIN the views: a chart with no analytics row yet ranks at 0 instead of dropping out.
            f"SELECT i.catalog_path, COUNT(DISTINCT x.chart_slug) AS n_charts, SUM(c.views_365d) AS views_365d "
            f"FROM {schema}.indicators i JOIN {schema}.charts_x_indicators x ON x.indicator_id = i.indicator_id "
            f"LEFT JOIN (SELECT chart_slug, MAX(views_365d) AS views_365d FROM {schema}.charts GROUP BY 1) c "
            f"ON c.chart_slug = x.chart_slug WHERE i.catalog_path LIKE 'grapher/{namespace}/%/{short_name}/%' "
            "GROUP BY 1"
        )
        source = "chart views over 365 days (analytics)"
    except Exception as error:  # the analytics layer is optional: fall back to counting charts
        rows = collect_dods._query(
            "SELECT v.catalogPath AS catalog_path, COUNT(DISTINCT cd.chartId) AS n_charts "
            "FROM variables v JOIN chart_dimensions cd ON cd.variableId = v.id "
            f"WHERE v.catalogPath LIKE 'grapher/{namespace}/%/{short_name}/%' GROUP BY 1"
        )
        frame = pd.DataFrame(rows, columns=["catalog_path", "n_charts"])
        frame["views_365d"] = 0
        source = f"number of charts (no view counts: analytics unreachable, {type(error).__name__})"
    if frame.empty:
        return pd.DataFrame(columns=["identity", "n_charts", "views_365d"]), source
    frame["identity"] = frame["catalog_path"].str.split("/", n=4).str[-1]
    frame["views_365d"] = pd.to_numeric(frame["views_365d"], errors="coerce").fillna(0)
    ranked = frame.groupby("identity", as_index=False)[["n_charts", "views_365d"]].sum()
    return ranked.sort_values(["views_365d", "n_charts"], ascending=False).reset_index(drop=True), source


def select_indicators(
    indicators: list[dict],
    ranking,
    findings: list[dict],
    top: int,
    full: bool,
    forced: list[str],
    max_flagged: int = MAX_FLAGGED,
) -> tuple[list[dict], str]:
    """Which indicators get the deep review, and why each of the others is skipped. Nothing drops out silently.

    Selected: the `top` most-viewed charted indicators, plus up to `max_flagged` more that the anomaly scan
    flagged (most-viewed first). On a dataset the size of WDI nearly every column carries a flag, so the cap keeps
    the review doable; every indicator past it is written to `ranking.csv` as SKIPPED with its flag count.
    """
    if ok(ranking):
        ranked, source = ranking
    else:
        ranked, source = pd.DataFrame(columns=["identity", "n_charts", "views_365d"]), f"ranking FAILED ({ranking})"
    known = {item["identity"] for item in indicators}
    use = {row["identity"]: row for row in fold_dimensions(ranked, known).to_dict("records")}
    flags: dict[str, int] = {}
    for finding in findings:
        key = f"{finding.get('table', '')}#{finding['column']}"
        flags[key] = flags.get(key, 0) + 1

    rows = []
    for item in indicators:
        charted = use.get(item["identity"], {})
        rows.append(
            {
                "identity": item["identity"],
                "column": item["column"],
                "n_charts": int(charted.get("n_charts", 0)),
                "views_365d": int(charted.get("views_365d", 0)),
                "anomaly_flags": flags_for(item["identity"], flags),
            }
        )
    rows.sort(key=lambda row: (-row["views_365d"], -row["n_charts"], row["identity"]))
    any_charted = any(row["n_charts"] for row in rows)
    review_all = full or (not any_charted and len(rows) <= SMALL_DATASET)
    flagged_room = max_flagged
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
        if row["column"] in forced or row["identity"] in forced:
            row["selected"], row["why"] = True, "named with --indicators"
        elif review_all:
            row["selected"], row["why"] = True, "--full" if full else "no charts yet and few indicators: all reviewed"
        elif any_charted and row["n_charts"] and rank <= top:
            row["selected"], row["why"] = True, f"top {top} by chart use"
        elif row["anomaly_flags"] and flagged_room > 0:
            # Rows come most-viewed first, so the room goes to the flagged indicators readers see most.
            flagged_room -= 1
            row["selected"], row["why"] = True, "flagged by the anomaly scan"
        else:
            row["selected"] = False
            flags_note = (
                f"{row['anomaly_flags']} anomaly flags, past the cap of {max_flagged} flagged indicators"
                if row["anomaly_flags"]
                else "no anomaly flag"
            )
            row["why"] = f"SKIPPED: rank {rank}, {row['views_365d']:,} views, {row['n_charts']} charts, {flags_note}"

    missing = [identity for identity in use if identity not in known]
    note = f"Ranked by {source}."
    if not any_charted and not review_all and not forced:
        note += (
            f" No chart uses these indicators yet and there are {len(rows)}: only the anomaly-flagged ones are "
            "selected. Name the main indicators with `--indicators a,b,c` (or pass `--full`) and run this again; "
            "the documentation is already saved."
        )
    if missing:
        note += (
            f" {len(missing)} charted identities are not in this build (renamed or dropped: resolve them through "
            "the indicator-upgrade mapping, or list them in the report): " + ", ".join(f"`{m}`" for m in missing[:15])
        )
        note += "." if len(missing) <= 15 else f", and {len(missing) - 15} more."
    return rows, note


def fold_dimensions(ranked: pd.DataFrame, known: set[str]) -> pd.DataFrame:
    """Chart use of a dimensioned indicator, summed over the Grapher columns it is published as.

    Grapher publishes each slice of a dimensioned column as its own indicator (`table#rate__sex_female`), while
    the local build has one column (`table#rate`). A chart that shows several slices counts once per slice.
    """
    if ranked.empty:
        return ranked

    def base(identity: str) -> str:
        if identity in known:
            return identity
        parts = identity.split("__")
        for end in range(len(parts) - 1, 0, -1):
            if "__".join(parts[:end]) in known:
                return "__".join(parts[:end])
        return identity

    folded = ranked.assign(identity=ranked["identity"].map(base))
    folded = folded.groupby("identity", as_index=False)[["n_charts", "views_365d"]].sum()
    return folded.sort_values(["views_365d", "n_charts"], ascending=False).reset_index(drop=True)


# ----------------------------------------------------------------------------------------------------------------
# Chart target
# ----------------------------------------------------------------------------------------------------------------


def prepare_chart(args: argparse.Namespace) -> str:
    brief = parse_brief(args.brief) if args.brief else {}
    reference = args.chart or brief.get("link") or brief.get("slug") or ""
    slug, params = parse_chart_reference(reference)
    if not slug:
        sys.exit("No chart found in the brief: pass --chart <slug or URL> as well.")
    folder = paths.BASE_DIR / "ai" / "fact-check" / "charts" / slug
    folder.mkdir(parents=True, exist_ok=True)

    config, chart_texts = load_chart(slug, args.staging)
    variable_ids = [d["variableId"] for d in config.get("dimensions") or [] if d.get("variableId")]
    roles = {d["variableId"]: d.get("property", "") for d in config.get("dimensions") or []}
    with ThreadPoolExecutor(max_workers=8) as pool:
        loaded = {v: pool.submit(load_indicator, v, args.staging) for v in variable_ids}
    indicators = []
    for variable_id, job in loaded.items():
        result = outcome(job)
        # A failed load is said in its own section, never shown as an indicator with no metadata.
        indicators.append(result if ok(result) else {"id": variable_id, "failed": str(result), "role": ""})
    for indicator in indicators:
        indicator["role"] = roles.get(indicator["id"], "")
    # The values the chart plots are what the strings talk about; colour and size carriers come along unchecked.
    main = [i for i in indicators if i["role"] in ("y", "x")] or indicators

    data_path = args.data or (Path(brief["data"]) if brief.get("data") else None)
    if data_path is None or not data_path.exists():
        data_path = download_chart_data(slug, params, folder)
    data = pd.read_csv(data_path) if data_path and data_path.exists() else pd.DataFrame()

    if args.strings:
        strings = [line for line in args.strings.read_text().splitlines() if line.strip()]
    elif brief.get("strings"):
        strings = brief["strings"]
    else:
        strings = [text for text in chart_texts.values() if text]

    datasets = sorted({i["dataset"] for i in main if i.get("dataset")})
    from etl.dag_helpers import load_dag

    dag = load_dag()
    per_dataset = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        texts = {
            key: [str(i.get(f) or "") for i in main if i.get("dataset") == key for f in META_FIELDS] for key in datasets
        }
        carried = {key: [o for i in main if i.get("dataset") == key for o in i.get("origins", [])] for key in datasets}
        futures = {
            key: pool.submit(dataset_sources, dag, key, args.refresh, texts[key], carried[key]) for key in datasets
        }
        per_dataset = {key: outcome(job) for key, job in futures.items()}

    out = [f"# Fact-check dossier: chart `{slug}`", ""]
    if brief.get("id"):
        out += [f"- brief: `{brief['id']}` (the result's first line is `Brief: {brief['id']}`)"]
    out += [f"- view: `{params or 'default'}`", f"- chart directory: `{rel(folder)}` (write scratch files here)", ""]
    out += ["## What the chart says", ""]
    out += [f"- {name}: {json.dumps(text, ensure_ascii=False)}" for name, text in chart_texts.items() if text]
    out += ["", "## Indicators", ""]
    for indicator in indicators:
        if indicator.get("failed"):
            out += [
                f"### indicator {indicator['id']} ({indicator['role'] or 'dimension'})",
                "",
                f"FAILED to load ({indicator['failed']}): read its metadata, origins and steps by hand.",
                "",
            ]
            continue
        checked = "" if indicator in main else " (carrier of color or size: not part of the check)"
        out += [
            f"### `{indicator.get('catalog_path') or indicator['id']}` ({indicator['role'] or 'dimension'}){checked}",
            "",
        ]
        if indicator in main:
            out += [f"- {field}: {block(indicator[field], 1200)}" for field in META_FIELDS if indicator.get(field)]
            out += [""]
    full = [f"# Indicator metadata in full: chart `{slug}`", ""]
    for indicator in main:
        full += [f"## `{indicator.get('catalog_path') or indicator['id']}`", ""]
        full += [f"**{field}**: {indicator[field]}\n" for field in META_FIELDS if indicator.get(field)]
    (folder / "metadata.md").write_text("\n".join(full))
    out += [f"Uncut: `{rel(folder / 'metadata.md')}`.", ""]

    for key in datasets:
        namespace, version, short_name = key
        result = per_dataset[key]
        out += [f"## Dataset `{namespace}/{version}/{short_name}`", ""]
        if not ok(result):
            out += [f"FAILED to prepare ({result}): find its steps, `.dvc` and documentation by hand.", ""]
            continue
        dossier = dossier_dir(namespace, version, short_name)
        out += [f"- dataset dossier: `{rel(dossier)}`"]
        if result["version_note"]:
            out += [f"- {result['version_note']}"]
        out += [""]
        out += section_pipeline(result["chain"], result["siblings"], result["files"])
        out += section_origins(result["origins"])
        out += section_docs(result["docs"], dossier)
        out += section_freshness(result["freshness"])
        columns = {i["column"] for i in main if i.get("dataset") == key}
        out += section_corrections(result["files"]["corrections"], columns)
        out += section_earlier(short_name, dossier)

    out += section_strings(strings, data)
    out += section_data(data, data_path, main)
    out += [
        "## Next",
        "",
        "Follow `.claude/skills/fact-check-dataset/reference/chart-texts.md`: every string above is a claim.",
        "Quote the producer from the saved text in `docs/` (Grep it); fetch only what the table marks unreachable.",
        "",
    ]
    text = "\n".join(out)
    (folder / "dossier.md").write_text(text)
    return text


def parse_brief(path: Path) -> dict:
    """The chart, the displayed data and the strings of a `check_facts.md` brief."""
    text = path.read_text()
    brief: dict = {"strings": []}
    if match := re.search(r"^Brief:\s*(\S+)", text, re.MULTILINE):
        brief["id"] = match.group(1)
    if match := re.search(r"^- chart: `([^`]+)`", text, re.MULTILINE):
        brief["slug"] = match.group(1)
    if match := re.search(r"^- link: (\S+)", text, re.MULTILINE):
        brief["link"] = match.group(1)
    if match := re.search(r"^- the displayed data: `([^`]+)`", text, re.MULTILINE):
        brief["data"] = match.group(1)
    section = text.split("## The strings", 1)[-1].split("## Report", 1)[0] if "## The strings" in text else ""
    for line in section.splitlines():
        # `- title: "…"` and `- "…" (label at {...})`: the string is the first JSON-quoted run on the line.
        if line.startswith("- ") and (match := re.search(r'"((?:[^"\\]|\\.)*)"', line)):
            try:
                brief["strings"].append(json.loads(f'"{match.group(1)}"'))
            except ValueError:
                brief["strings"].append(match.group(1))
    return brief


def parse_chart_reference(reference: str) -> tuple[str, str]:
    """(slug, query string) of a slug, a grapher URL or a numeric chart id."""
    reference = reference.strip()
    if reference.isdigit():
        rows = collect_dods._query(f"SELECT c.slug FROM charts c WHERE c.id = {int(reference)}")
        return (rows[0]["slug"], "") if rows else ("", "")
    if "/" not in reference and "?" not in reference:
        return reference, ""
    parts = urlsplit(reference)
    slug = parts.path.rstrip("/").rsplit("/", 1)[-1]
    return re.sub(r"\.(csv|svg|png|json|config\.json|metadata\.json)$", "", slug), parts.query


def load_chart(slug: str, staging: str | None) -> tuple[dict, dict]:
    """A chart's config and the texts it shows: the live chart, or a draft on a staging server."""
    if staging:
        from etl.config import OWIDEnv

        # The config that renders is the one `charts.configId` names (chart_configs also holds patch layers).
        frame = OWIDEnv.from_staging(staging).read_sql(
            "SELECT cc.config FROM charts c JOIN chart_configs cc ON cc.id = c.configId WHERE cc.slug = %(slug)s",
            params={"slug": slug},
        )
        if frame.empty:
            sys.exit(f"No chart `{slug}` on staging server `{staging}`.")
        value = frame["config"].iloc[0]
        config = json.loads(value) if isinstance(value, (str, bytes)) else value
        citation = ""
    else:
        response = session.get(f"{SITE}/{slug}.config.json", timeout=20)
        if response.status_code != 200:
            sys.exit(
                f"`{slug}` is not a published grapher chart (HTTP {response.status_code}). For a draft, pass "
                "--staging <branch>; for a multidim or explorer view, run the dataset target with --indicators."
            )
        config = response.json()
        meta = session.get(f"{SITE}/{slug}.metadata.json", timeout=20)
        citation = (meta.json().get("chart") or {}).get("citation", "") if meta.status_code == 200 else ""
    texts = {
        "title": config.get("title"),
        "subtitle": config.get("subtitle"),
        "note": config.get("note"),
        "source line": config.get("sourceDesc") or citation,
    }
    return config, texts


def load_indicator(variable_id: int, staging: str | None) -> dict:
    """An indicator's metadata as readers see it, from the public API (or a staging server's database)."""
    if staging:
        from etl.config import OWIDEnv

        env = OWIDEnv.from_staging(staging)
        frame = env.read_sql(
            "SELECT id, name, unit, shortUnit, catalogPath, titlePublic, descriptionShort, descriptionKey, "
            "descriptionProcessing, descriptionFromProducer FROM variables WHERE id = %(id)s",
            params={"id": variable_id},
        )
        if frame.empty:
            raise LookupError(f"indicator {variable_id} is not in staging server `{staging}`'s variables table")
        raw = frame.to_dict("records")[0]
        raw["presentation"] = {"titlePublic": raw.get("titlePublic")}
        origins = env.read_sql(
            "SELECT o.producer, o.title, o.urlMain, o.urlDownload, o.datePublished, o.dateAccessed, "
            "o.versionProducer, o.citationFull FROM origins o JOIN origins_variables ov ON ov.originId = o.id "
            "WHERE ov.variableId = %(id)s",
            params={"id": variable_id},
        )
        raw["origins"] = [{k: v for k, v in row.items() if v is not None} for row in origins.to_dict("records")]
    else:
        response = session.get(f"{INDICATOR_API}/{variable_id}.metadata.json", timeout=20)
        if response.status_code != 200:
            raise LookupError(f"indicator {variable_id}: HTTP {response.status_code} from the indicator API")
        raw = response.json()
    catalog_path = raw.get("catalogPath") or ""
    parts = catalog_path.split("#")[0].split("/")
    dataset = tuple(parts[1:4]) if len(parts) >= 5 else None
    return {
        "id": variable_id,
        "catalog_path": catalog_path,
        "dataset": dataset,
        "column": catalog_path.split("#")[-1] if "#" in catalog_path else "",
        "title": raw.get("name"),
        "title_public": (raw.get("presentation") or {}).get("titlePublic"),
        "unit": raw.get("unit"),
        "short_unit": raw.get("shortUnit"),
        "description_short": raw.get("descriptionShort"),
        "description_key": as_text(raw.get("descriptionKey")),
        "description_processing": raw.get("descriptionProcessing"),
        "description_from_producer": raw.get("descriptionFromProducer"),
        "origins": raw.get("origins") or [],
    }


def download_chart_data(slug: str, params: str, folder: Path) -> Path | None:
    """The data the chart view displays, as the site serves it."""
    query = dict(parse_qsl(params))
    query.update(csvType="filtered", useColumnShortNames="false")
    response = session.get(f"{SITE}/{slug}.csv", params=query, timeout=60)
    if response.status_code != 200:
        return None
    path = folder / "data.csv"
    path.write_bytes(response.content)
    return path


def dataset_sources(
    dag: dict, key: tuple[str, str, str], refresh: bool, first_texts: list[str], indicator_origins: list[dict]
) -> dict:
    """Pipeline, origins, documentation and source freshness of one dataset behind a chart."""
    namespace, version, short_name = key
    chain, siblings = pipeline(dag, namespace, version, short_name)
    version_note = ""
    if not chain:
        # The chart points at a version this checkout no longer has in its active DAG: use the latest that is.
        versions = sorted(p.parent.name for p in (paths.STEPS_DATA_DIR / "garden" / namespace).glob(f"*/{short_name}*"))
        if versions and versions[-1] != version:
            version_note = (
                f"the chart's indicators are at version {version}, which is not in this checkout's active DAG; "
                f"the steps below are {versions[-1]}'s. Confirm they are the ones behind the chart."
            )
            chain, siblings = pipeline(dag, namespace, versions[-1], short_name)
    files = step_files(chain + siblings, namespace, version, short_name)
    carried = unique_origins(indicator_origins)
    if version_note and carried:
        # The steps on disk are a newer version's: their snapshots are a later release than the chart shows. The
        # origins the chart's own indicators carry say which release that is.
        origins = [
            {
                **origin,
                "file": f"(the chart's indicators, version {version})",
                "note": "no `.dvc` of this version on disk",
            }
            for origin in carried
        ]
    else:
        origins = all_origins(dag, chain, files, carried)
    dossier = dossier_dir(namespace, version, short_name)
    with ThreadPoolExecutor(max_workers=2) as pool:
        links = collect_links(origins, files["text"], first_texts)
        docs = pool.submit(fetch_docs.fetch_all, links, dossier, refresh)
        freshness = pool.submit(check_sources, origins)
        return {
            "chain": chain,
            "siblings": siblings,
            "files": files,
            "origins": origins,
            "docs": outcome(docs),
            "freshness": outcome(freshness),
            "version_note": version_note,
        }


NUMBER_RE = re.compile(
    r"(?<![\w.,])[-−–]?[$€£]?\d(?:[\d,]*\d)?(?:\.\d+)?%?(?:\s*(?:thousand|million|billion|trillion))?"
)
SCALES = {"thousand": 1e3, "million": 1e6, "billion": 1e9, "trillion": 1e12}


def numbers_in(text: str) -> list[dict]:
    """Every number a string states: its text, its value, the decimals it shows, and whether it reads as a year."""
    found = []
    for match in NUMBER_RE.finditer(text):
        raw = match.group(0)
        digits = re.sub(r"[^\d.]", "", raw.split()[0])
        if not digits or digits == ".":
            continue
        value = float(digits)
        scale = next((factor for word, factor in SCALES.items() if word in raw), 1)
        decimals = len(digits.split(".")[1]) if "." in digits else 0
        is_year = (
            scale == 1 and decimals == 0 and "," not in raw and not re.search(r"[$€£%]", raw) and 1500 <= value <= 2100
        )
        negative = raw[0] in "-−–"
        if scale == 1 and decimals == 0 and value < 10 and not re.search(r"[$€£%]", raw):
            continue  # "under 5", "top 3": a bare digit matches data by accident
        found.append(
            {
                "text": raw.strip(),
                "value": -value if negative else value,
                "scale": scale,
                "decimals": decimals,
                "year": is_year,
            }
        )
    return found


def match_number(number: dict, data: pd.DataFrame, context: str = "", limit: int = 3) -> list[str]:
    """Where a stated number appears in the displayed data, at the precision the string shows.

    A rounded number can match many cells, so the likeliest come first: a series or an entity that the string
    itself names (`context` is the string), then the most recent period.
    """
    if data.empty:
        return []
    entity_col = next((c for c in ("Entity", "entity", "country") if c in data.columns), None)
    year_col = next((c for c in ("Year", "year", "Day", "date") if c in data.columns), None)
    said = " ".join(context.lower().split())
    hits = []
    for column in data.columns:
        if column in (entity_col, year_col, "Code") or not pd.api.types.is_numeric_dtype(data[column]):
            continue
        shown = (data[column] / number["scale"]).round(number["decimals"])
        for _, row in data[shown == number["value"]].iterrows():
            named = (str(column).lower() in said) + (bool(entity_col) and str(row[entity_col]).lower() in said)
            where = " ".join(str(row[c]) for c in (entity_col, year_col) if c)
            hits.append(
                (-named, -_as_number(row[year_col]) if year_col else 0, f"{where}: {column} = {row[column]:,.6g}")
            )
    hits.sort()
    more = f" (and {len(hits) - limit} more cells)" if len(hits) > limit else ""
    return [hit[2] for hit in hits[:limit]] + ([more.strip()] if more else [])


def section_strings(strings: list[str], data: pd.DataFrame) -> list[str]:
    """Each string, with the numbers it states matched against the displayed data. A match is not a verdict."""
    out = ["## Strings to check", ""]
    if not strings:
        return out + ["None found: pass --strings or --brief.", ""]
    years = _years(data)
    entity_col = next((c for c in ("Entity", "entity", "country") if c in data.columns), None)
    first, last = (int(years.min()), int(years.max())) if years is not None and years.notna().any() else (None, None)
    out += [
        "A number found in the data shows where it comes from; whether the sentence is true of it is still yours to judge.",
        "",
    ]
    for text in strings:
        out += [f"- {json.dumps(text, ensure_ascii=False)}"]
        for number in numbers_in(text):
            if number["year"]:
                if first is not None and not first <= number["value"] <= last:
                    out += [
                        f"  - `{number['text']}`: a year outside the displayed data ({first}–{last}): "
                        "a release year, an external fact, or a mistake"
                    ]
                elif first is not None and entity_col:
                    columns = series_named(text, data, entity_col)
                    at_year = data.loc[years == number["value"]]
                    have = set(at_year.loc[at_year[columns].notna().any(axis=1), entity_col]) if columns else set()
                    lacking = [str(e) for e in dict.fromkeys(data[entity_col]) if e not in have]
                    if lacking:
                        which = (
                            f" in {', '.join(map(str, columns))}"
                            if len(columns) < len(value_columns(data, entity_col))
                            else ""
                        )
                        out += [f"  - `{number['text']}`: NO data that year{which} for {', '.join(lacking[:8])}"]
                continue
            hits = match_number(number, data, text)
            if hits:
                out += [f"  - `{number['text']}`: in the data at " + "; ".join(hits)]
            else:
                out += [f"  - `{number['text']}`: NOT in the displayed data (derived or external: check it by hand)"]
    return out + [""]


def value_columns(data: pd.DataFrame, entity_col: str | None) -> list:
    """The displayed data's numeric series (not the entity, its code or the time column)."""
    skip = {entity_col, "Code", "Year", "year", "Day", "date"}
    return [c for c in data.columns if c not in skip and pd.api.types.is_numeric_dtype(data[c])]


def series_named(text: str, data: pd.DataFrame, entity_col: str | None) -> list:
    """The series a string names (by column name, case-insensitive), or every series when it names none."""
    columns = value_columns(data, entity_col)
    said = " ".join(text.lower().split())
    named = [c for c in columns if str(c).lower() in said]
    return named or columns


def section_data(data: pd.DataFrame, data_path: Path | None, main: list[dict]) -> list[str]:
    out = ["## Displayed data", ""]
    if data.empty:
        return out + ["Not available: download the chart's CSV by hand.", ""]
    entity_col = next((c for c in ("Entity", "entity", "country") if c in data.columns), None)
    year_col = next((c for c in ("Year", "year", "Day", "date") if c in data.columns), None)
    out += [f"- file: `{data_path}` ({len(data):,} rows; columns: {', '.join(map(str, data.columns))})"]
    if year_col:
        out += [f"- runs {data[year_col].min()} to {data[year_col].max()}"]
    if entity_col:
        entities = list(dict.fromkeys(data[entity_col]))
        out += [
            f"- {len(entities)} entities: {', '.join(map(str, entities[:20]))}" + (" …" if len(entities) > 20 else "")
        ]
        values = [
            c
            for c in data.columns
            if c not in (entity_col, year_col, "Code") and pd.api.types.is_numeric_dtype(data[c])
        ]
        # Each series' first and last value, with the change between them: what most derived claims rest on.
        if year_col and len(entities) * len(values) <= MAX_SERIES_SHOWN:
            ordered = data.sort_values(year_col)
            for entity in entities:
                for column in values:
                    series = ordered.loc[ordered[entity_col] == entity, [year_col, column]].dropna()
                    if series.empty:
                        continue
                    (first_year, start), (last_year, end) = series.iloc[0], series.iloc[-1]
                    change = f": ×{end / start:.3g}, {(end / start - 1) * 100:+.1f}%" if start > 0 else ""
                    out += [
                        f"  - {entity}, {column}: {start:,.6g} ({_period(first_year)}) → {end:,.6g} ({_period(last_year)}){change}"
                    ]
        elif year_col:
            out += [f"  - {len(entities) * len(values)} series: read first and last values from the file"]
    return out + [""]


# ----------------------------------------------------------------------------------------------------------------
# Dossier sections
# ----------------------------------------------------------------------------------------------------------------


def section_pipeline(chain: list[str], siblings: list[str], files: dict) -> list[str]:
    out = ["## Pipeline", ""]
    if not chain:
        out += ["Not in the active DAG; files found on disk:"]
    out += [f"- `{step}`" for step in chain]
    out += [f"- `{step}` (sibling)" for step in siblings]
    out += [
        "",
        "Files: " + ", ".join(f"`{rel(f)}`" for f in files["all"]) if files["all"] else "No step files found.",
        "",
    ]
    return out


def section_origins(origins: list[dict]) -> list[str]:
    out = [f"## Origins ({len(origins)} snapshot{'s' if len(origins) != 1 else ''})", ""]
    fields = (
        "producer",
        "title",
        "version_producer",
        "date_published",
        "date_accessed",
        "url_main",
        "url_download",
        "attribution",
        "citation_full",
        "description",
    )
    for origin in origins:
        out += [f"### `{origin['file']}`", ""]
        out += [f"- {key}: {block(origin[key], 600)}" for key in fields if origin.get(key)]
        license_ = origin.get("license")
        if isinstance(license_, dict):
            out += [f"- license: {license_.get('name')} ({license_.get('url') or 'no url'})"]
        if origin.get("note"):
            out += [f"- note: {origin['note']}"]
        out += [""]
    if not origins:
        out += ["No snapshot `.dvc` found.", ""]
    return out


def section_docs(docs, dossier: Path) -> list[str]:
    out = ["## Producer documentation", ""]
    if not ok(docs):
        return out + [f"FAILED to fetch ({docs}): fetch the links by hand.", ""]
    if not docs:
        return out + ["No producer links found in the `.dvc`, the metadata or the step code.", ""]
    out += [f"Saved as plain text in `{rel(dossier / 'docs')}`: Grep and quote from these files.", ""]
    out += ["| file | role | status | via | fetched | chars | link |", "|---|---|---|---|---|---|---|"]
    capped = [doc for doc in docs if doc.status == "not-fetched"]
    docs = [doc for doc in docs if doc.status != "not-fetched"]
    for doc in sorted(docs, key=lambda d: (d.status != "ok", d.role, d.url)):
        name = f"`{doc.file}`" if doc.file else "—"
        out += [
            f"| {name} | {doc.role} | {doc.status} | {doc.via or '—'} | {doc.fetched_at or '—'} | {doc.chars:,} | {doc.url} |"
        ]
    problems = [doc for doc in docs if doc.status in ("unreachable", "thin") or doc.via == "wayback"]
    if problems:
        out += ["", "To handle:"]
        out += [f"- {doc.url}: {doc.note}" for doc in problems]
    if capped:
        out += [
            "",
            f"NOT FETCHED: {len(capped)} more links found in the metadata and code, past the cap of {MAX_TEXT_LINKS} "
            f"(listed in `{rel(dossier / 'docs' / 'index.json')}`). Fetch one when a claim under review cites it.",
        ]
    out += [
        "",
        "Check each document is the edition that matches the data, and look for the sources this table lacks "
        "(methodology, codebook, release notes) before judging any text.",
        "",
    ]
    return out


def section_freshness(results) -> list[str]:
    out = ["## Source files, checked live", ""]
    if not ok(results):
        return out + [f"FAILED ({results}): compare the source's file dates and hashes by hand.", ""]
    for result in results:
        out += [
            f"- `{result['file']}` (accessed {result['date_accessed'] or '?'}): **{result['status']}**. {result['detail']}"
        ]
    return out + [""] if results else out + ["No snapshot to check.", ""]


def section_corrections(files: list[Path], columns: set[str] | None = None) -> list[str]:
    """Known source errors already handled. For a chart, only the entries on its indicators (`columns`)."""
    out = ["## Corrections already in place", ""]
    if not files:
        return out + ["No `.corrections.yml`.", ""]
    for file in files:
        loaded = yaml.safe_load(file.read_text()) or []
        entries = loaded if isinstance(loaded, list) else loaded.get("corrections") or [loaded]
        entries = [e for e in entries if isinstance(e, dict)]
        shown = [e for e in entries if columns is None or str(e.get("indicator")) in columns]
        out += [
            f"- `{rel(file)}`: {len(entries)} known source errors, already handled"
            + (f"; {len(shown)} on this chart's indicators" if columns is not None else "")
            + ". Acknowledge them; never re-flag them."
        ]
        for entry in shown[:25]:
            years = entry.get("years") or entry.get("year") or ""
            out += [
                f"  - `{entry.get('indicator', '?')}`, {entry.get('entity', 'all entities')}, {years}: "
                f"{entry.get('action', '?')} ({entry.get('status', 'no status')}). {block(entry.get('reason', ''), 220)}"
            ]
        if len(shown) > 25:
            out += [f"  - … and {len(shown) - 25} more in the file"]
    return out + [""]


def section_metadata(result, dossier: Path, files: dict, namespace: str, version: str, short_name: str) -> list[str]:
    out = ["## Metadata as readers see it", ""]
    if not ok(result):
        return out + [f"FAILED to read ({result}): read the `.meta.yml` files by hand.", ""]
    source, indicators = result
    lines = [f"# Rendered metadata: {namespace}/{version}/{short_name}", "", f"From {source}.", ""]
    for item in indicators:
        lines += [f"## `{item['identity']}`", ""]
        lines += [f"**{field}**: {item[field]}\n" for field in META_FIELDS if item.get(field)]
    (dossier / "metadata.md").write_text("\n".join(lines))
    out += [f"- {len(indicators)} indicators, from {source}: `{rel(dossier / 'metadata.md')}`"]
    out += [f"- source files: {', '.join(f'`{rel(f)}`' for f in files['meta']) or 'none found'}"]
    diff = metadata_changes(files["meta"], namespace, version)
    (dossier / "metadata_changes.diff").write_text(diff["text"])
    out += [f"- changed text: {diff['summary']} (`{rel(dossier / 'metadata_changes.diff')}`)", ""]
    return out


def metadata_changes(meta_files: list[Path], namespace: str, version: str) -> dict:
    """What changed in the metadata files: against master for an edited file, against the previous version otherwise."""
    chunks, summary = [], []
    for file in meta_files:
        relative = rel(file)
        on_master = subprocess.run(
            ["git", "cat-file", "-e", f"origin/master:{relative}"], cwd=paths.BASE_DIR, capture_output=True
        )
        if on_master.returncode == 0:
            diff = subprocess.run(
                ["git", "diff", "origin/master", "--", relative], cwd=paths.BASE_DIR, capture_output=True, text=True
            ).stdout
            base = "origin/master"
        else:
            older = earlier_versions(file, version)
            if not older:
                summary.append(f"`{file.name}` is new (no earlier version on disk): all of its text is in scope")
                continue
            diff = "".join(
                difflib.unified_diff(
                    older[-1][1].read_text().splitlines(keepends=True),
                    file.read_text().splitlines(keepends=True),
                    rel(older[-1][1]),
                    relative,
                    n=2,
                )
            )
            base = f"version {older[-1][0]}"
        changed = sum(1 for line in diff.splitlines() if line[:1] in "+-" and line[:3] not in ("+++", "---"))
        summary.append(f"`{file.name}`: {changed} changed lines against {base}")
        chunks.append(diff)
    return {"text": "\n".join(chunks), "summary": "; ".join(summary) or "no metadata files"}


def earlier_versions(file: Path, version: str) -> list[tuple[str, Path]]:
    """The same file in the earlier versions of its step, as (version, path), oldest first.

    The version is a directory of the path, wherever the file sits below it: `<ns>/<version>/<short>.meta.yml` for
    a script step, `<ns>/<version>/<short>/<short>.meta.yml` for a package step.
    """
    version_dir = next((p for p in file.parents if p.name == version), None)
    if version_dir is None:
        return []
    below = file.relative_to(version_dir)
    found = ((p.relative_to(version_dir.parent).parts[0], p) for p in version_dir.parent.glob(f"*/{below}"))
    return sorted((name, p) for name, p in found if name < version)


def section_selection(rows: list[dict], note: str, dossier: Path) -> list[str]:
    selected = [row for row in rows if row["selected"]]
    out = [f"## Indicators for the deep review ({len(selected)} of {len(rows)})", "", note, ""]
    out += ["| rank | indicator | charts | views (365 d) | anomaly flags | why |", "|---|---|---|---|---|---|"]
    out += [
        f"| {r['rank']} | `{r['identity']}` | {r['n_charts']} | {r['views_365d']:,} | {r['anomaly_flags']} | {r['why']} |"
        for r in selected[:40]
    ]
    if len(selected) > 40:
        out += [f"| … | {len(selected) - 40} more in `ranking.csv` | | | | |"]
    skipped = len(rows) - len(selected)
    out += [
        "",
        f"SKIPPED: {skipped} indicators, each with its rank and reason in `{rel(dossier / 'ranking.csv')}`. "
        "The report lists them all; `assemble_report.py` copies them in.",
        "",
    ]
    return out


def section_anomalies(findings: list[dict], notes: list[str], dossier: Path, selected: set[str]) -> list[str]:
    out = [f"## Anomaly candidates ({len(findings)})", ""]
    out += [f"- {note}" for note in notes]
    if findings:
        counts = pd.Series([f["kind"] for f in findings]).value_counts()
        out += ["- by kind: " + ", ".join(f"{kind} {count}" for kind, count in counts.items())]
        out += [f"- all of them: `{rel(dossier / 'anomalies.csv')}`", ""]
        # Shown first: the selected indicators, whole-column flags before single values, newest period first.
        order = {kind: position for position, kind in enumerate(ANOMALY_ORDER)}
        shown = sorted(
            findings,
            key=lambda f: (
                not any(identity_matches(i, f"{f['table']}#{f['column']}") for i in selected),
                order.get(f["kind"], 99),
                -_as_number(f.get("period")),
            ),
        )[:40]
        out += [f"- {f['kind']} `{f['table']}.{f['column']}`: {f['detail']}" for f in shown]
        if len(findings) > len(shown):
            out += [f"- … and {len(findings) - len(shown):,} more in the CSV (filter it by column or kind)"]
    return out + [""]


def _years(data: pd.DataFrame) -> pd.Series | None:
    """The year of each row of a chart's data: its year column, or the year of its day column."""
    for column in ("Year", "year"):
        if column in data.columns:
            return pd.to_numeric(data[column], errors="coerce")
    for column in ("Day", "date"):
        if column in data.columns:
            return pd.to_datetime(data[column], errors="coerce").dt.year
    return None


def _period(value) -> str:
    """A year without its trailing `.0`; a day (`2020-01-01`) as it is."""
    return f"{value:g}" if pd.api.types.is_number(value) else str(value)


def identity_matches(identity: str, key: str) -> bool:
    """Whether an indicator (`table#column`, a grapher name when built) is the garden column a flag names.

    The scan reads the garden build, where dimensions are columns; the grapher build flattens them into the name
    (`t#rate__sex_female` is garden `t#rate` at `sex=female`), so a garden column's flags belong to all its slices.
    """
    table, _, column = identity.partition("#")
    key_table, _, key_column = key.partition("#")
    return table == key_table and anomaly_scan.column_matches(column, key_column)


def flags_for(identity: str, flags: dict[str, int]) -> int:
    """The anomaly flags on an indicator: those on its own column, or on the garden column it is a slice of."""
    return sum(count for key, count in flags.items() if identity_matches(identity, key))


def _as_number(value) -> float:
    """A period as a number for sorting (a year, or a date's year); 0 when it is neither."""
    try:
        return float(str(value)[:4])
    except ValueError:
        return 0.0


def section_dods(result, dossier: Path) -> list[str]:
    out = ["## Details on Demand", ""]
    if not ok(result):
        return out + [f"FAILED ({result}): run `scripts/collect_dods.py` by hand.", ""]
    (dossier / "dods.md").write_text(result)
    names = re.findall(r"^## (\S+) \(last edited", result, re.MULTILINE)
    if not names:
        return out + ["None linked from the dataset's metadata, charts or articles.", ""]
    return out + [
        f"{len(names)} linked: {', '.join(f'`{n}`' for n in names)}. Text and where each appears: `{rel(dossier / 'dods.md')}`",
        "",
    ]


def section_earlier(short_name: str, dossier: Path) -> list[str]:
    reports = sorted((paths.BASE_DIR / "ai").glob(f"adversarial-review-*{short_name}*.md"))
    out = ["## Earlier reviews", ""]
    if not reports:
        return out + ["None on disk.", ""]
    out += [f"- `{rel(report)}`" for report in reports[-5:]]
    out += [
        "",
        "Reuse what an earlier review established about the source; re-check what the data or the text changed.",
        "",
    ]
    return out


def section_next(
    context: str,
    dossier: Path,
    namespace: str,
    version: str,
    short_name: str,
    selected: list[dict],
    origins: list[dict],
) -> list[str]:
    reference = ".claude/skills/fact-check-dataset/reference/dataset-review.md"
    out = ["## Next", ""]
    if context not in FULL_CONTEXTS:
        return out + [f"One agent, no lanes. Follow `{reference}` → *Claims lane*, limited to: {SCOPE[context]}.", ""]
    write_lane_briefs(context, dossier, namespace, version, short_name, selected, origins)
    report = f"ai/adversarial-review-{short_name}-{dt.date.today().isoformat()}.md"
    out += [
        "Launch both lanes as background agents **in one message**, each with only this prompt: "
        "`Read <brief> and do what it says.`",
        "",
        f"- claims lane: `{rel(dossier / 'lane_claims.md')}`",
        f"- values lane: `{rel(dossier / 'lane_values.md')}`",
        "",
        "When both are back, read each result's *Notes for the other lane*, settle any contradiction, then:",
        "",
        "```bash",
        f".venv/bin/python .claude/skills/fact-check-dataset/scripts/assemble_report.py {rel(dossier)} --reconciled",
        "```",
        "",
        f"The report lands in `{report}`.",
        "",
    ]
    return out


def write_lane_briefs(
    context: str,
    dossier: Path,
    namespace: str,
    version: str,
    short_name: str,
    selected: list[dict],
    origins: list[dict],
) -> None:
    reference = ".claude/skills/fact-check-dataset/reference/dataset-review.md"
    step = f"{namespace}/{version}/{short_name}"
    producers = ", ".join(sorted({str(o.get("producer")) for o in origins if o.get("producer")})) or "the producer"
    indicators = (
        "\n".join(f"- `{row['identity']}` ({row['why']})" for row in selected) or "- none selected: see the dossier"
    )
    common = f"""Read `.claude/skills/fact-check-dataset/SKILL.md` (the rules) and the *{{lane}}* section of
`{reference}` (the procedure), then the dossier `{rel(dossier / "dossier.md")}`. Everything mechanical is already
in the dossier directory `{rel(dossier)}`: do not rediscover it, and do not re-read a file the dossier quotes.

Context: **{context}**. Scope: {SCOPE[context]}.
Producer: {producers}.

Indicators for the deep review:
{indicators}

Read-only: do not edit repo files, commit or post anywhere. Write scratch files in the dossier directory.
"""
    claims = f"""# Claims lane: `{step}`

{common.format(lane="Claims lane")}
Your job: Part 0 (what the source publishes, what OWID adds, field placement) and the adversarial review of the
metadata text and the DoDs against the saved producer documentation.

Write your result to `{rel(dossier / "lane_claims.result.md")}` with exactly these headings:
`## Part 0 — Source verification`, `## Findings`, `## Notes for the other lane`.
Hand back three lines: the verdict, the count of 🔴/🟡/🟢, and the result's path.
"""
    values = f"""# Values lane: `{step}`

{common.format(lane="Values lane")}
Your job: triage the anomaly candidates (`anomalies.csv`), check the accounting identities, then the independent
online cross-check of the surviving anomalies and the anchors, and the snapshot-level attribution of any bad value.

Write your result to `{rel(dossier / "lane_values.result.md")}` with exactly these headings:
`## Findings`, `## Values checked`, `## Anomalies not checked`, `## Notes for the other lane`.
Hand back three lines: the verdict, the count of 🔴/🟡/🟢, and the result's path.
"""
    (dossier / "lane_claims.md").write_text(claims)
    (dossier / "lane_values.md").write_text(values)


# ----------------------------------------------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------------------------------------------


def load_skill_script(relative: str):
    """Import a script of another skill by path (skill scripts are not a package)."""
    path = HERE.parents[1] / relative
    name = path.stem
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Failed(str):
    """Why a job failed. Its section of the dossier says so, and the others are still written."""


def outcome(job: Future):
    """A job's result, or a `Failed` with the error: one failed section never loses the others."""
    try:
        return job.result()
    except Exception as error:
        return Failed(f"{type(error).__name__}: {str(error)[:200]}")


def outcome_of(function, *args):
    """The same, for a call made in the main thread."""
    try:
        return function(*args)
    except Exception as error:
        return Failed(f"{type(error).__name__}: {str(error)[:200]}")


def ok(result) -> bool:
    return result is not None and not isinstance(result, Failed)


def rel(path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(paths.BASE_DIR))
    except ValueError:
        return str(path)


def block(value, limit: int = 2000) -> str:
    """A value on one line, cut at `limit` characters with the cut said."""
    text = " ".join(str(value).split())
    return text if len(text) <= limit else f"{text[:limit]} … [cut: {len(text):,} characters in full]"


def split_list(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


if __name__ == "__main__":
    main()
