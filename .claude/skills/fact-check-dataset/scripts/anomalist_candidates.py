"""Turn Anomalist's stored results into candidates for a targeted fact-check pass.

owidbot runs Anomalist on every push to a branch and stores what it finds in the branch's staging DB. Anomalist
sees what the dossier's own scan (`anomaly_scan.py`, on the local garden build) cannot: the old-vs-new comparison
(`upgrade_change`, `upgrade_missing`) and outliers against a Gaussian-process fit (`gp_outlier`). This script
reads those stored results; it detects nothing itself and ranks them exactly as the Wizard page does, by reusing
`apps/anomalist/view.py`.

Anomalist adds candidates. It never replaces the scan, the anchors or the online cross-checks of the first pass,
and a dataset it says little about is not a clean dataset: the coverage section says what it left out.

    .venv/bin/python .claude/skills/fact-check-dataset/scripts/anomalist_candidates.py \
        <namespace>/<version>/<short_name> --staging <branch> [--top 30] [--min-score 0.3]

It writes, into the dataset's dossier `ai/fact-check/<namespace>/<version>/<short_name>/`:
    anomalist.csv        every stored candidate above --min-score: values, first-pass status, selected or why not
    anomalist.md         what it prints: coverage, the selected candidates, what to run next
    lane_anomalist.md    the brief for the one agent that checks the selected candidates

Each run starts over: an earlier run's result answers another candidate set, so it is moved to
`lane_anomalist.previous-result.md`, which the report ignores.
"""

import argparse
import csv
import os
import re
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import anomaly_scan  # noqa: E402

BRIEF = "lane_anomalist.md"
RESULT = "lane_anomalist.result.md"
PREVIOUS_RESULT = "lane_anomalist.previous-result.md"
# MySQL's "table doesn't exist": the indicator upgrader has never run on this DB.
NO_SUCH_TABLE = 1146
TYPES = ("upgrade_change", "upgrade_missing", "time_change", "gp_outlier")
UPGRADE_TYPES = ("upgrade_change", "upgrade_missing")
# Candidates already looked at by the first pass, or still to look at.
NEW = "new"
CHECKED = "checked in the first pass"
FLAGGED = "flagged by the first pass's scan"
COLUMNS = [
    "rank",
    "selected",
    "why",
    "status",
    "type",
    "indicator_id",
    "identity",
    "indicator_name",
    "unit",
    "entity",
    "year",
    "anomaly",
    "relevance",
    "value",
    "previous",
    "next",
    "old_value",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", help="<namespace>/<version>/<short_name> of the new version")
    parser.add_argument("--staging", required=True, metavar="BRANCH", help="the branch whose staging DB to read")
    parser.add_argument("--top", type=int, default=30, help="new candidates to select for the agent")
    parser.add_argument("--min-score", type=float, default=0.3, help="drop candidates below this relevance (0-1)")
    args = parser.parse_args()

    # The Anomalist helpers read through the global engine, so point it at the branch before importing them.
    os.environ["STAGING"] = args.staging
    from etl import config

    if config.STAGING != args.staging:
        sys.exit(f"Could not target staging-site-{args.staging} (STAGING is ignored when ENV_FILE is set).")
    print(run(args.step, args.top, args.min_score))


def run(step: str, top: int, min_score: float) -> str:
    from etl import paths
    from etl.db import get_engine

    namespace, version, short_name = parse_step(step)
    dossier = paths.BASE_DIR / "ai" / "fact-check" / namespace / version / short_name
    dossier.mkdir(parents=True, exist_ok=True)
    # A run regenerates the candidates, so an earlier run's brief and result answer another set: set them aside.
    set_aside = clear_previous_run(dossier)
    engine = get_engine()

    dataset = dataset_row(engine, f"{namespace}/{version}/{short_name}")
    if dataset is None:
        text = (
            f"# Anomalist candidates: `{step}`\n\nThe dataset is not in this branch's staging DB yet. Wait for the "
            f"staging build of your latest push (or upload it: `STAGING=<branch> .venv/bin/etlr "
            f"grapher://grapher/{namespace}/{version}/{short_name} --grapher`) and run this again.\n"
        )
        text += "".join(f"\n- {note}\n" for note in set_aside)
        (dossier / "anomalist.md").write_text(text)
        return text

    stored, indicators, notes = load_scores(engine, int(dataset["id"]), min_score)
    first_pass = first_pass_keys(dossier)
    rows = candidates(stored, indicators, first_pass, top)
    add_values(rows, paths.DATA_DIR, f"{namespace}/{version}/{short_name}", old_dataset(engine, dataset))
    write_csv(rows, dossier / "anomalist.csv")
    coverage = coverage_notes(stored, indicators, dataset, notes) + set_aside
    rel = str(dossier.relative_to(paths.BASE_DIR))
    text = render(step, rel, rows, coverage, first_pass, min_score)
    (dossier / "anomalist.md").write_text(text)
    # The brief exists only when there is something to check: `assemble_report.py` waits for the result of a brief.
    if any(r["selected"] for r in rows):
        (dossier / BRIEF).write_text(brief(step, rel, rows))
    return text


def clear_previous_run(dossier: Path) -> list[str]:
    """Remove an earlier run's candidates and brief, and move its result aside so `assemble_report.py` ignores it."""
    notes = []
    result = dossier / RESULT
    if result.exists():
        result.replace(dossier / PREVIOUS_RESULT)
        notes.append(
            f"An earlier run's `{RESULT}` answered another candidate set: it was moved to `{PREVIOUS_RESULT}`, "
            "and the report ignores it."
        )
    for name in (BRIEF, "anomalist.csv"):
        (dossier / name).unlink(missing_ok=True)
    return notes


# ----------------------------------------------------------------------------------------------------------------
# Reading the stored anomalies
# ----------------------------------------------------------------------------------------------------------------


def parse_step(text: str) -> tuple[str, str, str]:
    parts = re.sub(r"^[a-z-]+://", "", text.strip()).strip("/").split("/")
    if parts[0] in ("meadow", "garden", "grapher"):
        parts = parts[1:]
    if len(parts) != 3:
        sys.exit(f"Cannot read `{text}` as <namespace>/<version>/<short_name>.")
    return parts[0], parts[1], parts[2]


def dataset_row(engine, catalog_path: str) -> dict | None:
    from etl.db import read_sql

    df = read_sql(
        "SELECT id, name, namespace, shortName, version, sourceChecksum FROM datasets WHERE catalogPath = %(p)s",
        engine,
        params={"p": catalog_path},
    )
    return None if df.empty else df.iloc[0].to_dict()


def old_dataset(engine, dataset: dict) -> tuple[str | None, dict[int, str], str]:
    """The dataset this one replaces (`<ns>/<version>/<short_name>`), the old identity of each new indicator, and
    why there is none when the first is None.

    The indicator upgrader's mapping (`wiz__variable_mapping`) says it when it exists, also when the short name
    changed; otherwise it is the latest older version with the same short name, paired by identity. Only a missing
    mapping table falls back to that: any other failure to read the mapping leaves the old values unavailable,
    because the same-named fallback can be the wrong dataset after a rename or a remap.
    """
    from sqlalchemy.exc import DBAPIError

    from etl.db import read_sql

    try:
        mapping = read_sql(
            "SELECT m.id_new, v.catalogPath AS old_path, d.catalogPath AS old_dataset FROM wiz__variable_mapping m "
            "JOIN variables v ON v.id = m.id_old JOIN datasets d ON d.id = m.dataset_id_old "
            "WHERE m.dataset_id_new = %(d)s ORDER BY m.timestamp DESC",
            engine,
            params={"d": int(dataset["id"])},
        )
    except DBAPIError as error:
        if not is_missing_table(error):
            reason = f"unavailable: the variable mapping could not be read ({type(error.orig).__name__}: {error.orig})"
            return None, {}, reason[:240]
        mapping = pd.DataFrame()
    if not mapping.empty:
        olds = {int(r.id_new): identity_of(r.old_path, "") for r in mapping.drop_duplicates("id_new").itertuples()}
        return str(mapping["old_dataset"].iloc[0]), olds, ""
    df = read_sql(
        "SELECT catalogPath FROM datasets WHERE namespace = %(n)s AND shortName = %(s)s AND version < %(v)s "
        "ORDER BY version DESC LIMIT 1",
        engine,
        params={"n": dataset["namespace"], "s": dataset["shortName"], "v": dataset["version"]},
    )
    if df.empty:
        return None, {}, "no older version in the DB"
    return str(df["catalogPath"].iloc[0]), {}, ""


def is_missing_table(error: Exception) -> bool:
    """Whether a DB error is MySQL's "table doesn't exist"."""
    args = getattr(getattr(error, "orig", None), "args", None) or ()
    return bool(args) and args[0] == NO_SUCH_TABLE


def load_scores(engine, dataset_id: int, min_score: float) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Every stored anomaly of the dataset, scored as the Wizard scores them, with its indicator's names."""
    from apps.anomalist import view
    from etl.db import read_sql

    anomalies = view.load_anomalies(engine, [dataset_id])
    indicators = view.load_indicators(engine, [dataset_id])
    catalog_paths = read_sql(
        "SELECT id AS indicator_id, catalogPath FROM variables WHERE datasetId = %(d)s",
        engine,
        params={"d": dataset_id},
    )
    indicators = indicators.merge(catalog_paths, on="indicator_id", how="left")
    with_rows = [a for a in anomalies if a.dfReduced is not None and not a.dfReduced.empty]
    notes: dict = {
        "types": sorted({a.anomalyType for a in anomalies}),
        "checksums": {a.anomalyType: a.datasetSourceChecksum for a in anomalies},
        "relevance": True,
    }
    if not with_rows:
        return pd.DataFrame(), indicators, notes
    try:
        df = view.build_scores(with_rows, relevance=True)
    except Exception as error:
        # The relevance weights need the population dataset and the analytics table; rank without them.
        notes["relevance"] = f"{type(error).__name__}: {str(error)[:160]}"
        df = view.build_scores(with_rows, relevance=False)
    df = df[df["score_weighted"] >= min_score]
    return view.build_table(df, indicators), indicators, notes


# ----------------------------------------------------------------------------------------------------------------
# Candidates
# ----------------------------------------------------------------------------------------------------------------


def identity_of(catalog_path: str | None, fallback: str) -> str:
    """`<table>#<column>` from a variable's catalog path (`grapher/<ns>/<v>/<short>/<table>#<column>`)."""
    if not catalog_path or "#" not in str(catalog_path):
        return fallback
    table, _, column = str(catalog_path).rpartition("/")[2].partition("#")
    return f"{table}#{column}"


def first_pass_keys(dossier: Path) -> dict:
    """What the first pass covered: (indicator, entity, year) it checked, (column, detail, period) its scan flagged."""
    checked: set[tuple[str, str, str]] = set()
    result = dossier / "lane_values.result.md"
    if result.exists():
        for line in table_section(result.read_text(), "Values checked"):
            cells = [c.strip().strip("`") for c in line.strip().strip("|").split("|")]
            if len(cells) >= 3:
                checked.add((cells[0].lower(), cells[1].lower(), cells[2].lower()))
    flagged: set[tuple[str, str, str]] = set()
    scan = dossier / "anomalies.csv"
    if scan.exists():
        with scan.open() as handle:
            for row in csv.DictReader(handle):
                key = f"{row.get('table', '')}#{row.get('column', '')}"
                flagged.add((key, row.get("detail", ""), row.get("period") or ""))
    return {"checked": checked, "flagged": flagged, "has_result": result.exists(), "has_scan": scan.exists()}


def table_section(text: str, heading: str) -> list[str]:
    """The table rows (header and rule excluded) under a `## <heading>` section."""
    rows, inside = [], False
    for line in text.splitlines():
        if line.startswith("## "):
            inside = line[3:].strip().lower().startswith(heading.lower())
            continue
        if inside and line.strip().startswith("|") and not re.match(r"^\|[\s:|-]+\|?$", line.strip()):
            rows.append(line)
    return rows[1:] if rows else rows


def status_of(identity: str, entity: str, year: str, first_pass: dict) -> str:
    """Whether the first pass already checked this value, or its scan flagged it, or neither.

    A checked row names its indicator loosely (a column, a title), so it matches when it names this column; a scan
    flag names the garden column, which a grapher name extends (`rate__sex_female` is garden `rate`).
    """
    table, _, column = identity.partition("#")
    for indicator, checked_entity, checked_year in first_pass["checked"]:
        names_it = bool(indicator) and (column.lower() in indicator or indicator in (column.lower(), identity.lower()))
        if names_it and checked_entity == entity.lower() and checked_year in (str(year).lower(), ""):
            return CHECKED
    for key, detail, period in first_pass["flagged"]:
        key_table, _, key_column = key.partition("#")
        if key_table == table and anomaly_scan.column_matches(column, key_column) and entity and entity in detail:
            if not period or str(period).removesuffix(".0") == str(year):
                return FLAGGED
    return NEW


def candidates(stored: pd.DataFrame, indicators: pd.DataFrame, first_pass: dict, top: int) -> list[dict]:
    """One row per stored anomaly, most relevant first; the top `top` new ones are selected, the rest say why not."""
    if stored.empty:
        return []
    paths_by_id = {}
    if "catalogPath" in indicators.columns:
        paths_by_id = dict(zip(indicators["indicator_id"], indicators["catalogPath"]))
    rows = []
    for record in stored.sort_values("score_weighted", ascending=False).to_dict("records"):
        identity = identity_of(paths_by_id.get(record["indicator_id"]), str(record.get("indicator", "")))
        year = "" if pd.isna(record.get("year")) else str(int(record["year"]))
        entity = str(record.get("entity_name", ""))
        rows.append(
            {
                "type": record.get("type", ""),
                "indicator_id": int(record["indicator_id"]),
                "identity": identity,
                "indicator_name": record.get("indicator_name", ""),
                "unit": record.get("unit", ""),
                "entity": entity,
                "year": year,
                "anomaly": record.get("anomaly"),
                "relevance": record.get("relevance"),
                "status": status_of(identity, entity, year, first_pass),
            }
        )
    selected = 0
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
        if row["status"] != NEW:
            row["selected"], row["why"] = False, row["status"]
        elif selected < top:
            row["selected"], row["why"] = True, "new candidate"
            selected += 1
        else:
            row["selected"], row["why"] = False, f"past the cap of {top} new candidates (raise --top)"
    return rows


def add_values(rows: list[dict], data_dir: Path, new: str, old: tuple[str | None, dict[int, str], str]) -> None:
    """The flagged value, its neighbors and the old version's value, from the grapher datasets on disk."""
    old_path, old_identities, no_old = old
    new_dir = data_dir / "grapher" / new
    old_dir = data_dir / "grapher" / old_path if old_path else None
    cache: dict = {}
    for row in rows:
        if not row["selected"]:
            continue
        table, _, column = row["identity"].partition("#")
        year = int(row["year"]) if row["year"] else None
        row["value"], row["previous"], row["next"] = neighbors(
            read_series(cache, new_dir, table, column, row["entity"]), year
        )
        if old_dir is None:
            row["old_value"] = no_old
            continue
        old_table, _, old_column = old_identities.get(row["indicator_id"], row["identity"]).partition("#")
        old_series = read_series(cache, old_dir, old_table, old_column, row["entity"])
        row["old_value"] = f"{old_path} not on disk" if old_series is None else value_at(old_series, year)


def read_series(cache: dict, directory: Path | None, table: str, column: str, entity: str) -> pd.Series | None:
    """One entity's series of a grapher column, indexed by year; None when the dataset or column is missing."""
    if directory is None or not (directory / "index.json").exists():
        return None
    key = (directory, table)
    if key not in cache:
        try:
            from owid.catalog import Dataset

            cache[key] = Dataset(directory).read(table, safe_types=False).reset_index()
        except Exception:
            cache[key] = None
    tb = cache[key]
    if tb is None or column not in tb.columns or "year" not in tb.columns:
        return None
    entity_column = "country" if "country" in tb.columns else "entity"
    rows = tb[tb[entity_column].astype(str) == entity]
    return rows.set_index("year")[column].dropna().sort_index()


def neighbors(series: pd.Series | None, year: int | None) -> tuple:
    if series is None:
        return "not on disk (build the grapher step)", "", ""
    if year is None:
        return "", "", ""
    before, after = series[series.index < year], series[series.index > year]
    previous = f"{fmt(before.iloc[-1])} ({before.index[-1]})" if len(before) else ""
    following = f"{fmt(after.iloc[0])} ({after.index[0]})" if len(after) else ""
    return value_at(series, year), previous, following


def value_at(series: pd.Series, year: int | None) -> str:
    if year is None or year not in series.index:
        return "missing"
    return fmt(series.loc[year])


def fmt(value) -> str:
    """A value with up to 6 significant digits: enough to compare with a producer's table, short enough to read."""
    return f"{value:.6g}" if isinstance(value, (int, float)) and not isinstance(value, bool) else str(value)


# ----------------------------------------------------------------------------------------------------------------
# Coverage: what Anomalist did not look at
# ----------------------------------------------------------------------------------------------------------------


def coverage_notes(stored: pd.DataFrame, indicators: pd.DataFrame, dataset: dict, notes: dict) -> list[str]:
    out = []
    flagged = stored["indicator_id"].nunique() if not stored.empty else 0
    out.append(
        f"{flagged:,} of {len(indicators):,} indicators have a candidate above the cut. An indicator without one "
        "may be clean, or may never have been checked (Anomalist samples large datasets and gives `gp_outlier` a "
        "time budget): stored rows cannot tell the two apart."
    )
    missing = [t for t in TYPES if t not in notes["types"]]
    if missing:
        out.append(f"No results stored for: {', '.join(missing)}.")
    if any(t in missing for t in UPGRADE_TYPES):
        out.append(
            "The old-vs-new detectors are missing: they run only with a variable mapping and the old grapher "
            "dataset on disk. See `/update-dataset` step 7 for the full-mapping re-run."
        )
    stale = [t for t, checksum in notes["checksums"].items() if checksum != dataset["sourceChecksum"]]
    if stale:
        out.append(
            f"Stale: {', '.join(stale)} ran on an earlier build of the dataset (checksum differs). Wait for the "
            "staging build of your latest push, or re-run `etl anomalist` with `--force`."
        )
    if notes["relevance"] is not True:
        out.append(f"Ranked by the raw anomaly score: the relevance weights failed ({notes['relevance']}).")
    out.append(
        "Anomalist skips tables keyed by date instead of year, and keeps only the worst year per indicator "
        "and entity: other years of a flagged series may be off too."
    )
    return out


# ----------------------------------------------------------------------------------------------------------------
# Output
# ----------------------------------------------------------------------------------------------------------------


def write_csv(rows: list[dict], path: Path) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def render(step: str, rel: str, rows: list[dict], coverage: list[str], first_pass: dict, min_score: float) -> str:
    selected = [r for r in rows if r["selected"]]
    counts = pd.Series([r["why"] for r in rows]).value_counts().to_dict() if rows else {}
    out = [f"# Anomalist candidates: `{step}`", ""]
    out += [f"- candidates at relevance ≥ {min_score}: {len(rows):,}"]
    out += [f"  - {why}: {n:,}" for why, n in counts.items()]
    if not first_pass["has_result"]:
        out += ["- the first pass's `lane_values.result.md` is not in the dossier: nothing is marked as checked"]
    if not first_pass["has_scan"]:
        out += ["- the first pass's `anomalies.csv` is not in the dossier: nothing is marked as flagged by the scan"]
    out += [f"- all of them: `{rel}/anomalist.csv`", "", "## Coverage", ""]
    out += [f"- {note}" for note in coverage]
    out += ["", f"## Selected ({len(selected)})", ""]
    if selected:
        out += ["| # | type | indicator | entity | year | value | previous | next | old version | relevance |"]
        out += ["|---|---|---|---|---|---|---|---|---|---|"]
        out += [
            f"| {r['rank']} | {r['type']} | `{r['identity']}` | {r['entity']} | {r['year']} | {r.get('value', '')} | "
            f"{r.get('previous', '')} | {r.get('next', '')} | {r.get('old_value', '')} | {r['relevance']} |"
            for r in selected
        ]
    out += ["", "## Next", ""]
    if selected:
        out += [
            f"Start one background agent with the prompt `Read {rel}/lane_anomalist.md and do what it says.` When it "
            f"is back, run `.venv/bin/python .claude/skills/fact-check-dataset/scripts/assemble_report.py {rel} "
            "--reconciled`, which folds its result into the report."
        ]
    else:
        out += ["No new candidate: no agent to start. Say so in the PR body, with the coverage above."]
    return "\n".join(out) + "\n"


def brief(step: str, rel: str, rows: list[dict]) -> str:
    selected = sum(1 for r in rows if r["selected"])
    return f"""# Anomalist lane: `{step}`

Read `.claude/skills/fact-check-dataset/SKILL.md` (the rules), then the *Anomalist candidates* and *Values lane*
sections of `.claude/skills/fact-check-dataset/reference/dataset-review.md` (the procedure), then
`{rel}/anomalist.md` and the dossier `{rel}/dossier.md`. The first pass's results are in the same directory: do
not redo what they established.

Your job: the {selected} candidates with `selected` = True in `{rel}/anomalist.csv`, and nothing else. Triage each
one (a stated reason for every discard), compare the survivors with the producer's own figures and the two raw
snapshots, cross-check what is left against independent sources, and attribute every bad value to the source or to
our processing.

Read-only: do not edit repo files, commit or post anywhere. Write scratch files in the dossier directory.

Write your result to `{rel}/lane_anomalist.result.md` with exactly these headings:
`## Findings`, `## Values checked`, `## Anomalies not checked`, `## Notes for the other lane`.
Tag each finding and each row of *Values checked* with its Anomalist type (`anomalist:upgrade_change`, ...).
Hand back three lines: the verdict, the count of 🔴/🟡/🟢, and the result's path.
"""


if __name__ == "__main__":
    main()
