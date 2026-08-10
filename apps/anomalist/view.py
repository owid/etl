"""Render stored anomalies as compact text.

`etl anomalist` detects anomalies and stores them in the grapher `anomalies` table: one row per
dataset and anomaly type, with the per-entity scores as a feather blob. The Wizard page renders
those for a human to click through, and its "AI Summary" button sends them to GPT.

This command prints the same rows as plain text instead, so that whoever reads it — an agent, or
anyone on a terminal — does the summarizing. It deliberately calls no model itself.

It only reads. Anomalies must already have been detected:

    STAGING=<branch> etl anomalist --dataset-ids <id>
"""

import json
from typing import Any, Literal, cast, get_args

import click
import numpy as np
import pandas as pd
import structlog
from rich_click.rich_command import RichCommand
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.anomalist.anomalist_api import ANOMALY_TYPE, add_auxiliary_scores, combine_and_reduce_scores_df
from etl.db import get_engine, read_sql
from etl.grapher import model as gm

log = structlog.get_logger()

GROUP_BY = Literal["indicator", "entity"]
OUTPUT_FORMAT = Literal["md", "csv", "json"]

# Columns printed for each anomaly, per grouping. The column the anomalies are grouped by is
# omitted here: it goes in the section headline instead of being repeated on every row, which is
# most of what makes the raw table expensive to read.
ROW_COLUMNS: dict[str, list[str]] = {
    "indicator": ["entity_name", "year", "type", "anomaly", "relevance"],
    "entity": ["indicator", "indicator_id", "year", "type", "anomaly", "relevance"],
}

# Columns for the flat (csv/json) output, which has no headlines to carry the group key.
FLAT_COLUMNS = ["entity_name", "year", "type", "indicator_id", "indicator", "anomaly", "relevance"]

PLURAL: dict[str, str] = {"indicator": "indicators", "entity": "entities"}


def row_columns(group_by: GROUP_BY, relevance: bool) -> list[str]:
    """Columns to print. Without the relevance weighting, `relevance` just repeats `anomaly`."""
    columns = ROW_COLUMNS[group_by]
    return columns if relevance else [column for column in columns if column != "relevance"]


def flat_columns(relevance: bool) -> list[str]:
    return FLAT_COLUMNS if relevance else [column for column in FLAT_COLUMNS if column != "relevance"]


def score_name(relevance: bool) -> str:
    return "relevance" if relevance else "anomaly"


def load_anomalies(engine: Engine, dataset_ids: list[int], anomaly_types: tuple[str, ...] = ()) -> list[gm.Anomaly]:
    """Load stored anomalies for the given datasets, optionally restricted to some anomaly types."""
    with Session(engine) as session:
        anomalies = gm.Anomaly.load_anomalies(session, dataset_ids)

    if anomaly_types:
        anomalies = [anomaly for anomaly in anomalies if anomaly.anomalyType in anomaly_types]

    return anomalies


def load_indicators(engine: Engine, dataset_ids: list[int]) -> pd.DataFrame:
    """Load the metadata that makes an indicator id readable (short name, title, unit)."""
    q = """
    SELECT id AS indicator_id, shortName AS indicator, name AS indicator_name, unit
    FROM variables
    WHERE datasetId IN %(dataset_ids)s
    """
    return read_sql(q, engine, params={"dataset_ids": dataset_ids})


def load_datasets(engine: Engine, dataset_ids: list[int]) -> pd.DataFrame:
    q = """
    SELECT id AS dataset_id, name AS dataset_name, catalogPath, sourceChecksum
    FROM datasets
    WHERE id IN %(dataset_ids)s
    """
    return read_sql(q, engine, params={"dataset_ids": dataset_ids})


def build_scores(anomalies: list[gm.Anomaly], relevance: bool = True) -> pd.DataFrame:
    """Combine the stored per-anomaly score frames into one flat table.

    With `relevance`, the anomaly score is combined with population, chart-views and scale scores
    into `score_weighted`, exactly as the Wizard page ranks them. That enrichment needs the
    population dataset and the analytics table, so `relevance=False` keeps the raw anomaly score
    only — faster, and it works without them.
    """
    df = combine_and_reduce_scores_df(anomalies)

    if relevance:
        return add_auxiliary_scores(df)

    # Mirror the renaming add_auxiliary_scores does, so downstream code is score-agnostic, and rank
    # by the raw anomaly score.
    df = df.rename(columns={"variable_id": "indicator_id", "anomaly_score": "score"}, errors="raise")
    df["score_weighted"] = df["score"]
    df["views"] = np.nan

    return df


def select_anomalies(
    df: pd.DataFrame,
    group_by: GROUP_BY = "indicator",
    top: int = 50,
    rows_per_group: int = 5,
    min_score: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rank and cap the anomalies to show.

    Groups are ranked by their single most relevant anomaly, not by how many they have: one
    badly-broken country matters more than a hundred marginal ones. Returns the selected rows, and
    the per-group stats for *all* groups above `min_score` so the caller can report what was cut.
    """
    key = group_key(group_by)

    df = cast(pd.DataFrame, df[df["score_weighted"] >= min_score])

    group_stats = (
        df.groupby(key)
        .agg(n_rows=("score_weighted", "size"), max_relevance=("score_weighted", "max"))
        .sort_values("max_relevance", ascending=False)
    )

    df = df[df[key].isin(group_stats.head(top).index)].sort_values("score_weighted", ascending=False)
    # groupby(...).head() keeps the original row order, which the line above sorted by score.
    selected = cast(pd.DataFrame, df.groupby(key, sort=False).head(rows_per_group))

    return selected, group_stats


def group_key(group_by: GROUP_BY) -> str:
    return "indicator_id" if group_by == "indicator" else "entity_name"


def flagged(n: int, what: str) -> str:
    return f"{n:,} {what if n == 1 else PLURAL[what]} flagged"


def as_scores(values: pd.Series) -> pd.Series:
    """Convert stored scores in [0, 1] to the 0-100 integers that Anomalist displays."""
    return (values * 100).round().astype("Int64")


def build_table(selected: pd.DataFrame, indicators: pd.DataFrame) -> pd.DataFrame:
    """Resolve indicator names and format scores, keeping every column the renderers may need."""
    df = selected.merge(indicators, on="indicator_id", how="left")
    # An indicator can be missing from `variables` (deleted after its anomalies were computed). Fall
    # back to the id, and keep the missing text empty rather than a truthy NaN the renderers print.
    df["indicator"] = df["indicator"].fillna(df["indicator_id"].astype(str))
    df["indicator_name"] = df["indicator_name"].fillna(df["indicator"])
    df["unit"] = df["unit"].fillna("")
    df["anomaly"] = as_scores(df["score"])
    df["relevance"] = as_scores(df["score_weighted"])

    return df


def render_header(
    datasets: pd.DataFrame,
    anomalies: list[gm.Anomaly],
    df: pd.DataFrame,
    group_stats: pd.DataFrame,
    indicators: pd.DataFrame,
    group_by: GROUP_BY,
    top: int,
    relevance: bool,
) -> list[str]:
    """Build the preamble: what these anomalies are, how they rank, and what they do not cover."""
    titles = ", ".join(f"{row.dataset_name} (dataset {row.dataset_id})" for row in datasets.itertuples())
    lines = [f"# Anomalies · {titles}", ""]

    computed_at = max(anomaly.updatedAt for anomaly in anomalies)
    lines.append(
        f"Computed {computed_at:%Y-%m-%d %H:%M} UTC · "
        + " · ".join(f"{anomaly_type} {count:,}" for anomaly_type, count in df["type"].value_counts().items())
    )

    # Coverage. Anomalist samples variables (`--sample-n`, 500 by default; owidbot caps at 1000), so
    # on a large dataset the anomalies cover a fraction of it. Say so — an unqualified digest reads
    # as if the whole dataset had been checked.
    n_covered = df["indicator_id"].nunique()
    n_total = len(indicators)
    coverage = f"Coverage: {n_covered:,} of {n_total:,} indicators in the dataset have anomalies"
    if n_covered < n_total:
        coverage += " — the rest were NOT checked (Anomalist samples variables; see --sample-n)"
    lines.append(coverage)

    # Staleness. The anomalies were computed against one version of the dataset's data; if the
    # dataset has been rebuilt since, they describe data that is no longer there.
    checksums = {}
    for anomaly in anomalies:
        checksums.setdefault(anomaly.datasetId, set()).add(anomaly.datasetSourceChecksum)
    for row in datasets.itertuples():
        if row.dataset_id in checksums and row.sourceChecksum not in checksums[row.dataset_id]:
            lines.append(
                f"⚠️ {row.dataset_name}: the dataset changed since its anomalies were computed, so they may be "
                f"stale. Re-run `etl anomalist --dataset-ids {row.dataset_id}`."
            )

    score = (
        "relevance (anomaly score weighted by population, chart views and scale)"
        if relevance
        else "anomaly score (raw — relevance weighting disabled)"
    )
    lines += [
        "",
        f"Scores are 0-100. Ranked by {score}.",
        f"Showing the top {min(top, len(group_stats)):,} of {len(group_stats):,} {PLURAL[group_by]} with "
        "anomalies, worst first.",
        f"Rows are: {','.join(row_columns(group_by, relevance))}",
    ]

    return lines


def render_markdown(
    header: list[str],
    table: pd.DataFrame,
    group_stats: pd.DataFrame,
    group_by: GROUP_BY,
    top: int,
    relevance: bool = True,
) -> str:
    """Render one section per group: a headline of what the group is, then its anomalies as CSV."""
    key = group_key(group_by)
    columns = row_columns(group_by, relevance)
    score = score_name(relevance)

    lines = list(header)
    for group in group_stats.head(top).itertuples():
        rows = table[table[key] == group.Index]
        n_rows = int(group.n_rows)

        if group_by == "indicator":
            first = rows.iloc[0]
            parts = [f"## {first['indicator_name']} [{group.Index}]"]
            if first["unit"]:
                parts.append(str(first["unit"]))
            if pd.notna(first["views"]):
                parts.append(f"views14d={int(first['views']):,}")
            parts.append(flagged(n_rows, "entity"))
        else:
            parts = [f"## {group.Index}", flagged(n_rows, "indicator")]

        lines += ["", " · ".join(parts)]
        lines.append(rows[columns].to_csv(index=False, header=False, lineterminator="\n").strip())

        # Never let a cap pass silently: what is not listed is still counted, with the score band it
        # sits in, so the reader knows whether raising --rows-per-group would show them anything.
        n_hidden = n_rows - len(rows)
        if n_hidden > 0:
            lines.append(f"… and {n_hidden:,} more, {score} ≤ {rows[score].min()}")

    return "\n".join(lines) + "\n"


def render(
    engine: Engine,
    dataset_ids: list[int],
    anomaly_types: tuple[str, ...] = (),
    group_by: GROUP_BY = "indicator",
    top: int = 50,
    rows_per_group: int = 5,
    min_score: float = 0.0,
    relevance: bool = True,
    output_format: OUTPUT_FORMAT = "md",
) -> str:
    """Load, rank and render the stored anomalies for the given datasets."""
    ids = " --dataset-ids ".join(str(dataset_id) for dataset_id in dataset_ids)

    anomalies = load_anomalies(engine, dataset_ids, anomaly_types)
    if not anomalies:
        types = f" of type(s) {', '.join(anomaly_types)}" if anomaly_types else ""
        return (
            f"No anomalies stored for dataset(s) {ids}{types}.\n"
            f"Detect them first: `etl anomalist --dataset-ids {ids}`.\n"
        )

    # An anomaly whose score frame is empty carries no rows to show (combine_and_reduce_scores_df
    # logs and skips it), and concatenating nothing would raise.
    if all(anomaly.dfReduced is None or anomaly.dfReduced.empty for anomaly in anomalies):
        return f"Anomalies exist for dataset(s) {ids} but hold no scored rows.\n"

    df = build_scores(anomalies, relevance=relevance)
    indicators = load_indicators(engine, dataset_ids)
    datasets = load_datasets(engine, dataset_ids)

    selected, group_stats = select_anomalies(
        df, group_by=group_by, top=top, rows_per_group=rows_per_group, min_score=min_score
    )
    if selected.empty:
        return f"No anomalies scoring at least {min_score} (of {len(df):,} stored).\n"

    table = build_table(selected, indicators)

    if output_format == "csv":
        return cast(str, table[flat_columns(relevance)].to_csv(index=False, lineterminator="\n"))

    if output_format == "json":
        payload: dict[str, Any] = {
            "datasets": json.loads(datasets.to_json(orient="records")),
            "indicators_with_anomalies": int(df["indicator_id"].nunique()),
            "indicators_in_datasets": len(indicators),
            "anomalies_stored": len(df),
            "anomalies_shown": len(table),
            "ranked_by": score_name(relevance),
            "anomalies": json.loads(table[flat_columns(relevance)].to_json(orient="records")),
        }
        return json.dumps(payload, indent=2) + "\n"

    header = render_header(
        datasets=datasets,
        anomalies=anomalies,
        df=df,
        group_stats=group_stats,
        indicators=indicators,
        group_by=group_by,
        top=top,
        relevance=relevance,
    )

    return render_markdown(
        header=header, table=table, group_stats=group_stats, group_by=group_by, top=top, relevance=relevance
    )


@click.command(name="anomalist-view", cls=RichCommand, help=__doc__)
@click.option(
    "--dataset-ids",
    type=int,
    multiple=True,
    required=True,
    help="Grapher dataset ID (or multiple IDs) whose stored anomalies to show.",
)
@click.option(
    "--anomaly-types",
    type=click.Choice(list(get_args(ANOMALY_TYPE))),
    multiple=True,
    default=None,
    help="Only show anomalies of this type (or types). Default: all stored types.",
)
@click.option(
    "--group-by",
    type=click.Choice(list(get_args(GROUP_BY))),
    default="indicator",
    help="Group by indicator (which indicators look broken) or by entity (which countries do).",
)
@click.option(
    "--top",
    type=int,
    default=50,
    help="Number of groups (indicators or entities) to show.",
)
@click.option(
    "--rows-per-group",
    type=int,
    default=5,
    help="Number of anomalies to show per group. The rest are counted, not listed.",
)
@click.option(
    "--min-score",
    type=float,
    default=0.0,
    help="Drop anomalies scoring below this, on the 0-1 scale of the ranking score.",
)
@click.option(
    "--relevance/--no-relevance",
    default=True,
    type=bool,
    help="Rank by the score weighted with population, chart views and scale (needs the population "
    "dataset and the analytics table), instead of by the raw anomaly score.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(list(get_args(OUTPUT_FORMAT))),
    default="md",
    help="Output format: grouped digest (md), or the same selected rows flat (csv, json).",
)
def cli(
    dataset_ids: tuple[int, ...],
    anomaly_types: tuple[str, ...],
    group_by: GROUP_BY,
    top: int,
    rows_per_group: int,
    min_score: float,
    relevance: bool,
    output_format: OUTPUT_FORMAT,
) -> None:
    """Print stored anomalies as compact text.

    **Example 1:** Which indicators of a dataset look most broken?

    ```
    $ STAGING=my-branch etl anomalist-view --dataset-ids 7123
    ```

    **Example 2:** Only the old-vs-new comparison, by country, as CSV

    ```
    $ STAGING=my-branch etl anomalist-view --dataset-ids 7123 --group-by entity --format csv \\
        --anomaly-types upgrade_change --anomaly-types upgrade_missing
    ```
    """
    click.echo(
        render(
            engine=get_engine(),
            dataset_ids=list(dataset_ids),
            anomaly_types=anomaly_types,
            group_by=group_by,
            top=top,
            rows_per_group=rows_per_group,
            min_score=min_score,
            relevance=relevance,
            output_format=output_format,
        ),
        nl=False,
    )


if __name__ == "__main__":
    cli()
