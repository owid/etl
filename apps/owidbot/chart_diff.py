from dataclasses import dataclass

import pandas as pd
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader
from etl.config import OWIDEnv, get_container_name
from etl.db import production_or_master_engine

from . import github_utils as gh_utils

log = get_logger()


@dataclass
class ChartDiffStatus:
    status_icon: str
    status_title: str
    status_conclusion: str


def create_check_run(repo_name: str, branch: str, charts_df: pd.DataFrame, dry_run: bool = False) -> None:
    access_token = gh_utils.github_app_access_token()
    repo = gh_utils.get_repo(repo_name, access_token=access_token)
    pr = gh_utils.get_pr(repo, branch)
    if not pr:
        log.warning(f"No open pull request found for branch {branch}")
        return

    # Get the latest commit of the pull request
    latest_commit = pr.get_commits().reversed[0]

    status = chart_diff_status(charts_df)

    if not dry_run:
        # Create the check run and complete it in a single command
        repo.create_check_run(
            name="owidbot/chart-diff",
            head_sha=latest_commit.sha,
            status="completed",
            conclusion=status.status_conclusion,
            output={
                "title": status.status_title,
                "summary": format_chart_diff(charts_df),
            },
        )


def chart_diff_status(charts_df: pd.DataFrame) -> ChartDiffStatus:
    # Ignore charts with no config changes
    # TODO: Old staging servers might not have the change_types column, but fix this once
    #   all staging servers are updated
    if "change_types" in charts_df.columns:
        charts_df = charts_df[charts_df.change_types.map(lambda x: "config" in x) | charts_df.is_new]

    if charts_df.empty:
        return ChartDiffStatus("✅", "No charts for review", "neutral")
    elif charts_df[~charts_df.is_rejected].error.any():
        return ChartDiffStatus("⚠️", "Some charts have errors", "failure")
    elif charts_df.is_reviewed.all():
        return ChartDiffStatus("✅", "All charts are reviewed", "success")
    else:
        return ChartDiffStatus("❌", "Some charts are not reviewed", "failure")


def run(branch: str, charts_df: pd.DataFrame) -> str:
    container_name = get_container_name(branch) if branch else "dry-run"

    chart_diff = format_chart_diff(charts_df)
    status = chart_diff_status(charts_df)

    # TODO: Should be using plain /chart-diff instead of query redirect (this is a workaround)
    # Waiting for https://github.com/streamlit/streamlit/issues/8388#issuecomment-2145524922 to be resolved
    body = f"""
<details open>
<summary><a href="http://{container_name}/etl/wizard/?page=chart-diff"><b>chart-diff</b></a>: {status.status_icon}</summary>
{chart_diff}
</details>
    """.strip()

    return body


def call_chart_diff(branch: str) -> pd.DataFrame:
    source_engine = OWIDEnv.from_staging(branch).get_engine()
    target_engine = production_or_master_engine()

    df = ChartDiffsLoader(source_engine, target_engine).get_diffs_summary_df(
        config=True,
        metadata=True,
        data=True,
    )

    return df


def format_chart_diff(df: pd.DataFrame) -> str:
    # Calculate number of data & metadata changes
    # TODO: Old staging servers might not have the change_types column, but fix this once
    #    all staging servers are updated
    if "change_types" in df.columns:
        num_charts_data_change = df.change_types.map(lambda x: "data" in x).sum()
        num_charts_metadata_change = df.change_types.map(lambda x: "metadata" in x).sum()

        # From now on, ignore charts with no config changes
        df = df[df.change_types.map(lambda x: "config" in x) | df.is_new]
    else:
        num_charts_data_change = 0
        num_charts_metadata_change = 0

    if df.empty:
        return "No charts for review."

    rejected = df[df.is_rejected]
    new = df[df.is_new & ~df.is_rejected]
    modified = df[~df.is_new & ~df.is_rejected]

    # Total charts
    num_charts = len(df)
    num_charts_reviewed = df.is_reviewed.sum()

    # Modified charts
    num_charts_modified = len(modified)
    num_charts_modified_reviewed = modified.is_reviewed.sum()

    # New charts
    num_charts_new = len(new)
    num_charts_new_reviewed = new.is_reviewed.sum()

    # Rejected charts
    num_charts_rejected = len(rejected)

    # Errors
    if df.error.any():
        errors = f"<li>Errors: {df.error.notnull().sum()}</li>"
    else:
        errors = ""

    return f"""
<ul>
    <li>{num_charts_reviewed}/{num_charts} reviewed charts</li>
    <li>Modified: {num_charts_modified_reviewed}/{num_charts_modified}</li>
    <li>New: {num_charts_new_reviewed}/{num_charts_new}</li>
    <li>Rejected: {num_charts_rejected}</li>
    <li>Data changes: {num_charts_data_change}</li>
    <li>Metadata changes: {num_charts_metadata_change}</li>
    {errors}
</ul>
    """.strip()
