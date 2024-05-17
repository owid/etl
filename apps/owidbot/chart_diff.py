import pandas as pd
from sqlmodel import Session
from structlog import get_logger

from apps.staging_sync.cli import _get_container_name, _modified_chart_ids_by_admin
from apps.wizard.pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.utils.env import OWID_ENV, OWIDEnv

from . import github_utils as gh_utils

log = get_logger()


def create_check_run(repo_name: str, branch: str, charts_df: pd.DataFrame, dry_run: bool = False) -> None:
    access_token = gh_utils.github_app_access_token()
    repo = gh_utils.get_repo(repo_name, access_token=access_token)
    pr = gh_utils.get_pr(repo, branch)

    # Get the latest commit of the pull request
    latest_commit = pr.get_commits().reversed[0]

    if charts_df.empty:
        conclusion = "neutral"
        title = "No new or modified charts"
    elif charts_df.approved.all():
        conclusion = "success"
        title = "All charts are approved"
    else:
        conclusion = "failure"
        title = "Some charts are not approved"

    if not dry_run:
        # Create the check run and complete it in a single command
        repo.create_check_run(
            name="owidbot/chart-diff",
            head_sha=latest_commit.sha,
            status="completed",
            conclusion=conclusion,
            output={
                "title": title,
                "summary": format_chart_diff(charts_df),
            },
        )


def run(branch: str, charts_df: pd.DataFrame) -> str:
    container_name = _get_container_name(branch) if branch else "dry-run"

    chart_diff = format_chart_diff(charts_df)

    if charts_df.empty or charts_df.approved.all():
        status = "✅"
    else:
        status = "❌"

    body = f"""
<details open>
<summary>{status} <a href="http://{container_name}/etl/wizard/Chart%20Diff"><b>chart-diff</b></a>: </summary>
{chart_diff}
</details>
    """.strip()

    return body


def call_chart_diff(branch: str) -> pd.DataFrame:
    source_engine = OWIDEnv.from_staging(branch).get_engine()

    if OWID_ENV.env_type_id == "live":
        target_engine = OWID_ENV.get_engine()
    else:
        log.warning("ENV file doesn't connect to production DB, comparing against staging-site-master")
        target_engine = OWIDEnv.from_staging("master").get_engine()

    df = []
    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            modified_chart_ids = _modified_chart_ids_by_admin(source_session)

            for chart_id in modified_chart_ids:
                diff = ChartDiffModified.from_chart_id(chart_id, source_session, target_session)
                df.append(
                    {
                        "chart_id": diff.chart_id,
                        "approved": diff.approved,
                        "is_new": diff.is_new,
                    }
                )

    return pd.DataFrame(df)


def format_chart_diff(df: pd.DataFrame) -> str:
    if df.empty:
        return "No new or modified charts."

    new = df[df.is_new]
    modified = df[~df.is_new]

    return f"""
<ul>
    <li>{len(new)} new charts ({new.approved.sum()} approved)</li>
    <li>{len(modified)} modified charts ({modified.approved.sum()} approved)</li>
</ul>
    """.strip()
