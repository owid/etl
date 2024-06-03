import pandas as pd
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.chart_sync.cli import _modified_chart_ids_by_admin
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.utils.env import OWID_ENV, OWIDEnv
from etl.config import get_container_name

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
        title = "No charts for review"
    elif charts_df.is_reviewed.all():
        conclusion = "success"
        title = "All charts are reviewed"
    else:
        conclusion = "failure"
        title = "Some charts are not reviewed"

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
    container_name = get_container_name(branch) if branch else "dry-run"

    chart_diff = format_chart_diff(charts_df)

    if charts_df.empty or charts_df.is_reviewed.all():
        status = "✅"
    else:
        status = "❌"

    # TODO: Should be using plain /chart-diff instead of query redirect (this is a workaround)
    # Waiting for https://github.com/streamlit/streamlit/issues/8388#issuecomment-2145524922 to be resolved
    body = f"""
<details open>
<summary><a href="http://{container_name}/etl/wizard/?page=chart-diff"><b>chart-diff</b></a>: {status}</summary>
{chart_diff}
</details>
    """.strip()

    return body


def call_chart_diff(branch: str) -> pd.DataFrame:
    source_engine = OWIDEnv.from_staging(branch).get_engine()

    if OWID_ENV.env_type_id == "production":
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
                        "is_approved": diff.is_approved,
                        "is_pending": diff.is_pending,
                        "is_rejected": diff.is_rejected,
                        "is_reviewed": diff.is_reviewed,
                        "is_new": diff.is_new,
                    }
                )

    return pd.DataFrame(df)


def format_chart_diff(df: pd.DataFrame) -> str:
    if df.empty:
        return "No charts for review."

    new = df[df.is_new]
    modified = df[~df.is_new]

    return f"""
<ul>
    <li>{len(new)} new charts ({new.is_reviewed.sum()} reviewed)</li>
    <li>{len(modified)} modified charts ({modified.is_reviewed.sum()} reviewed)</li>
</ul>
    """.strip()
