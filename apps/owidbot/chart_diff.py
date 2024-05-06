import pandas as pd
from sqlmodel import Session
from structlog import get_logger

from apps.staging_sync.cli import _get_container_name, _get_engine_for_env, _modified_chart_ids_by_admin
from apps.wizard.pages.chart_diff.chart_diff import ChartDiffModified
from etl.paths import ENV_FILE_PROD

log = get_logger()


def run(branch: str) -> str:
    container_name = _get_container_name(branch) if branch else "dry-run"

    chart_diff = format_chart_diff(call_chart_diff(branch))

    body = f"""
<details open>
<summary><b>Chart diff</b>: </summary>
{chart_diff}
<a href="http://{container_name}/etl/wizard/Chart%20Diff">Details</a>
</details>
    """.strip()

    return body


def call_chart_diff(branch: str) -> pd.DataFrame:
    source_engine = _get_engine_for_env(branch)
    if ENV_FILE_PROD.exists():
        target_engine = _get_engine_for_env(ENV_FILE_PROD)
    else:
        log.warning(f"Production env file {ENV_FILE_PROD} not found, comparing against staging-site-master")
        target_engine = _get_engine_for_env("staging-site-master")

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
