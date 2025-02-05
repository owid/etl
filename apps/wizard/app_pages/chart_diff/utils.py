from datetime import datetime

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from structlog import get_logger

from etl import config
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher import model as gm

log = get_logger()

WARN_MSG = []

SOURCE = OWID_ENV
assert OWID_ENV.env_remote != "production", "Your .env points to production DB, please use a staging environment."

# Try to compare against production DB if possible, otherwise compare against staging-site-master
if config.ENV_FILE_PROD:
    TARGET = OWIDEnv.from_env_file(config.ENV_FILE_PROD)
else:
    warning_msg = "ENV file doesn't connect to production DB, comparing against `staging-site-master`."
    log.warning(warning_msg)
    WARN_MSG.append(warning_msg)
    TARGET = OWIDEnv.from_staging("master")


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    return SOURCE.engine, TARGET.engine


def prettify_date(chart):
    """Obtain prettified date from a chart.

    Format is:
        - Previous years: `Jan 10, 2020 10:15`
        - This year: `Mar 15, 10:15` (no need to explicitly show the year)
    """
    if chart.updatedAt.year == datetime.now().date().year:
        return chart.updatedAt.strftime("%b %d, %H:%M")
    else:
        return chart.updatedAt.strftime("%b %d, %Y %H:%M")


@st.cache_data
def indicators_in_charts(chart_ids: list[int]) -> dict[int, str]:
    # Get a list of used indicators in chart diffs
    with Session(SOURCE.engine) as session:
        indicator_ids = gm.ChartDimensions.indicators_in_charts(session, chart_ids)
        rows = gm.Variable.from_id(session, variable_id=list(indicator_ids), columns=["id", "name"])
        return {r.id: r.name for r in rows}  # type: ignore
