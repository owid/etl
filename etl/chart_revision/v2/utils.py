from datetime import datetime
from typing import Any, Dict, List

from sqlmodel import Session

import etl.grapher_model as gm
from etl.config import GRAPHER_USER_ID
from etl.db import get_engine


def bake_chart_comparison_from_configs(
    config_1: Dict[str, Any], config_2: Dict[str, Any]
) -> gm.SuggestedChartRevisions:
    """Create a SuggestedChartRevisions object from a two configurations of the same chart.

    This object contains the configuration for two flavours of a chart. Typicaly, `chart_1` is the original chart configuration and
    `chart_2` is the new chart configuration. However, you create a comparison between any two chart configurations.

    IMPORTANT: Configurations must be from the same chart.

    TODO: In the future, this should support comparisons of two *different* charts. For this to happen, we need to add a new table
    in our database (chart_comparison), which would be a generalisation of our current table `suggested_chart_revisions`.

    Parameters
    ----------
    config_1 : Dict[str, Any]
        Configuration 1 of the chart. Typically, the original chart configuration.
    config_2 : Dict[str, Any]
        Configuration 2 of the chart. Typically, the new chart configuration.

    Returns
    -------
    gm.SuggestedChartRevisions
        Sugested revision.
    """
    if config_1["id"] != config_2["id"]:
        raise ValueError("Configurations must be from the same chart.")
    chart_id = config_1["id"]
    return gm.SuggestedChartRevisions(
        chartId=chart_id,
        createdBy=GRAPHER_USER_ID,
        originalConfig=config_1,
        suggestedConfig=config_2,
        status="pending",
        createdAt=datetime.now(),
        updatedAt=datetime.now(),
    )


def submit_revision(revisions: List[gm.SuggestedChartRevisions]) -> None:
    """Submit revisions to the database.

    Parameters
    ----------
    revisions : List[gm.SuggestedChartRevisions]
        Chart revisions.
    """
    with Session(get_engine()) as session:
        session.bulk_save_objects(revisions)
        session.commit()
