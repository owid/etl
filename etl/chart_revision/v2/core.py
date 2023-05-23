from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel import Session
from structlog import get_logger

import etl.grapher_model as gm
from etl.chart_revision.v2.base import ChartUpdater
from etl.chart_revision.v2.updaters import ChartUpdaterFASTTReduce, ChartVariableUpdater
from etl.config import GRAPHER_USER_ID
from etl.db import get_engine

log = get_logger()


def create_and_submit_charts_revisions(
    variable_mapping: Optional[Dict[int, int]] = None,
    charts: Optional[List[gm.Chart]] = None,
    chatgpt_reviews: bool = False,
    fastt_reduce: bool = True,
):
    """Create and submit chart revisions.

    You can feed a list of charts to be updated or, alternatively, a variable mapping dictionary. The variable mapping dictionary maps old to new variable IDs.
    A variable mapping is used when we are updating variables and want charts to reflect these.

    The logic is as follows:
        - if `variable_mapping` is given, then we get the list of charts to be updated from the variable mapping and proceed from there.
        - else if `charts` is given, we use these charts to proceed. In this scenario, `ChartVariableUpdater` is not used.
        - else an error is raised.

    Note that this function will commit all chart comparisons to the database.

    Parameters
    ----------
    variable_mapping : Dict[int, int], optional
        Mapping between old and new variable IDs. Is given priority over `charts` to get the list of charts to be updated.
    charts : List[gm.Chart], optional
        List of charts to be reviewed. Only used if `variable_mapping` is not given.
    chatgpt_reviews : bool, optional
        Set to True if you want to use ChatGPT to suggest revisions for the charts (e.g. subtitle revisions, etc.). Defaults to False.
    fastt_reduce : bool, optional
        Set to True if you want to simplify the FASTT. At the moment it just removes the field "data". Defaults to True.
    """
    # Create comparions
    comparisons = update_and_create_chart_comparisons(variable_mapping, charts, chatgpt_reviews, fastt_reduce)
    # Submit chart comparisons
    submit_chart_comparisons(comparisons)


def update_and_create_chart_comparisons(
    variable_mapping: Optional[Dict[int, int]] = None,
    charts: Optional[List[gm.Chart]] = None,
    chatgpt_reviews: bool = False,
    fastt_reduce: bool = True,
) -> List[gm.SuggestedChartRevisions]:
    """Create chart comparisons.

    You can feed a list of charts to be updated or, alternatively, a variable mapping dictionary. The variable mapping dictionary maps old to new variable IDs.
    A variable mapping is used when we are updating variables and want charts to reflect these.

    The logic is as follows:
        - if `variable_mapping` is given, we get the list of charts to be updated from `variable_mapping` and proceed from there.
        - else if `charts` is given, we use these charts to proceed. In this scenario, `ChartVariableUpdater` is not used.
        - else an error is raised.

    Parameters
    ----------
    variable_mapping : Dict[Union[str, int], Union[str, int]], optional
        Mapping between old and new variable IDs. Is given priority over `charts` to get the list of charts to be updated.
    charts : List[gm.Chart], optional
        List of charts to be reviewed. Only used if `variable_mapping` is not given.
    chatgpt_reviews : bool, optional
        Set to True if you want to use ChatGPT to suggest revisions for the charts (e.g. subtitle revisions, etc.). Defaults to False.
    fastt_reduce : bool, optional
        Set to True if you want to simplify the FASTT. At the moment it just removes the field "data". Defaults to True.

    Returns
    -------
    List[gm.SuggestedChartRevisions]
        Sugested chart comparions.
    """
    # Build updaters (and get charts based on `variable_mapping`)
    updaters, charts = build_updaters_and_get_charts(variable_mapping, charts, chatgpt_reviews, fastt_reduce)

    # Initiate list with comparisons
    comparisons = []
    for chart in charts:
        log.info(f"chart_revision: creating comparison for chart {chart.id}")
        # Update chart config
        config_new = update_chart_config(chart.config, updaters)
        # Create chart comparison and add to list
        comparison = create_chart_comparison(chart.config, config_new)
        comparisons.append(comparison)
    return comparisons


def build_updaters_and_get_charts(
    variable_mapping: Optional[Dict[int, int]] = None,
    charts: Optional[List[gm.Chart]] = None,
    chatgpt_reviews: bool = False,
    fastt_reduce: bool = True,
) -> Tuple[List[ChartUpdater], List[gm.Chart]]:
    """Build the list of updaters to process charts.

    You can feed a list of charts to be updated or, alternatively, a variable mapping dictionary. The variable mapping dictionary maps old to new variable IDs.
    A variable mapping is used when we are updating variables and want charts to reflect these.

    The logic is as follows:
        - if `variable_mapping` is given, we get the list of charts to be updated from `variable_mapping` and proceed from there.
        - else if `charts` is given, we use these charts to proceed. In this scenario, `ChartVariableUpdater` is not used.
        - else an error is raised.

    Parameters
    ----------
    variable_mapping : Dict[Union[str, int], Union[str, int]], optional
        Mapping between old and new variable IDs. Is given priority over `charts` to get the list of charts to be updated.
    charts : List[gm.Chart], optional
        List of charts to be reviewed. Only used if `variable_mapping` is not given.
    chatgpt_reviews : bool, optional
        [NOT IMPLEMENTED] Set to True if you want to use ChatGPT to suggest revisions for the charts (e.g. subtitle revisions, etc.). Defaults to False.
    fastt_reduce : bool, optional
        Set to True if you want to simplify the FASTT. At the moment it just removes the field "data". Defaults to True.

    Returns
    -------
    List[ChartUpdater]
        List with updaters. Find more details on available updaters at `etl.chart_revision.v2.updaters`.
    List[gm.Chart]
        List of charts to be updated.
    """
    updaters = []
    # If variable mapping is given, get list of charts to be updated
    if variable_mapping is not None:
        updater_from_variables = ChartVariableUpdater(variable_mapping)
        # Get list of charts affected
        charts = updater_from_variables.find_charts_to_be_updated()
        # Add updater to list
        updaters.append(updater_from_variables)
    # Else if charts is given, check if chatgpt_reviews is True. Otherwise no chart revision is done.
    elif charts is not None:
        if not chatgpt_reviews:
            raise ValueError(
                "If `charts` is given, `chatgpt_reviews` must be True. Otherwise, no chart revision is done!"
            )
    else:
        raise ValueError("You must provide either `variable_mapping` or `charts`!")

    # Add GPT updated if set by user
    if chatgpt_reviews:
        pass
    # Add FASTT reduce updater
    if fastt_reduce:
        updaters.append(ChartUpdaterFASTTReduce())
    return updaters, charts


def update_chart_config(config: Dict[str, Any], updaters: List[ChartUpdater]) -> Dict[str, Any]:
    """Update chart configuration using the `updaters`.

    Chart configuration is updated by applying each updater in the `updaters` list. Each updater has a
    `run` method that takes a chart configuration and returns an updated chart configuration.

    Parameters
    ----------
    config : Dict[str, Any]
        Original chart configuration.
    updaters : List[ChartUpdater]
        List with chart updaters. A chart updater is a class that implements the ChartUpdater interface. It has a run() method that takes a chart configuration and returns an updated chart configuration.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    config_new = deepcopy(config)
    for updater in updaters:
        config_new = updater.run(config_new)
    return config_new


def create_chart_comparison(config_1: Dict[str, Any], config_2: Dict[str, Any]) -> gm.SuggestedChartRevisions:
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
        Sugested chart comparions.
    """
    if config_1["id"] != config_2["id"]:
        raise ValueError("Configurations must be from the same chart.")
    chart_id = config_1["id"]
    return gm.SuggestedChartRevisions(
        chartId=chart_id,
        createdBy=int(GRAPHER_USER_ID),  # type: ignore
        originalConfig=config_1,
        suggestedConfig=config_2,
        status="pending",
        createdAt=datetime.now(),
        updatedAt=datetime.now(),
    )


def submit_chart_comparisons(revisions: List[gm.SuggestedChartRevisions]) -> None:
    """Submit revisions to the database.

    Parameters
    ----------
    revisions : List[gm.SuggestedChartRevisions]
        Chart revisions.
    """
    with Session(get_engine()) as session:
        session.bulk_save_objects(revisions)
        session.commit()
