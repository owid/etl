"""Tools to generate chart configs."""

from copy import deepcopy
from typing import Any, Dict, List, Optional

import numpy as np

from etl.config import OWID_ENV, OWIDEnv
from etl.grapher.io import ensure_load_variable
from etl.grapher.model import Variable

CONFIG_BASE = {
    # "title": "Placeholder",
    # "subtitle": "Placeholder.",
    # "originUrl": "placeholder",
    # "slug": "placeholder",
    # "selectedEntityNames": ["placeholder"],
    "entityType": "entity",
    "entityTypePlural": "entities",
    "facettingLabelByYVariables": "metric",
    "invertColorScheme": False,
    "yAxis": {
        "canChangeScaleType": False,
        "min": 0,
        "max": "auto",
        "facetDomain": "shared",
        "removePointsOutsideDomain": False,
        "scaleType": "linear",
    },
    "hideTotalValueLabel": False,
    "hideTimeline": False,
    "hideLegend": False,
    "tab": "chart",
    "logo": "owid",
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.007.json",
    "showYearLabels": False,
    "id": 807,
    "selectedFacetStrategy": "none",
    "stackMode": "absolute",
    "minTime": "earliest",
    "compareEndPointsOnly": False,
    "version": 14,
    "sortOrder": "desc",
    "maxTime": "latest",
    "chartTypes": ["LineChart"],
    "hideRelativeToggle": True,
    "addCountryMode": "add-country",
    "hideAnnotationFieldsInTitle": {"entity": False, "changeInPrefix": False, "time": False},
    "matchingEntitiesOnly": False,
    "showNoDataArea": True,
    "scatterPointLabelStrategy": "year",
    "hideLogo": False,
    "xAxis": {
        "canChangeScaleType": False,
        "min": "auto",
        "max": "auto",
        "facetDomain": "shared",
        "removePointsOutsideDomain": False,
        "scaleType": "linear",
    },
    "hideConnectedScatterLines": False,
    "zoomToSelection": False,
    "hideFacetControl": True,
    "hasMapTab": True,
    "hideScatterLabels": False,
    "missingDataStrategy": "auto",
    "isPublished": False,
    "timelineMinTime": "earliest",
    "timelineMaxTime": "latest",
    "sortBy": "total",
}


def bake_chart_config(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int | List[int]] = None,
    variable: Optional[Variable | List[Variable]] = None,
    selected_entities: Optional[list] = None,
    included_entities: Optional[list] = None,
    display: Optional[List[Any]] = None,
    tab: Optional[str] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> Dict[str, Any]:
    """Bake a Grapher chart configuration.

    Bakes a very basic config, which will be enough most of the times. If you want a more complex config, use this as a baseline to adjust to your needs.

    Note: You can find more details on our Grapher API at https://files.ourworldindata.org/schemas/grapher-schema.latest.json.

    """
    # Define chart config
    chart_config = deepcopy(CONFIG_BASE)

    # Tweak config
    if isinstance(variable_id, (int, np.integer)):
        chart_config["dimensions"] = [{"property": "y", "variableId": variable_id}]
    elif isinstance(variable_id, list):
        chart_config["dimensions"] = [{"property": "y", "variableId": v} for v in variable_id]
    elif isinstance(catalog_path, str):
        variable = ensure_load_variable(catalog_path=catalog_path, owid_env=owid_env)
        chart_config["dimensions"] = [{"property": "y", "variableId": variable.id}]
    elif isinstance(variable, Variable):
        chart_config["dimensions"] = [{"property": "y", "variableId": variable.id}]
    elif isinstance(variable, list):
        chart_config["dimensions"] = [{"property": "y", "variableId": v.id} for v in variable]
    else:
        variable = ensure_load_variable(catalog_path, variable_id, variable, owid_env)
        chart_config["dimensions"] = [{"property": "y", "variableId": variable.id}]

    if display is not None:
        assert len(display) == len(chart_config["dimensions"])
        for i, d in enumerate(display):
            chart_config["dimensions"][i]["display"] = d

    ## Selected entities?
    if selected_entities is not None:
        chart_config["selectedEntityNames"] = selected_entities

    # Included entities
    if included_entities is not None:
        included_entities = [str(entity) for entity in included_entities]
        chart_config["includedEntities"] = included_entities

    # Edit initial tab
    if tab is not None:
        chart_config["tab"] = tab
    return chart_config
