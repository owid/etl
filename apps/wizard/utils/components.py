import json
from contextlib import contextmanager
from copy import deepcopy
from random import sample
from typing import Any, Dict, Optional

import numpy as np
import streamlit as st
import streamlit.components.v1 as components

from etl.config import OWID_ENV, OWIDEnv
from etl.grapher_io import ensure_load_variable, load_variable_data
from etl.grapher_model import Variable

HORIZONTAL_STYLE = """<style class="hide-element">
    /* Hides the style container and removes the extra spacing */
    .element-container:has(.hide-element) {
        display: none;
    }
    /*
        The selector for >.element-container is necessary to avoid selecting the whole
        body of the streamlit app, which is also a stVerticalBlock.
    */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) {
        display: flex;
        flex-direction: row !important;
        flex-wrap: wrap;
        gap: 1rem;
        align-items: baseline;
    }
    /* Override the default width of selectboxes in horizontal layout */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) select {
        min-width: 200px;  /* Set a minimum width for selectboxes */
        max-width: 400px;  /* Optional: Set a max-width to avoid overly wide selectboxes */
    }
    /* Buttons and their parent container all have a width of 704px, which we need to override */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) div {
        width: max-content !important;
    }
    /* Just an example of how you would style buttons, if desired */
    /*
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) button {
        border-color: red;
    }
    */
</style>
"""


@contextmanager
def st_horizontal():
    st.markdown(HORIZONTAL_STYLE, unsafe_allow_html=True)
    with st.container():
        st.markdown('<span class="hide-element horizontal-marker"></span>', unsafe_allow_html=True)
        yield


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
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "showYearLabels": False,
    "id": 807,
    "selectedFacetStrategy": "none",
    "stackMode": "absolute",
    "minTime": "earliest",
    "compareEndPointsOnly": False,
    "version": 14,
    "sortOrder": "desc",
    "maxTime": "latest",
    "type": "LineChart",
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
    "hasChartTab": True,
    "timelineMaxTime": "latest",
    "sortBy": "total",
}


def default_converter(o):
    if isinstance(o, np.integer):  # ignore
        return int(o)
    else:
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def grapher_chart(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    chart_config: Optional[Dict[str, Any]] = None,
    owid_env: OWIDEnv = OWID_ENV,
    selected_entities: Optional[list] = None,
    num_sample_selected_entities: int = 5,
    height=600,
    **kwargs,
):
    """Plot a Grapher chart using the Grapher API.

    You can either plot a given chart config (using chart_config) or plot an indicator with its default metadata using either catalog_path, variable_id or variable.

    Parameters
    ----------
    catalog_path : Optional[str], optional
        Path to the catalog file, by default None
    variable_id : Optional[int], optional
        Variable ID, by default None
    variable : Optional[Variable], optional
        Variable object, by default None
    chart_config : Optional[Dict[str, Any]], optional
        Configuration of the chart, by default None
    owid_env : OWIDEnv, optional
        Environment configuration, by default OWID_ENV
    selected_entities : Optional[list], optional
        List of entities to plot, by default None. If None, a random sample of num_sample_selected_entities will be plotted.
    num_sample_selected_entities : int, optional
        Number of entities to sample if selected_entities is None, by default 5. If there are less entities than this number, all will be plotted.
    height : int, optional
        Height of the chart, by default 600
    """
    # Get data / metadata if no chart config is provided
    if chart_config is None:
        # Define chart config
        chart_config = deepcopy(CONFIG_BASE)

        # Tweak config
        if variable_id is not None:
            chart_config["dimensions"] = [{"property": "y", "variableId": variable_id}]
        else:
            variable = ensure_load_variable(catalog_path, variable_id, variable, owid_env)
            chart_config["dimensions"] = [{"property": "y", "variableId": variable.id}]

        ## Selected entities?
        if selected_entities is not None:
            chart_config["selectedEntityNames"] = selected_entities
        else:
            # Get variable data
            if variable_id is not None:
                df = load_variable_data(variable_id=variable_id, owid_env=owid_env)
            else:
                df = load_variable_data(variable=variable, owid_env=owid_env)
            # Pick selected entities
            entities = list(df["entity"].unique())
            chart_config["selectedEntityNames"] = sample(entities, min(len(entities), num_sample_selected_entities))

    _chart_html(chart_config, owid_env, height=height, **kwargs)


def _chart_html(chart_config: Dict[str, Any], owid_env: OWIDEnv, height=600, **kwargs):
    """Plot a Grapher chart using the Grapher API.

    Parameters
    ----------
    chart_config : Dict[str, Any]
        Configuration of the chart.
    owid_env : OWIDEnv
        Environment configuration. This is needed to access the correct API (changes between servers).
    """
    chart_config_tmp = deepcopy(chart_config)

    chart_config_tmp["bakedGrapherURL"] = f"{owid_env.base_site}/grapher"
    chart_config_tmp["adminBaseUrl"] = owid_env.base_site
    chart_config_tmp["dataApiUrl"] = f"{owid_env.indicators_url}/"

    HTML = f"""
    <link href="https://fonts.googleapis.com/css?family=Lato:300,400,400i,700,700i|Playfair+Display:400,700&amp;display=swap" rel="stylesheet" />
    <link rel="stylesheet" href="https://ourworldindata.org/assets/owid.css" />
    <div class="StandaloneGrapherOrExplorerPage">
        <main>
            <figure data-grapher-src></figure>
        </main>
        <script> document.cookie = "isAdmin=true;max-age=31536000" </script>
        <script type="module" src="https://ourworldindata.org/assets/owid.mjs"></script>
        <script type="module">
            var jsonConfig = {json.dumps(chart_config_tmp, default=default_converter)}; window.Grapher.renderSingleGrapherOnGrapherPage(jsonConfig);
        </script>
    </div>
    """

    components.html(HTML, height=height, **kwargs)


def st_tag(tag_name: str, color: str, icon: str):
    """Create a custom HTML tag.

    Parameters
    ----------
    tag_name : str
        Tag name.
    color : str
        Color of the tag. Must be replaced with any of the following supported colors: blue, green, orange, red, violet, gray/grey, rainbow
    icon: str
        Icon of the tag. Can be material (e.g. ':material/upgrade:') or emoji (e.g. '🪄').
    """
    tag_raw = f":{color}-background[{icon}: {tag_name}]"
    st.markdown(tag_raw)
