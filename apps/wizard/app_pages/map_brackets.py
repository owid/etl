"""TODO

"""

from typing import Any, Dict

import numpy as np
import requests
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.utils import chart_html
from apps.wizard.utils.env import OWID_ENV
from etl.grapher_model import Variable


def load_variable_metadata(session: Session, variable_id: int) -> Dict[str, Any]:
    variable = Variable.load_variable(session=session, variable_id=variable_id)
    variable_metadata = requests.get(variable.s3_metadata_path(typ="http")).json()

    return variable_metadata


def create_default_chart_config_for_variable(variable_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Create a default chart for a variable with id `variable_id`."""
    # TODO: This logic is incomplete. The full logic of how a default chart is constructed for a given variable must be
    #  somewhere in owid_grapher. Ideally, we should recreate that logic, but for now this is enough.
    #   * Get all the ingredients needed from the variable metadata to build the map tab that would be displayed for a given variable id in an indicator-based explorer.
    #   * Also, get the content of the explorer itself, where some of the default map tab parameters may be overridden.
    chart_title = variable_metadata["presentation"].get("titlePublic") or variable_metadata["name"]
    attributions = [
        origin.get("attribution") or f"{origin['producer']} ({origin['datePublished'][0:4]})"
        for origin in variable_metadata["origins"]
    ]
    chart_footer = variable_metadata["presentation"].get("attribution") or "; ".join(attributions)
    chart_config = {
        "title": chart_title,
        "sourceDesc": chart_footer,
        "hasMapTab": True,
        "hasChartTab": False,
        "tab": "map",
        "map": {
            # "timeTolerance": 0,
            "colorScale": {
                "baseColorScheme": "BrBG",
                "binningStrategy": "manual",
                #     "customNumericMinValue": 0,
                #     "customNumericValues": [
                #         10,
                #         20,
                #     ]
            },
        },
        "dimensions": [{"property": "y", "variableId": variable_metadata["id"]}],
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.004.json",
    }

    return chart_config


st.set_page_config(
    page_title="Wizard: ETL Map bracket generator",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)
########################################
# GLOBAL VARIABLES and SESSION STATE
########################################

# Logging
log = get_logger()

VARIABLE_ID = 901018

########################################
# TITLE and DESCRIPTION
########################################
st.title("ETL Map bracket generator")
st.markdown("""🔨 WIP.""")


@st.cache_data
def load_variable_metadata_and_create_default_chart_config(variable_id: int) -> Dict[str, Any]:
    with Session(OWID_ENV.engine) as session:
        variable_metadata = load_variable_metadata(session=session, variable_id=variable_id)

    chart_config = create_default_chart_config_for_variable(variable_metadata=variable_metadata)

    return chart_config


# TODO: Load (and cache) variable data.

chart_config = load_variable_metadata_and_create_default_chart_config(variable_id=VARIABLE_ID)

# TODO: Review the logic suggested in https://github.com/owid/owid-grapher/issues/3641
#  * Add a checkbox to have open lower limit, and another to have an open upper limit.
#    * If lower limit is open, then make "customNumericMinValue" a big number (larger than the minimum bracket).
#    * If the upper limit is open, then add a bracket after the maximum bracket, with a value that is smaller than that maximum bracket.
#  * Create another slider (from 0 to 10) for tolerance.
#  * Create dropdown for color schema.
#  * Create a radio button for linear, log x2, log x3, log x10, or custom.

# chart_config["map"]["colorScale"]["customNumericMinValue"] = 0
# chart_config["map"]["timeTolerance"] = 0
lower_limit, upper_limit = st.slider(label="Select a range of values", min_value=0, max_value=10, value=(0, 10))
# brackets = np.linspace(start=lower_limit, stop=upper_limit, num=10).tolist()
brackets = np.arange(lower_limit, upper_limit, 1).tolist()
chart_config["map"]["colorScale"]["customNumericValues"] = brackets

# Display the chart.
chart_html(chart_config, owid_env=OWID_ENV)
