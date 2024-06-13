"""TODO

"""

import math
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import requests
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.utils import chart_html
from apps.wizard.utils.env import OWID_ENV
from etl.grapher_model import Variable


@st.cache_data
def load_variable_from_id(variable_id: int):
    with Session(OWID_ENV.engine) as session:
        variable = Variable.load_variable(session=session, variable_id=variable_id)

    return variable


@st.cache_data
def load_variable_metadata(variable: Variable) -> Dict[str, Any]:
    metadata = requests.get(variable.s3_metadata_path(typ="http")).json()

    return metadata


@st.cache_data
def load_variable_data(variable: Variable) -> pd.DataFrame:
    data = requests.get(variable.s3_data_path(typ="http")).json()
    df = pd.DataFrame(data)

    return df


@st.cache_data
def create_default_chart_config_for_variable(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Create a default chart for a variable with id `variable_id`."""
    chart_config = {
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
        "dimensions": [{"property": "y", "variableId": metadata["id"]}],
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.004.json",
    }

    return chart_config


st.set_page_config(
    page_title="Wizard: ETL Map bracket generator",
    # layout="wide",
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

variable = load_variable_from_id(variable_id=VARIABLE_ID)
df = load_variable_data(variable=variable)
metadata = load_variable_metadata(variable=variable)
chart_config = create_default_chart_config_for_variable(metadata=metadata)

# TODO: Review the logic suggested in https://github.com/owid/owid-grapher/issues/3641
#  * Add a checkbox to have open lower limit, and another to have an open upper limit.
#    * If lower limit is open, then make "customNumericMinValue" a big number (larger than the minimum bracket).
#    * If the upper limit is open, then add a bracket after the maximum bracket, with a value that is smaller than that maximum bracket.
#  * Create another slider (from 0 to 10) for tolerance.
#  * Create dropdown for color schema.
#  * Add "custom" to the list of radio buttons for bracket type.
#  * Consider categorical values.

# Maximum number of brackets allowed in a chart.
MAX_NUM_BRACKETS = 10

# Labels of the different bracket types.
LABEL_LINEAR = "linear"
LABEL_LOG_X2 = "log x2"
LABEL_LOG_X3 = "log x3"
LABEL_LOG_X10 = "log x10"

# For now, focus on the latest year.
values = df[df["years"] == df["years"].max()]["values"]
min_value = values.min()
max_value = values.max()


def round_to_nearest_power_of_ten(number, floor: bool = True):
    if floor:
        return 10 ** (math.floor(math.log10(abs(number if number != 0 else 1))))
    else:
        return 10 ** (math.ceil(math.log10(abs(number if number != 0 else 1))))


def round_to_sig_figs(value: Union[int, float], sig_figs: int = 1) -> float:
    return round(value, sig_figs - 1 - math.floor(math.log10(abs(value if value != 0 else 1))))


# TODO: Move to tests.
def test_round_to_sig_figs_1_sig_fig():
    tests = {
        0.01: 0.01,
        0.059: 0.06,
        0.055: 0.06,
        0.050: 0.05,
        0.0441: 0.04,
        0: 0,
        1: 1,
        5: 5,
        9: 9,
        10: 10,
        11: 10,
        15: 20,
        440.0321: 400,
        450.0321: 500,
        987: 1000,
    }
    for test in tests.items():
        assert round_to_sig_figs(test[0], sig_figs=1) == float(test[1])
        # Check also the same numbers but negative.
        assert round_to_sig_figs(-test[0], sig_figs=1) == -float(test[1])


def test_round_to_sig_figs_2_sig_fig():
    # NOTE: Python will always ignore trailing zeros (even when printing in scientific notation).
    # We could have a function that returns a string that respect significant trailing zeros.
    # But for now, this is good enough.
    tests = {
        0.01: 0.010,
        0.059: 0.059,
        0.055: 0.055,
        0.050: 0.050,
        0.0441: 0.044,
        0: 0.0,
        1: 1.0,
        5: 5.0,
        9: 9.0,
        10: 10,
        11: 11,
        15: 15,
        440.0321: 440,
        450.0321: 450,
        987: 990,
    }
    for test in tests.items():
        assert round_to_sig_figs(test[0], sig_figs=2) == test[1]
        # Check also the same numbers but negative.
        assert round_to_sig_figs(-test[0], sig_figs=2) == -test[1]


def test_round_to_nearest_power_of_ten_floor():
    tests = {
        -0.1: 0,
        -90: 0,
        0: 0,
        1: 1,
        123: 100,
        1001: 1000,
        9000: 1000,
        0.87: 0.1,
        0.032: 0.01,
        0.0005: 0.0001,
    }
    for test in tests.items():
        assert round_to_nearest_power_of_ten(test[0]) == test[1]


def test_round_to_nearest_power_of_ten_ceil():
    tests = {
        -0.1: 0,
        -90: 0,
        0: 0,
        1: 1,
        123: 1000,
        1001: 10000,
        9000: 10000,
        0.87: 1,
        0.032: 0.1,
        0.0005: 0.001,
    }
    for test in tests.items():
        assert round_to_nearest_power_of_ten(test[0], floor=False) == test[1], test


def get_all_possible_log_like_brackets(values) -> Dict[str, List[float]]:
    # TODO: Do we want log-like brackets to also start at the beginning of a log-decade (e.g. 0.1, or 1, or 1000)
    #  and never at 0.3 or 3 or 3000?
    #  I think that's probably the case for log-like brackets that have both positive and negative values.
    #  Not sure about only positive cases. For now, assume so.

    # Find the minimum and maximum absolute nonzero values.
    values_nonzero = abs(values[values != 0])
    # Find the closest power of 10 that is right below the minimum nonzero value.
    # That would be the minimum bracket possible.
    min_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.min())
    # Find the closest power of 10 that is right above the maximum nonzero value.
    # That would be the maximum bracket possible.
    max_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.max(), floor=False)
    # Create the minimum number of brackets that would fully contain the values.
    # First, do it in powers of 10.
    brackets_x10 = 10 ** np.arange(np.log10(min_bracket_possible), np.log10(max_bracket_possible), 1)

    # Now, do it following the sequence 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, etc.
    brackets_x2 = np.sort(np.hstack([brackets_x10, brackets_x10[:-1] * 2, brackets_x10[:-1] * 5]))

    # Now, do it following the sequence 0.1, 0.3, 1, 3, 10, 30, etc.
    brackets_x3 = np.sort(np.hstack([brackets_x10, brackets_x10[:-1] * 3]))

    brackets_all = {
        LABEL_LOG_X10: brackets_x10.tolist(),
        LABEL_LOG_X2: brackets_x2.tolist(),
        LABEL_LOG_X3: brackets_x3.tolist(),
    }

    return brackets_all


def are_brackets_open(values):
    # If the minimum value in the data is negative, assume that the lower bracket is open.
    # Otherwise, the most common scenario is that the lower bracket is closed.
    lower_bracket_open = values.min() < 0
    # The upper bracket is most frequently open.
    upper_bracket_open = False

    # TODO: Consider creating some heuristics about the openness of the lower and upper brackets,
    # e.g. if the maximum (minimum) bracket is 100 (-100), close the upper (lower) bracket.

    return lower_bracket_open, upper_bracket_open


# Calculate the brackets for all possible log-like bracket types.
brackets_all = get_all_possible_log_like_brackets(values=values)

# Estimate whether the lower and upper brackets should be open.
lower_bracket_open_default, upper_bracket_open_default = are_brackets_open(values=values)

# Add toggles to control whether lower and upper brackets should be open.
lower_bracket_open = st.toggle("Lower bracket open", lower_bracket_open_default)
upper_bracket_open = st.toggle("Upper bracket open", upper_bracket_open_default)

# chart_config["map"]["timeTolerance"] = 0
bracket_type = st.radio(
    "Select linear or log-like",
    options=[LABEL_LINEAR, LABEL_LOG_X2, LABEL_LOG_X3, LABEL_LOG_X10],
    index=0,
    horizontal=True,
)
if bracket_type == LABEL_LINEAR:
    # TODO: Implement linear brackets.
    st.number_input("Increments", min_value=0, max_value=100, value=1, step=1)
    brackets = np.arange(min_value, max_value, 1).tolist()
else:
    brackets = [round_to_sig_figs(bracket, sig_figs=1) for bracket in brackets_all[bracket_type]]  # type: ignore

# lower_limit, upper_limit = st.slider(label="Select a range of values", min_value=0, max_value=10, value=(0, 10))
min_selected, max_selected = st.select_slider(  # type: ignore
    "Select a range of values:",
    options=brackets,
    value=(min(brackets), max(brackets)),  # default range
)
# Filter the list to get the selected values
brackets_selected = [bracket for bracket in brackets if min_selected <= bracket <= max_selected]
if len(brackets_selected) > MAX_NUM_BRACKETS:
    st.warning(f"WARNING: {len(brackets_selected)} is too many brackets. Taking only the first {MAX_NUM_BRACKETS}.")
    brackets_selected = brackets_selected[:MAX_NUM_BRACKETS]
# brackets = np.linspace(start=lower_limit, stop=upper_limit, num=10).tolist()
chart_config["map"]["colorScale"]["customNumericValues"] = brackets_selected
if lower_bracket_open:
    # To ensure the lower bracket is open, use a value of "customNumericMinValue" that is larger than the lowest bracket.
    chart_config["map"]["colorScale"]["customNumericMinValue"] = brackets_selected[0] + 1
if upper_bracket_open:
    # To ensure the upper bracket is open, add an upper bracket with a value that is very small.
    chart_config["map"]["colorScale"]["customNumericValues"].append(min_value)

# Display the chart.
chart_html(chart_config, owid_env=OWID_ENV)
