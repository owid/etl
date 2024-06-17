"""Helper tool to create map brackets for all indicators in an indicator-based explorer.

"""

import json
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
import streamlit as st
from owid.catalog import find
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.utils import chart_html
from apps.wizard.utils.env import OWID_ENV
from etl.data_helpers.misc import round_to_nearest_power_of_ten, round_to_shifted_power_of_ten, round_to_sig_figs
from etl.explorer_helpers import Explorer
from etl.grapher_model import Entity, Variable
from etl.paths import BASE_DIR

# TODO:
#  * Create another slider (from 0 to 10) for tolerance.
#  * Add "custom" to the list of radio buttons for bracket type.
#  * Consider categorical values.
#  * For linear brackets, here's a possible strategy: Take positive values. Focus either on the entire range from min to max, or from 5% to 95% percentiles. Find all possible ways to divide that range in steps of 1, 2, and 5 (and powers of 10 of those numbers), so that the result has between 4 and 10 brackets.
#  * To choose the best configuration (both for linear and log), rank all possibilities by Gini.

# Logging
log = get_logger()

# Default path to the explorers folder.
EXPLORERS_DIR = BASE_DIR.parent / "owid-content/explorers"
EXPLORER_NAME_DEFAULT = "natural-disasters-temp"

# Maximum number of brackets allowed in a chart.
MAX_NUM_BRACKETS = 10

# Minimum number of brackets allowed in a chart.
MIN_NUM_BRACKETS = 4

# Smallest number (in absolute value) to consider.
SMALLEST_NUMBER_DEFAULT = 0.01

# Labels of the different bracket types.
BRACKET_LABELS = {
    "linear": {
        "1": "linear +1",
        "2": "linear +2",
        "5": "linear +5",
        "custom": "linear custom",
    },
    "log": {
        "x10": "log x10",
        "x2": "log x2",
        "x3": "log x3",
    },
}

# Types of uses of this tool.
USE_TYPE_EXPLORERS = "by explorer"
USE_TYPE_CHART = "by chart"
USE_TYPE_ETL = "by etl path"


@st.cache_data
def load_mappable_regions_and_ids(df: pd.DataFrame) -> Dict[str, int]:
    # TODO: Is there a better way to obtain the list of mappable countries, and their entity ids?
    # Load the external regions dataset.
    regions = find(
        "regions", dataset="regions", namespace="owid_grapher", channels=["external"], version="latest"
    ).load()
    regions_mappable = regions[regions["is_mappable"]]["name"].tolist()

    # List all entity ids used by this variable.
    entity_ids = list(set(df["entities"]))

    # Fetch the mapping of entity ids to names.
    with Session(OWID_ENV.engine) as session:
        entities = Entity.load_entity_mapping(session=session, entity_ids=entity_ids)

    # Create a dictionary mapping regions (only the ones that show in grapher maps) to entity ids.
    region_to_id = {entity: entity_id for entity_id, entity in entities.items() if entity in regions_mappable}

    return region_to_id


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
                # "baseColorScheme": "BrBG",
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


@st.cache_data
def load_explorer(explorer_path: Path) -> str:
    # Load explorer file as a string.
    with open(explorer_path, "r") as f:
        explorer = f.read()

    return explorer


class MapBracketer:
    def __init__(self, variable_id: int):
        self.variable_id = variable_id
        # Load variable from db.
        self.variable = load_variable_from_id(variable_id)
        # Load variable data.
        self.df = load_variable_data(variable=self.variable)
        # Load variable metadata.
        self.metadata = load_variable_metadata(variable=self.variable)
        # Load regions to entity id mapping.
        self.regions_to_id = load_mappable_regions_and_ids(df=self.df)
        # Create a chart config.
        self.chart_config = create_default_chart_config_for_variable(metadata=self.metadata)
        # Select only regions that appear in grapher maps.
        # And for now, focus on the latest year.
        self.values = self.df[
            (self.df["entities"].isin(self.regions_to_id.values())) & (self.df["years"] == self.df["years"].max())
        ]["values"]
        # Get minimum and maximum values in the data.
        self.min_value = self.values.min()
        self.max_value = self.values.max()
        # Get the smallest relevant number in the data.
        self.smallest_number = self.metadata["display"].get("numDecimalPlaces", SMALLEST_NUMBER_DEFAULT)
        # Initialize an attribute of linear increments.
        self.increments = 1
        # Get the most complete list of map brackets that would contain the data.
        self.brackets_all = self.get_all_brackets()
        # Estimate whether the lower and upper brackets should be open.
        self.lower_bracket_open, self.upper_bracket_open = self.are_brackets_open()
        # Define an attribute that determines the bracket type (by default, pick one).
        self.bracket_type = BRACKET_LABELS["log"]["x10"]
        # Define selected brackets (which will be updated later on).
        self.brackets_selected = []
        # Initialize a color scheme attribute.
        self.color_scheme = None

    @property
    def brackets(self):
        # Get a default set of brackets.
        return self.brackets_all[self.bracket_type]

    @property
    def brackets_positive(self):
        return [bracket for bracket in self.brackets if bracket > 0]

    def get_all_brackets(self) -> Dict[str, List[float]]:
        # Find the minimum and maximum absolute nonzero values.
        values_nonzero = abs(self.values[abs(self.values) > self.smallest_number])
        # Find the closest power of 10 that is right below the minimum nonzero value.
        # That would be the minimum bracket possible.
        min_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.min())
        # Find the closest power of 10 that is right above the maximum nonzero value.
        # That would be the maximum bracket possible.
        max_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.max(), floor=False)

        # Initialize a dictionary of brackets.
        brackets_all = {}

        # Create the minimum number of brackets that would fully contain the values.
        # First, do it in powers of 10.
        brackets_x10 = 10 ** np.arange(np.log10(min_bracket_possible), np.log10(max_bracket_possible) + 1, 1)
        brackets_all[BRACKET_LABELS["log"]["x10"]] = brackets_x10

        # Now, do it following the sequence 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, etc.
        brackets_all[BRACKET_LABELS["log"]["x2"]] = np.sort(
            np.hstack([brackets_x10, brackets_x10[:-1] * 2, brackets_x10[:-1] * 5])
        )

        # Now, do it following the sequence 0.1, 0.3, 1, 3, 10, 30, etc.
        brackets_all[BRACKET_LABELS["log"]["x3"]] = np.sort(np.hstack([brackets_x10, brackets_x10[:-1] * 3]))

        for bracket_type, brackets in brackets_all.items():
            if self.values.min() < -self.smallest_number:
                # If there is any negative value in the data, replicate the bracket to the left (to negative values).
                # NOTE: Instead of 0, we assume -0.001, in case there is some numeric noise.
                brackets = np.hstack([-brackets[::-1], [0], brackets])

            # Round numbers.
            brackets_all[bracket_type] = np.array([round_to_sig_figs(bracket) for bracket in brackets])  # .tolist()

        # We want linear brackets to go in increments of 1, 2 or 5 (and powers of 10 of this, e.g. 10, 20, 50, or 0.1, 0.2, 0.5).
        # In principle, we could also have increments of, say, 4, e.g. [0, 40, 80, 120], but by default it's usually better to round to 1, 2, 5, e.g. [0, 20, 40, 60, 80, 100, 120] or [0, 50, 100, 150]. We'll see if this assumption needs to be relaxed.
        # If there are negative values, one of the stops should definitely be zero.
        # E.g. we don't want brackets like [-300, -100, 100, 300], but rather [-400, -200, 0, 200, 400].
        # NOTE: It's not clear if, for purely positive (or purely negative) lists, we need brackets that would pass through zero.
        #  For example, it would be fine to do [300, 500, 700, 900], instead of imposing [200, 400, 600, 800].
        #  For now, and for simplicity, assume that 0 is always one of the stops.

        # First find the smallest size of the increment that would ensure all values can be contained in <=10 bins.
        # TODO: There could be a toggle to "avoid outliers". If activated, use 5th and 95th percentiles instead of min and max.
        smallest_increment = (self.max_value - self.min_value) / (MAX_NUM_BRACKETS)

        shifts = [1, 2, 5]
        for shift in shifts:
            # Round the smallest increment up to the closest shifted power of 10.
            increment = round_to_shifted_power_of_ten(smallest_increment, shifts=[shift], floor=False)
            # Find the largest bracket that would fully cover the minimum data value.
            bracket_min = increment * np.floor(self.min_value / increment).astype(int)
            # Find the smallest bracket that would fully cover the maximum data value.
            bracket_max = increment * np.ceil(self.max_value / increment).astype(int)
            # Create map brackets linearly spaced.
            brackets_all[BRACKET_LABELS["linear"][str(shift)]] = np.arange(
                bracket_min, bracket_max + increment, increment
            ).astype(float)

        # Add an option for linear brackets where the increment is chosen manually.
        brackets_all[BRACKET_LABELS["linear"]["custom"]] = self.get_custom_linear_brackets(
            min_value=self.min_value, max_value=self.max_value, increments=self.increments
        )

        return brackets_all

    def get_custom_linear_brackets(self, min_value, max_value, increments) -> List[int]:
        return np.arange(min_value, max_value + increments, increments).tolist()

    def update_linear_brackets(self) -> None:
        self.brackets_all[BRACKET_LABELS["linear"]["custom"]] = self.get_custom_linear_brackets(  # type: ignore
            min_value=self.min_value, max_value=self.max_value, increments=self.increments
        )

    def are_brackets_open(self):
        # If the minimum value in the data is negative, assume that the lower bracket is open.
        # Otherwise, the most common scenario is that the lower bracket is closed.
        lower_bracket_open = self.values.min() < -self.smallest_number
        # The upper bracket is most frequently open.
        upper_bracket_open = True

        # TODO: Consider creating some additional heuristics about the openness of the lower and upper brackets,
        # e.g. if the maximum (minimum) bracket is 100 (-100), close the upper (lower) bracket.

        return lower_bracket_open, upper_bracket_open

    def update_brackets_selected(self, min_selected: float, max_selected: float) -> None:
        # Filter the list to get the selected values.
        brackets_selected = [
            bracket for bracket in self.brackets if min_selected <= abs(bracket) <= max_selected or bracket == 0
        ]
        if len(brackets_selected) > MAX_NUM_BRACKETS:
            if mb.min_value < -mb.smallest_number:
                st.warning(
                    f"WARNING: {len(brackets_selected)} is too many brackets. Taking only the first {MAX_NUM_BRACKETS} around zero."
                )
                brackets_selected = brackets_selected[
                    int(len(brackets_selected) / 2) - int(MAX_NUM_BRACKETS / 2) : int(len(brackets_selected) / 2)
                    + int(MAX_NUM_BRACKETS / 2)
                ]
            else:
                st.warning(
                    f"WARNING: {len(brackets_selected)} is too many brackets. Taking only the first {MAX_NUM_BRACKETS}."
                )
                brackets_selected = brackets_selected[:MAX_NUM_BRACKETS]
        elif len(brackets_selected) < MIN_NUM_BRACKETS:
            st.warning(f"WARNING: {len(brackets_selected)} is too few brackets. Increase the limits.")

        self.brackets_selected = brackets_selected

    def update_chart_config(self) -> None:
        self.chart_config["map"]["colorScale"]["baseColorScheme"] = self.color_scheme
        self.chart_config["map"]["colorScale"]["customNumericValues"] = self.brackets_selected
        if self.lower_bracket_open:
            # To ensure the lower bracket is open, use a large value of "customNumericMinValue".
            self.chart_config["map"]["colorScale"]["customNumericMinValue"] = self.max_value
        if self.upper_bracket_open:
            # To ensure the upper bracket is open, add an upper bracket with a value that is very small.
            self.chart_config["map"]["colorScale"]["customNumericValues"].append(self.min_value)
        else:
            # To ensure the upper bracket is closed, ensure the upper bracket value is larger than any data value.
            if self.chart_config["map"]["colorScale"]["customNumericValues"][-1] < self.max_value:
                # Find the lowest bracket that is above the maximum value in the data, and use that as the upper bracket.
                self.chart_config["map"]["colorScale"]["customNumericValues"][-1] = min(
                    [bracket for bracket in self.brackets if bracket >= self.max_value]
                )

        if (
            self.chart_config["map"]["colorScale"]["customNumericValues"][0]
            == self.chart_config["map"]["colorScale"].get("customNumericMinValue")
        ) or (self.chart_config["map"]["colorScale"]["customNumericValues"][0] == 0):
            # For some reason, when the lowest bracket is 0, the zeroth bracket gets repeated.
            # No sure what the best solution is. For now, I'll remove the lowest bracket.
            self.chart_config["map"]["colorScale"]["customNumericValues"] = self.chart_config["map"]["colorScale"][
                "customNumericValues"
            ][1:]


def map_bracketer_interactive(mb: MapBracketer) -> None:
    # Add a dropdown for color scheme.
    # TODO: Add full list of color schemes.
    mb.color_scheme = st.selectbox(  # type: ignore
        label="Color scheme (not fully implemented!)",
        options=["BuGn", "BinaryMapPaletteA"],
        help="Color scheme for the map.",
    )

    # Add toggles to control whether lower and upper brackets should be open.
    mb.lower_bracket_open = st.toggle("Lower bracket open", mb.lower_bracket_open)
    mb.upper_bracket_open = st.toggle(
        "Upper bracket open",
        mb.upper_bracket_open,
        help="Note that, even if set to close, it may still remain open if there is a high data value in a previous year.",
    )

    # Select bracket type.
    mb.bracket_type = st.radio(  # type: ignore
        "Select linear or log-like",
        options=list(BRACKET_LABELS["log"].values()) + list(BRACKET_LABELS["linear"].values()),
        index=0,
        horizontal=True,
    )
    if mb.bracket_type == BRACKET_LABELS["linear"]["custom"]:
        # In the case of linear brackets, an additional input is the increment.
        mb.increments = st.number_input("Increments", min_value=0, max_value=100, value=mb.increments, step=1)  # type: ignore
        mb.update_linear_brackets()

    # Create a slider for positive values.
    # TODO: Maybe have a toggle to be able to manually select negative and positive values separately.
    if len(mb.brackets_positive) <= 1:
        st.error("No brackets possible.")
        st.stop()
    min_selected, max_selected = st.select_slider(  # type: ignore
        "Select a range of values:",
        options=mb.brackets_positive,
        value=(min(mb.brackets_positive), max(mb.brackets_positive)),  # default range
    )

    # Update map brackets.
    mb.update_brackets_selected(min_selected=min_selected, max_selected=max_selected)

    # Update chart config given the selections.
    mb.update_chart_config()

    # Display the chart.
    chart_html(mb.chart_config, owid_env=OWID_ENV)


########################################
# TITLE and DESCRIPTION
########################################

st.set_page_config(
    page_title="Wizard: ETL Map bracket generator",
    # layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)
st.title("ETL Map bracket generator")
st.markdown("""🔨 WIP.""")

# Radio buttons to choose how to use this tool.
use_type = st.radio(
    "Select how to use this tool",
    options=[
        USE_TYPE_EXPLORERS,
        USE_TYPE_CHART,
        USE_TYPE_ETL,
    ],
    captions=[
        "Select an indicator-based explorer, improve map brackets one by one, and update the explorer file.",
        "NOT IMPLEMENTED: Search for a chart by id or slug, improve its brackets, and save map configuration in the chart config to (staging) db.",
        "NOT IMPLEMENTED: Search for an ETL path to a dataset, improve indicator map brackets one by one, and save the configuration to a grapher yaml file.",
    ],
    index=0,
    horizontal=True,
)

if use_type in [USE_TYPE_CHART, USE_TYPE_ETL]:
    st.error(f"Use type '{use_type}' not yet implemented.")
    st.stop()
elif use_type == USE_TYPE_EXPLORERS:
    if not EXPLORERS_DIR.is_dir():
        st.error(f"Explorer directory not found: {EXPLORERS_DIR}")
        st.stop()
    # List all explorer names.
    explorer_names = [
        explorer_file.stem.replace(".explorer", "") for explorer_file in sorted(EXPLORERS_DIR.glob("*.explorer.tsv"))
    ]
    # Select an explorer name from a dropdown menu.
    explorer_name: str = st.selectbox(  # type: ignore
        label="Name of explorer file",
        options=explorer_names,
        index=[i for i, name in enumerate(explorer_names) if name == EXPLORER_NAME_DEFAULT][0],
        help="Name of .explorer.tsv file inside owid-content/explorers.",
    )

    # Load and parse explorer content.
    explorer = Explorer(name=explorer_name)

    # Gather all variable ids of indicators with a map tab.
    variable_ids = list(
        dict.fromkeys(sum(explorer.df_graphers[explorer.df_graphers["hasMapTab"]]["yVariableIds"].tolist(), []))
    )

    # Select a variable id from a dropdown menu.
    variable_id: int = st.selectbox(  # type: ignore
        label="Indicator id",
        options=variable_ids,
        index=0,
    )

    # For debugging, fix the value of variable id.
    # Energy variable that has both negative and positive values.
    # variable_id = 900950

    # Initialize map bracketer.
    mb = MapBracketer(variable_id=variable_id)  # type: ignore

    map_bracketer_interactive(mb=mb)

    if st.button("Save brackets in explorer file", type="primary"):
        if "customNumericValues" in mb.chart_config["map"]["colorScale"]:
            # If map brackets have been defined, update explorer.
            if "colorScaleNumericBins" not in explorer.df_graphers.columns:
                # Ensure the column for map brackets exists in the explorer.
                explorer.df_graphers["colorScaleNumericBins"] = None
            # Add entry to the map brackets column for this indicator.
            explorer.df_graphers.loc[
                explorer.df_graphers["yVariableIds"] == variable_id, "colorScaleNumericBins"
            ] = json.dumps(mb.chart_config["map"]["colorScale"]["customNumericValues"])
        if "customNumericMinValue" in mb.chart_config["map"]["colorScale"]:
            # If a minimum bracket have been defined, update explorer.
            if "colorScaleNumericMinValue" not in explorer.df_graphers.columns:
                # Ensure the column for minimum brackets exists in the explorer.
                explorer.df_graphers["colorScaleNumericMinValue"] = None
            # Add entry to the minimum bracket column for this indicator.
            explorer.df_graphers.loc[
                explorer.df_graphers["yVariableIds"] == variable_id, "colorScaleNumericMinValue"
            ] = json.dumps(mb.chart_config["map"]["colorScale"]["customNumericMinValue"])
        # Overwrite explorer file.
        # TODO: Check if the explorer works after these changes, I think the format that the explorer needs may be different (using semicolons?).
        explorer.write()
