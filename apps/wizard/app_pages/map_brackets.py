"""Helper tool to create map brackets for all indicators in an indicator-based explorer."""

import json
from pathlib import Path
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import requests
import streamlit as st
from owid.catalog import find
from owid.datautils.common import ExceptionFromDocstring
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.utils.components import grapher_chart
from etl.collection.explorer.legacy import ExplorerLegacy
from etl.config import OWID_ENV
from etl.data_helpers.misc import round_to_nearest_power_of_ten, round_to_shifted_power_of_ten, round_to_sig_figs
from etl.grapher.model import Entity, Explorer, Variable

# TODO:
#  * Create another slider (from 0 to 10) for tolerance.
#  * Consider categorical values.

# Logging
log = get_logger()

# EXPLORER_NAME_DEFAULT = "natural-disasters-temp"
EXPLORER_NAME_DEFAULT = "minerals"

# Maximum number of brackets allowed in a chart.
MAX_NUM_BRACKETS = 10

# Minimum number of brackets allowed in a chart.
MIN_NUM_BRACKETS = 4

# Smallest number (in absolute value) to consider.
SMALLEST_NUMBER_DEFAULT = 0.01

# Default minimum and maximum percentiles to consider when defining brackets.
MIN_PERCENTILE = 5
MAX_PERCENTILE = 95

# True to display the instances (country, year and value) where the maximum values in the data occur.
DISPLAY_MAXIMUM_INSTANCES = True

# Labels of the different bracket types.
BRACKET_LABELS = {
    "linear": {
        "1": "linear +1",
        "2": "linear +2",
        "5": "linear +5",
    },
    "log": {
        "x10": "log x10",
        "x2": "log x2",
        "x3": "log x3",
    },
    "custom": {"custom": "custom"},
}

# Types of uses of this tool.
USE_TYPE_EXPLORERS = "by explorer"
USE_TYPE_CHART = "by chart"
USE_TYPE_ETL = "by etl path"

# Create a dictionary that maps explorer elements (from the "columns" table) to grapher config elements.
# TODO: Complete with additional keys.
EXPLORER_TO_GRAPHER_KEYS = {
    "colorScaleNumericBins": "customNumericValues",
    "colorScaleNumericMinValue": "customNumericMinValue",
    "colorScaleScheme": "baseColorScheme",
}


class EqualMinimumAndMaximumValues(ExceptionFromDocstring):
    """The selected range of values has a minimum and a maximum that are identical. If you are considering only data from the latest year, you might want to consider all data instead and try again. Otherwise, this indicator should possibly not have a map chart."""


@st.cache_data
def load_mappable_regions_and_ids(df: pd.DataFrame) -> Dict[str, int]:
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
def load_variable_from_catalog_path(catalog_path: str):
    with Session(OWID_ENV.engine) as session:
        variable = Variable.from_catalog_path(session=session, catalog_path=catalog_path)

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
        "chartTypes": [],
        "tab": "map",
        "map": {
            # "timeTolerance": 0,
            "colorScale": {
                # "baseColorScheme": "BrBG",
                "binningStrategy": "manual",
                # "customNumericMinValue": 0,
                # "customNumericValues": [
                #     10,
                #     20,
                # ]
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


@st.cache_data
def load_color_schemes() -> List[str]:
    data = requests.get("https://files.ourworldindata.org/schemas/grapher-schema.004.json").json()
    color_schemes = data["$defs"]["colorScale"]["properties"]["baseColorScheme"]["enum"]

    return color_schemes


def dispersion(hist: Union[List[float], np.ndarray]) -> float:
    """Estimate the dispersion of a histogram.

    It's based on the Gini coefficient. It's zero if a histogram contains the same number of non-zero elements, and
    it's 1 if the content of the histogram is very heterogeneous.

    However, if the content is made of zeros, this function returns 1.

    """

    hist_len = len(hist)
    hist_sum = sum(hist)
    if (hist_len == 0) or (hist_sum == 0):
        return 1
    gini = sum(abs(x_i - x_j) for x_i in hist for x_j in hist) / (2 * hist_len * hist_sum)

    return gini


class MapBracketer:
    def __init__(self, variable_id_or_path: str):
        # Load variable from db.
        self.variable_id_or_path = variable_id_or_path
        if variable_id_or_path.isnumeric():
            self.variable = load_variable_from_id(variable_id=int(variable_id_or_path))
            self.variable_id = int(variable_id_or_path)
            self.catalog_path = self.variable.catalogPath
            self.defined_by_id = True
        else:
            self.variable = load_variable_from_catalog_path(catalog_path=variable_id_or_path)
            self.catalog_path = self.variable_id_or_path
            self.variable_id = self.variable.id
            self.defined_by_id = False
        # Load variable data.
        self.df = load_variable_data(variable=self.variable)
        # Load variable metadata.
        self.metadata = load_variable_metadata(variable=self.variable)
        # Load regions to entity id mapping.
        self.region_to_id = load_mappable_regions_and_ids(df=self.df)
        # For convenience, create the reverse dictionary too.
        self.id_to_region = {v: k for k, v in self.region_to_id.items()}
        # Create a chart config.
        self.chart_config = create_default_chart_config_for_variable(metadata=self.metadata)
        # Define a flag that, if True, the brackets will be decided based on the latest year only.
        # Otherwise, the data for all years will be considered.
        # To begin with, assume True.
        self.latest_year = True
        # Initialize a color scheme attribute.
        self.color_scheme = None
        # Get the smallest relevant number in the data.
        self.smallest_number = self.metadata["display"].get("numDecimalPlaces", SMALLEST_NUMBER_DEFAULT)
        # Define the minimum and maximum percentiles (if 0 and 100, they will be identical to the min and max values).
        # The selected brackets will ensure to cover these values.
        self.percentile_min = MIN_PERCENTILE
        self.percentile_max = MAX_PERCENTILE
        # Define the bracket type (by default, pick an arbitrary one).
        self.bracket_type = BRACKET_LABELS["log"]["x10"]
        # Define all remaining attributes with arbitrary values (they will be updated by self.run()).
        self.lower_bracket_open = False
        self.upper_bracket_open = True
        self.values = pd.Series()
        self.min_value = -np.inf
        self.max_value = np.inf
        self.percentiles_ignored = False

    def run(
        self,
        reload_data_values=True,
        reload_openness=True,
        reload_all_brackets=True,
        reload_rank=True,
        reload_optimal_brackets=True,
    ):
        if reload_data_values:
            # Select only regions that appear in grapher maps.
            data_mask = self.df["entities"].isin(self.region_to_id.values())
            if self.latest_year:
                # Focus on the values of the latest year.
                data_mask &= self.df["years"] == self.df["years"].max()
            # Define an array of values in the data.
            self.values = self.df[data_mask]["values"]
            # Get minimum and maximum values in the data.
            self.min_value = self.values.min()
            self.max_value = self.values.max()
            if self.min_value == self.max_value:
                st.warning(f"Consider setting hasMapTab False for {mb.variable_id_or_path}")
                raise EqualMinimumAndMaximumValues()

        if reload_openness:
            # Estimate whether the lower and upper brackets should be open.
            self.lower_bracket_open, self.upper_bracket_open = self.are_brackets_open()

        if reload_all_brackets:
            # Get the most complete list of map brackets that would fully contain the data.
            self.brackets_all = self.get_all_brackets()

        if reload_rank:
            # Get optimal brackets for all bracket types.
            self.brackets_df = self._get_bracket_score()

        if reload_optimal_brackets:
            # Create a dictionary with the optimal brackets of all types.
            self.brackets_optimal = self.get_optimal_brackets()

        # Define default selected brackets (which will be updated later on).
        self.brackets_selected = self.brackets_all[self.bracket_type].tolist()
        # Define the grapher version of the selected brackets, which needs a minimum value and a list of brackets.
        self.brackets_selected_grapher_min_value = None
        self.brackets_selected_grapher_values = self.brackets_selected.copy()  # type: ignore

    @property
    def brackets(self):
        # Get a default set of brackets.
        return self.brackets_all[self.bracket_type]

    @property
    def brackets_positive(self):
        return [bracket for bracket in self.brackets if bracket > 0]

    def are_brackets_open(self):
        if self.min_value < -self.smallest_number:
            # If the minimum value in the data is negative, assume that the lower bracket is open.
            lower_bracket_open = self.min_value < -self.smallest_number
        else:
            # Otherwise, the most common scenario is that the lower bracket is closed.
            lower_bracket_open = False

        # The upper bracket is most frequently open.
        upper_bracket_open = True

        if ((-101 < self.min_value < 99) or (-1 < self.min_value < 1)) and (99 < self.max_value < 101):
            # If the data is between -100 and 100, or between 0 and 100 (plus minus 1), assume it's percentages.
            # If so, then both brackets should be assumed closed to begin with.
            lower_bracket_open = False
            upper_bracket_open = False

        return lower_bracket_open, upper_bracket_open

    def get_all_brackets(self) -> Dict[str, np.ndarray]:
        # Find the minimum and maximum absolute nonzero values.
        values_nonzero = abs(self.values[abs(self.values) > self.smallest_number])
        if values_nonzero.empty:
            # This happens if the only values are zeros (or smaller than self.smallest_number).
            # For example, 899976.
            # In such cases, to avoid further errors, assign arbitrary brackets.
            st.warning(f"WARNING: Variable {self.variable_id} has no values larger than {self.smallest_number}.")
            brackets_all = {
                label: np.array([0.0, 1.0])
                for label in list(BRACKET_LABELS["linear"].values())
                + list(BRACKET_LABELS["log"].values())
                + list(BRACKET_LABELS["custom"].values())
            }
            return brackets_all

        # Find the closest power of 10 that is right below the minimum nonzero value.
        # That would be the minimum nonzero bracket (the lower bin will be added afterwards).
        min_nonzero_bracket = round_to_nearest_power_of_ten(values_nonzero.min())
        # Find the closest power of 10 that is right above the maximum nonzero value.
        # That would be the maximum bracket possible.
        max_bracket_possible = round_to_nearest_power_of_ten(values_nonzero.max(), floor=False)

        # Initialize a dictionary of brackets.
        brackets_all = {}

        # Calculate log-like brackets.

        # Create the minimum number of brackets that would fully contain the values.
        # First, do it in powers of 10.
        # NOTE: The lower bin is always added to the left. If the brackets are open, it will not be shown.
        brackets_x10 = np.hstack(
            [[0], 10 ** np.arange(np.log10(min_nonzero_bracket), np.log10(max_bracket_possible) + 1, 1)]
        )
        brackets_all[BRACKET_LABELS["log"]["x10"]] = brackets_x10

        # Now, do it following the sequence 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, etc.
        brackets_all[BRACKET_LABELS["log"]["x2"]] = np.sort(
            np.hstack([brackets_x10, brackets_x10[1:-1] * 2, brackets_x10[1:-1] * 5])
        )

        # Now, do it following the sequence 0.1, 0.3, 1, 3, 10, 30, etc.
        brackets_all[BRACKET_LABELS["log"]["x3"]] = np.sort(np.hstack([brackets_x10, brackets_x10[1:-1] * 3]))

        for bracket_type, brackets in brackets_all.items():
            if self.values.min() < -self.smallest_number:
                # If there is any negative value in the data, replicate the bracket to the left (to negative values).
                # NOTE: Instead of 0, we assume -0.001, in case there is some numeric noise.
                brackets = np.hstack([-brackets[::-1], brackets[1:]])

            # Round numbers.
            brackets_all[bracket_type] = np.array([round_to_sig_figs(bracket) for bracket in brackets])  # .tolist()

        # Calculate linear brackets.

        # We want linear brackets to go in increments of 1, 2 or 5 (and powers of 10 of this, e.g. 10, 20, 50, or 0.1, 0.2, 0.5).
        # In principle, we could also have increments of, say, 4, e.g. [0, 40, 80, 120], but by default it's usually better to round to 1, 2, 5, e.g. [0, 20, 40, 60, 80, 100, 120] or [0, 50, 100, 150]. We'll see if this assumption needs to be relaxed.
        # If there are negative values, one of the stops should definitely be zero.
        # E.g. we don't want brackets like [-300, -100, 100, 300], but rather [-400, -200, 0, 200, 400].
        # NOTE: It's not clear if, for purely positive (or purely negative) lists, we need brackets that would pass through zero.
        #  For example, it would be fine to do [300, 500, 700, 900], instead of imposing [200, 400, 600, 800].
        #  For now, and for simplicity, assume that 0 is always one of the stops.

        # First find the smallest size of the increment that would ensure all values can be contained in <=10 bins.
        # smallest_increment = (self.max_value - self.min_value) / (MAX_NUM_BRACKETS)
        _min_value = np.percentile(self.values, self.percentile_min)
        _max_value = np.percentile(self.values, self.percentile_max)

        smallest_increment = (_max_value - _min_value) / (MAX_NUM_BRACKETS)
        if smallest_increment == 0:
            smallest_increment = (self.max_value - self.min_value) / (MAX_NUM_BRACKETS)
            self.percentiles_ignored = True
        else:
            self.percentiles_ignored = False

        shifts = [1, 2, 5]
        for shift in shifts:
            # Round the smallest increment up to the closest shifted power of 10.
            increment = round_to_shifted_power_of_ten(smallest_increment, shifts=[shift], floor=False)  # type: ignore
            # Find the largest bracket that would fully cover the minimum data value.
            bracket_min = increment * np.floor(self.min_value / increment).astype(int)
            # Find the smallest bracket that would fully cover the maximum data value.
            bracket_max = increment * np.ceil(self.max_value / increment).astype(int)
            # Create map brackets linearly spaced.
            # NOTE: Here, we add two increments to ensure the output always has at least MIN_NUM_BRACKETS.
            #  This may need to be rethought.
            bracket_values = np.arange(bracket_min, bracket_max + increment * 2, increment).astype(float)
            if len(bracket_values) == 0:
                # If no brackets are possible, then fill the brackets with the minimum and maximum values possible, and
                # zeros in between.
                bracket_values = [self.min_value] + [0] * (MIN_NUM_BRACKETS - 1) + [self.max_value]
            brackets_all[BRACKET_LABELS["linear"][str(shift)]] = bracket_values

        return brackets_all

    def _get_dispersion_for_all_windows_for_given_brackets(self, brackets):
        brackets = brackets.copy()
        # Roll a given list of brackets until finding the window with a most homogeneous distribution of countries.
        # TODO: Here, we need to ensure that the brackets we are analyzing are aligned with the choices of lower and upper bracket openness, and the bracket values.
        #  I think the slider should show the absolute minimum and maximum, and the default selection should be the
        #  smart one.
        if brackets[-1] < brackets[-2]:
            # To keep the upper bracket open, a value smaller than the upper bracket was added.
            # But now this causes an issue when calculating the histogram.
            # So, instead of a small bracket, add a very big bracket, to include all high values in the histogram.
            brackets[-1] = np.inf
        if brackets[0] > self.min_value:
            # TODO: Instead of this, shouldn't we take the bracket min value?
            # Ensure the lowest bin is added to the left.
            brackets = [self.min_value] + brackets

        if (brackets[0] > self.min_value) or (brackets[-1] < self.max_value):
            # TODO: This should never happen. Consider deleting.
            st.error("Bracket do not fully cover the data.")

        # Find the number of countries in each bracket.
        histogram = np.histogram(self.values, bins=brackets, density=False)[0]
        # Consider if taking a subset of the brackets would improve the histogram.
        # This would happen, for example, if the lowest been has only 1 country.
        # In such a case, it's better to collapse the first and second bin together.
        results = {"brackets": [], "histogram": [], "dispersion": []}
        # TODO: If latest_year is False, we need to calculate the minimum average dispersion (averaged over all years).
        # TODO: For brackets with negative values, the following search will not work. Currently, it goes like:
        # [-1000.0, -1.0, -0.1, -0.01, 1000.0]
        # [-1000.0, -1.0, -0.1, -0.01, -0.0, 1000.0]
        # [-1000.0, -1.0, -0.1, -0.01, -0.0, 0.01, 1000.0]
        # [-1000.0, -1.0, -0.1, -0.01, -0.0, 0.01, 0.1, 1000.0]
        # [-1000.0, -1.0, -0.1, -0.01, -0.0, 0.01, 0.1, 1.0, 1000.0]
        # [-1000.0, -1.0, -0.1, -0.01, -0.0, 0.01, 0.1, 1.0, 10.0, 1000.0]
        # [-1000.0, -1.0, -0.1, -0.01, -0.0, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
        # [-1000.0, -0.1, -0.01, -0.0, 1000.0]
        # [-1000.0, -0.1, -0.01, -0.0, 0.01, 1000.0]
        # [-1000.0, -0.1, -0.01, -0.0, 0.01, 0.1, 1000.0]
        # [-1000.0, -0.1, -0.01, -0.0, 0.01, 0.1, 1.0, 1000.0]
        # But we want to try all combinations of the lower bracket, the upper bracket, and the first increment above and below zero (which should be symmetrical). This would be a bit trickier to implement.
        for bracket_low in range(len(brackets)):
            for bracket_upp in range(
                bracket_low + MIN_NUM_BRACKETS, min(len(histogram) + 1, bracket_low + MAX_NUM_BRACKETS + 1)
            ):
                _brackets = np.hstack([brackets[0], brackets[bracket_low + 1 : bracket_upp], brackets[-1]])

                _histogram = np.hstack(
                    [
                        [sum(histogram[: bracket_low + 1])],
                        histogram[bracket_low + 1 : bracket_upp - 1],
                        [sum(histogram[bracket_upp - 1 :])],
                    ]
                )
                # Sanity check (remove if it always works, to improve performance).
                assert (_histogram == np.histogram(self.values, bins=_brackets)[0]).all()
                if sum(_histogram) != len(self.values):
                    st.error("Unexpected error (histogram does not add up to the number of entities).")
                # Calculate dispersion.
                dispersion_value = dispersion(_histogram)

                # Store results.
                results["brackets"].append(_brackets.tolist())
                results["histogram"].append(_histogram)
                results["dispersion"].append(dispersion_value)
        ranking = pd.DataFrame(results).sort_values("dispersion").reset_index(drop=True)

        return ranking

    def _get_bracket_score(self):
        df = pd.DataFrame()
        for bracket_type, brackets in self.brackets_all.items():
            _df = self._get_dispersion_for_all_windows_for_given_brackets(brackets=brackets)
            _df["bracket_type"] = bracket_type
            df = pd.concat([df, _df], ignore_index=True)

        # There are different criteria to select the optimal score.
        # (1) Countries should be as equally distributed among bins as possible.
        #  For that, we create the "score_dispersion": The closer to 1, the more homogeneous the bins are.
        # (2) Each bin should contain as few countries as possible.
        #  For that, we create the "score_histogram": The closer to 1, the smaller the number of countries per bin is.

        # The final score should be a combination of both.

        # A simple way to obtain the optimal brackets is to choose the configuration with minimum dispersion.
        # That would give the brackets where countries are most homogeneously distributed.
        # However, this could happen with very few brackets (usually the minimum, 4).
        # But we want as few countries in each bracket as possible, so that brackets are as informative as possible.

        # Alternatively, we could define a score that is 0.5 when the number of brackets is MIN_NUM_BRACKETS, and 1 when it is MAX_NUM_BRACKETS.
        # This would strongly reward brackets with MAX_NUM_BRACKETS.
        # df["n_bins"] = df["histogram"].apply(len)
        # df["score_bins"] = (df["n_bins"] + MAX_NUM_BRACKETS - 2 * MIN_NUM_BRACKETS)/( 2 * (MAX_NUM_BRACKETS - MIN_NUM_BRACKETS))

        # In the end, the score is defined as follows:
        # def score(hist):
        #     norm = sum(hist)
        #     return (1 - max(hist)/norm) * (1 - min(hist)/norm) * (1 - dispersion(hist))

        # Get the minimum and maximum value in each histogram.
        histogram_min = df["histogram"].apply(min)
        histogram_max = df["histogram"].apply(max)
        histogram_sum = df["histogram"].apply(sum).unique()
        assert len(histogram_sum) == 1, "Unexpected error in histograms: Sums do not add up to the same number."
        norm = histogram_sum[0]
        df["score_histogram"] = (1 - histogram_min / norm) * (1 - histogram_max / norm)
        df["score_dispersion"] = 1 - df["dispersion"]
        df["score"] = df["score_histogram"] * df["score_dispersion"]

        # Sort the resulting dataframe by score from highest to lowest.
        df = df.sort_values("score", ascending=False).reset_index(drop=True)

        return df

    def get_optimal_brackets(self):
        # Get the best brackets for each bracket type.
        rank = self.brackets_df.drop_duplicates(subset="bracket_type", keep="first").reset_index(drop=True)

        # Create a dictionary.
        _brackets_all_optimal = rank.set_index("bracket_type").to_dict()["brackets"]

        brackets_all_optimal = {}
        for bracket_type, brackets in _brackets_all_optimal.items():
            _brackets = np.array(brackets)
            brackets_all_optimal[bracket_type] = {"brackets": _brackets.tolist()}
            if bracket_type in list(BRACKET_LABELS["log"].values()):
                # TODO: Improve this logic. I suppose it depends on the openness of the limits
                brackets_all_optimal[bracket_type]["lower"] = min(_brackets[_brackets > 0])
                brackets_all_optimal[bracket_type]["upper"] = _brackets[-2]
            else:
                brackets_all_optimal[bracket_type]["lower"] = _brackets[1]
                brackets_all_optimal[bracket_type]["upper"] = _brackets[-2]

        # Create an additional dictionary with the configuration of the optimal brackets.
        brackets_all_optimal["optimal"] = rank.iloc[0].to_dict()["bracket_type"]

        return brackets_all_optimal

    def update_custom_brackets(self, brackets_manual) -> None:
        self.brackets_all[BRACKET_LABELS["custom"]["custom"]] = brackets_manual
        self.brackets_selected = brackets_manual
        self._update_grapher_brackets()

    def update_brackets_selected(self, min_selected: float, max_selected: float) -> None:
        # Filter the list to get the selected values.
        if self.bracket_type in (BRACKET_LABELS["log"].values()):
            brackets_selected = [
                bracket for bracket in self.brackets if min_selected <= abs(bracket) <= max_selected or bracket == 0
            ]
        else:
            brackets_selected = [
                bracket for bracket in self.brackets if min_selected <= bracket <= max_selected or bracket == 0
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

        # Update grapher version of the selected brackets.
        self._update_grapher_brackets()

    def _update_grapher_brackets(self):
        self.brackets_selected_grapher_values = self.brackets_selected.copy()  # type: ignore
        if self.lower_bracket_open:
            # To ensure the lower bracket is open, use a large value of "customNumericMinValue".
            self.brackets_selected_grapher_min_value = self.max_value
        else:
            # Otherwise, I'm not sure what the default is, but simply leave it undefined.
            self.brackets_selected_grapher_min_value = None

        if self.upper_bracket_open:
            self.brackets_selected_grapher_values.append(self.min_value)
        else:
            # To ensure the upper bracket is closed, ensure the upper bracket value is larger than any data value.
            if self.brackets_selected_grapher_values[-1] < self.max_value:
                # Find the lowest bracket that is above the maximum value in the data, and use that as the upper bracket.
                # NOTE: The result may still show an open bracket.
                # The reason is that some country in a year different to the one currently selected has a larger value.
                # This is a limitation of considering only the values of the current year, but I think it's fine.
                # (We shouldn't use closed brackets anyway if the data is not restricted to a closed interval).
                self.brackets_selected_grapher_values.append(
                    min([bracket for bracket in self.brackets if bracket >= self.max_value])
                )

        if (self.brackets_selected_grapher_values[0] == self.brackets_selected_grapher_min_value) or (
            self.brackets_selected_grapher_values[0] == 0
        ):
            # For some reason, when the lowest bracket is 0, the zeroth bracket gets repeated.
            # No sure what the best solution is. For now, I'll remove the lowest bracket.
            self.brackets_selected_grapher_values = self.brackets_selected_grapher_values[1:]

    def update_chart_config(self) -> None:
        self.chart_config["map"]["colorScale"]["baseColorScheme"] = self.color_scheme
        self.chart_config["map"]["colorScale"]["customNumericValues"] = self.brackets_selected_grapher_values
        if self.brackets_selected_grapher_min_value:
            self.chart_config["map"]["colorScale"]["customNumericMinValue"] = self.brackets_selected_grapher_min_value


def map_bracketer_interactive(mb: MapBracketer) -> None:
    # Add a dropdown for color scheme.
    color_schemes = load_color_schemes()
    mb.color_scheme = st.selectbox(  # type: ignore
        label="Color scheme",
        options=color_schemes,
        index=[i for i, color in enumerate(color_schemes) if color == "BuGn"][0],
        help="Color scheme for the map.",
    )

    # Add toggle to control whether the brackets are based only on the data for the latest year, or all years.
    mb.latest_year = st.toggle(
        "Consider only latest year",
        mb.latest_year,
        help="Consider only the values of the latest year in the data, given that the latest year is usually the default view of the chart. These values will be used to create the ranges of values available for the brackets, as well as the optimal brackets. NOTE: Currently, the search for optimal brackets works only for the latest year in the data.",
    )
    if not mb.latest_year:
        st.warning("The optimal bracket search is only properly implemented if choosing data for the latest year.")

    try:
        mb.run(
            reload_data_values=True,
            reload_openness=True,
            reload_all_brackets=True,
            reload_rank=True,
            reload_optimal_brackets=True,
        )
    except Exception as e:
        log.error(e)
        st.error(e)
        st.stop()

    if mb.min_value < -mb.smallest_number:
        st.warning("The optimal bracket search is only properly implemented for purely positive brackets.")

    # Add toggles to control whether lower and upper brackets should be open.
    _message = ""
    if mb.latest_year:
        _message = "Note that, even if set to close, a bracket may still remain open."
        _message = " This happens when closing the bracket implies breaking the scale. For example, if the maximum data value is 100 and the scale is [0, 10, 20, 30, 40], closing the upper bracket would mean breaking the scale: [0, 10, 20, 30, 100], which is undesirable."
        if mb.latest_year:
            _message += " Also, it is possible that a value in a previous year exceeds the current brackets. For example, suppose the maximum data value for the latest year is 35, and brackets are [0, 10, 20, 30, 40]. The maximum bracket will still remain open if the maximum data value in a previous year is larger than 40."
    mb.lower_bracket_open = st.toggle("Lower bracket open", mb.lower_bracket_open, help=_message)
    mb.upper_bracket_open = st.toggle("Upper bracket open", mb.upper_bracket_open, help=_message)
    mb.run(
        reload_data_values=False,
        reload_openness=False,
        reload_all_brackets=True,
        reload_rank=True,
        reload_optimal_brackets=True,
    )
    mb.percentile_min, mb.percentile_max = st.slider(
        label="Percentile range covered by brackets",
        min_value=0,
        max_value=100,
        value=(MIN_PERCENTILE, MAX_PERCENTILE),
        help="This range is used to calculate the total range of values for brackets, to avoid creating a bracket just for one or a few outliers. For example, if there is a single, very high value, the upper bracket will cover only values below the 95th percentile, and the outlier will be placed in the very last (open) bracket. NOTE: Currently, this range only affects linear brackets.",
    )
    mb.run(
        reload_data_values=False,
        reload_openness=False,
        reload_all_brackets=True,
        reload_rank=True,
        reload_optimal_brackets=True,
    )
    if mb.percentiles_ignored:
        st.warning(
            "Minimum and maximum percentiles are identical. Percentiles are ignored. Consider broadening the percentile range."
        )

    # Select bracket type.
    bracket_type_labels = {
        # Add optimal choice
        f"Optimal ({mb.brackets_optimal['optimal']})": mb.brackets_optimal["optimal"],
        # Rest of the options
        **{
            value: value
            for value in list(BRACKET_LABELS["log"].values())
            + list(BRACKET_LABELS["linear"].values())
            + list(BRACKET_LABELS["custom"].values())
        },
    }
    # Create a bracket type selector with a select box.
    bracket_type = st.selectbox(
        "Bracket type (linear, log-like, or custom)",
        options=bracket_type_labels,
        # NOTE: By default, the first option will be selected, which is expected to be the optimal one.
        index=0,
        # horizontal=True,
        help="We usually use either linear or log-like brackets. And we usually prefer certain increments for linear brackets (e.g. 100, or 20, or 5000) rather than arbitrary increments (e.g. 700, 42, or 3.14). Similarly, we usually prefer certain factors for log-like brackets, namely 2 (and 2.5), 3 (and 3.33), and 10. Here you can choose among those options. Alternatively, you can manually enter custom values.",
    )
    mb.bracket_type = bracket_type_labels[bracket_type]  # type: ignore
    if mb.bracket_type == BRACKET_LABELS["custom"]["custom"]:
        # Create an input text box for custom brackets.
        try:
            brackets_manual = st.text_input(
                label="Enter custom brackets, e.g. [0, 10, 20, 30, 40]", value=json.dumps(mb.brackets_selected)
            )
            brackets_manual = json.loads(brackets_manual)
            brackets_manual = [float(bracket) for bracket in brackets_manual]
            if (np.diff(brackets_manual) < 0).any():
                st.warning("Custom brackets are not monotonically increasing.")
        except json.JSONDecodeError:
            st.error("Invalid format for input brackets.")
            st.stop()
        mb.update_custom_brackets(brackets_manual)
    elif mb.bracket_type in list(BRACKET_LABELS["log"].values()):
        # Create a slider for positive values.
        # TODO: Maybe have a toggle to be able to manually select negative and positive values separately.
        if len(mb.brackets_positive) <= 1:
            st.error("No brackets possible.")
            st.stop()
        min_selected, max_selected = st.select_slider(  # type: ignore
            "Full range of valid positive brackets",
            options=mb.brackets_positive,
            value=(
                abs(mb.brackets_optimal[mb.bracket_type]["lower"]),
                abs(mb.brackets_optimal[mb.bracket_type]["upper"]),
            ),
            help="Select the value of the minimum and maximum brackets (in absolute value).",
        )
        # Update log map brackets.
        mb.update_brackets_selected(min_selected=min_selected, max_selected=max_selected)
    elif mb.bracket_type in list(BRACKET_LABELS["linear"].values()):
        # Create a slider for all values.
        min_selected, max_selected = st.select_slider(  # type: ignore
            "Full range of values",
            options=mb.brackets,
            value=(
                mb.brackets_optimal[mb.bracket_type]["lower"],
                mb.brackets_optimal[mb.bracket_type]["upper"],
            ),
            help="Select the value of the minimum and maximum brackets.",
        )
        # Update linear map brackets.
        mb.update_brackets_selected(min_selected=min_selected, max_selected=max_selected)

    # Update chart config given the selections.
    mb.update_chart_config()


def update_explorer_file(mb: MapBracketer, explorer: ExplorerLegacy) -> None:
    index = get_index_of_var(mb, explorer)

    # If custom brackets have been defined, update explorer.
    if "customNumericValues" in mb.chart_config["map"]["colorScale"]:
        if "colorScaleNumericBins" not in explorer.df_columns.columns:
            # Ensure the column for map brackets exists in the columns table of the explorer.
            explorer.df_columns["colorScaleNumericBins"] = None
        # Add entry to the map brackets column for this indicator.
        # NOTE: We should move towards using only catalogPath instead of variableId.
        if (
            (not explorer.df_columns.empty)
            and ("variableId" in explorer.df_columns.columns)
            and ("catalogPath" not in explorer.df_columns.columns)
        ):
            st.warning("Converting explorer to use catalogPath instead of variableId")
            explorer.convert_ids_to_etl_paths()
        # Add a variableId only if there is no catalogPath.
        if pd.isnull(mb.catalog_path):
            st.warning("No catalog path found for current variable. Storing a variableId in explorer.")
            explorer.df_columns.loc[index, "variableId"] = mb.variable_id
        else:
            explorer.df_columns.loc[index, "catalogPath"] = mb.catalog_path

        # Note that, to assign a list to a cell in a dataframe, the "at" method needs to be used, instead of loc.
        explorer.df_columns.at[index, "colorScaleNumericBins"] = mb.chart_config["map"]["colorScale"][
            "customNumericValues"
        ]

    # If a minimum bracket have been defined, update explorer.
    if "customNumericMinValue" in mb.chart_config["map"]["colorScale"]:
        if "colorScaleNumericMinValue" not in explorer.df_columns.columns:
            # Ensure the column for minimum brackets exists in the columns table of the explorer.
            explorer.df_columns["colorScaleNumericMinValue"] = None
        # Add entry to the minimum bracket column for this indicator.
        if "catalogPath" in explorer.df_columns.columns:
            explorer.df_columns.loc[index, ["catalogPath", "colorScaleNumericMinValue"]] = (
                mb.catalog_path,
                mb.chart_config["map"]["colorScale"]["customNumericMinValue"],
            )
        else:
            explorer.df_columns.loc[index, ["variableId", "colorScaleNumericMinValue"]] = (
                mb.variable_id,
                mb.chart_config["map"]["colorScale"]["customNumericMinValue"],
            )

    if "baseColorScheme" in mb.chart_config["map"]["colorScale"]:
        if "colorScaleScheme" not in explorer.df_columns.columns:
            # Ensure the column for color scheme exists in the columns table of the explorer.
            explorer.df_columns["colorScaleScheme"] = None
        # Add entry to the color scheme column for this indicator.
        if "catalogPath" in explorer.df_columns.columns:
            explorer.df_columns.loc[index, "catalogPath"] = mb.catalog_path
        else:
            explorer.df_columns.loc[index, "variableId"] = mb.variable_id
        explorer.df_columns.loc[index, "colorScaleScheme"] = mb.chart_config["map"]["colorScale"]["baseColorScheme"]

    else:
        # If a minimum bracket is not defined in map bracketer, but it was in the original explorer, delete it from the latter.
        if ("colorScaleNumericMinValue" in explorer.df_columns.columns) and (
            pd.notnull(explorer.df_columns.loc[index]["colorScaleNumericMinValue"])
        ):
            explorer.df_columns.loc[index, "colorScaleNumericMinValue"] = None

    # Overwrite explorer file.
    if not explorer.has_changed():
        st.error("Explorer has not changed")
        return
    else:
        explorer.save()
        st.info(f"Successfully updated {explorer.name} explorer file.")


def get_index_of_var(mb, explorer):
    # get index of variable if it exists in the in columns table, otherwise add it.
    if "variableId" not in explorer.df_columns.columns and "catalogPath" not in explorer.df_columns.columns:
        explorer.df_columns["variableId"] = None
    # If variable configuration is already specified in the columns table, overwrite its config.
    if "variableId" in explorer.df_columns.columns and mb.variable_id in set(explorer.df_columns["variableId"]):
        index = explorer.df_columns.loc[explorer.df_columns["variableId"] == mb.variable_id].index.item()
    elif "catalogPath" in explorer.df_columns.columns and mb.catalog_path in set(explorer.df_columns["catalogPath"]):
        index = explorer.df_columns.loc[explorer.df_columns["catalogPath"] == mb.catalog_path].index.item()
    # Otherwise, add new variable to columns table.
    else:
        index = len(explorer.df_columns)

    return index


def pretty_print_number(number: float) -> str:
    # Print numbers using scientific notation only when it's convenient.
    if number > 1e6:
        number_string = f"{number:.2e}"
    elif 0 < abs(number) < 0.01:
        number_string = f"{number:.2e}"
    else:
        if number == int(number):
            number_string = str(int(number))
        else:
            number_string = str(number)

    return number_string


# Display where the maximum values are reached.
def _create_maximum_instances_message(mb: MapBracketer) -> str:
    maximum_instances_to_show = 3
    # Select rows in the data with the maximum value, and select only mappable regions.
    maximum_at = mb.df[(mb.df["values"] == mb.max_value) & (mb.df["entities"].isin(mb.id_to_region))][
        ["entities", "years"]
    ].drop_duplicates()
    maximum_at_string = f"Maximum value ({pretty_print_number(mb.max_value)}) at: " + ", ".join(
        [f"{mb.id_to_region[entity]}-{int(year)}" for entity, year in maximum_at.values[0:maximum_instances_to_show]]
    )
    if len(maximum_at) > maximum_instances_to_show:
        maximum_at_string += f"... ({len(maximum_at)} instances)."
    return maximum_at_string


########################################
# TITLE and DESCRIPTION
########################################

st.set_page_config(
    page_title="Wizard: ETL Map bracket generator",
    # layout="wide",
    page_icon="🪄",
    # initial_sidebar_state="collapsed",
)
st.title(":material/map: Map bracketer")
with st.popover("ℹ️ Learn about it"):
    st.markdown(
        "This tool will find optimal map brackets for a specific variable, and let you manually edit it in a way that is consistent with our guidelines."
    )
    st.markdown(
        """
    **Limitations:**\n
    (1) It works only for indicator-based explorers.\n
    (2) It can find optimal brackets for the latest year in the data, but not all years.\n
    (3) The search for optimal brackets does not work well when there are negative values.\n
    (4) There are still many edge cases that will make this tool fail. It's work in progress!
    """
    )
    st.warning(
        "Currently only supports indicator-based explorers. In the future, we will add support for individual charts and indicators."
    )

# TODO: Change when more use cases are implemented.
# Radio buttons to choose how to use this tool.
# use_type = st.radio(
#     "Select how to use this tool",
#     options=[
#         USE_TYPE_EXPLORERS,
#         USE_TYPE_CHART,
#         USE_TYPE_ETL,
#     ],
#     captions=[
#         "Select an indicator-based explorer, improve map brackets one by one, and update the explorer file.",
#         "NOT IMPLEMENTED: Search for a chart by id or slug, improve its brackets, and save map configuration in the chart config to (staging) db.",
#         "NOT IMPLEMENTED: Search for an ETL path to a dataset, improve indicator map brackets one by one, and save the configuration to a grapher yaml file.",
#     ],
#     index=0,
#     horizontal=True,
# )

use_type = USE_TYPE_EXPLORERS


if use_type in [USE_TYPE_CHART, USE_TYPE_ETL]:
    st.error(f"Use type '{use_type}' not yet implemented.")
    st.stop()
elif use_type == USE_TYPE_EXPLORERS:
    with st.container(border=True):
        # List all explorer names.
        with Session(OWID_ENV.engine) as session:
            explorers = Explorer.load_explorers(session=session, columns=["slug"])
            explorer_names = [explorer.slug for explorer in explorers]

        # Select an explorer name from a dropdown menu.
        explorer_name: str = st.selectbox(  # type: ignore
            label="Name of explorer",
            options=explorer_names,
            index=[i for i, name in enumerate(explorer_names) if name == EXPLORER_NAME_DEFAULT][0],
            help="Name/slug of explorer",
        )

        # Load and parse explorer content.
        explorer = ExplorerLegacy.from_db(explorer_name)

        # Gather all variable ids of indicators with a map tab. In yVariableIds there can be variable ids or etl paths, so be careful going forward
        variable_ids = list(
            dict.fromkeys(sum(explorer.df_graphers[explorer.df_graphers["hasMapTab"]]["yVariableIds"].tolist(), []))
        )

        # Add toggle to include variable ids for which brackets have already been defined in the explorer file.
        include_all_variable_ids = st.toggle(
            "Include indicators with brackets already defined in the explorer file", False
        )
        if not include_all_variable_ids:
            if "colorScaleNumericBins" in explorer.df_columns.columns:
                # Ignore variable_ids for which a map bracket is already defined.
                if "variableId" in explorer.df_columns.columns:
                    id_column = "variableId"
                elif "catalogPath" in explorer.df_columns.columns:
                    id_column = "catalogPath"
                else:
                    st.error("Explorer file does not contain 'variableId' or 'catalogPath' column.")
                    st.stop()
                variable_ids_with_brackets_already_defined = set(
                    explorer.df_columns[explorer.df_columns["colorScaleNumericBins"].notnull()][id_column]
                )
                variable_ids = [
                    variable_id
                    for variable_id in variable_ids
                    if variable_id not in variable_ids_with_brackets_already_defined
                ]

            if len(variable_ids) == 0:
                st.error("No variables to choose from. They may have map brackets already defined.")
                st.stop()

        # Select a variable id from a dropdown menu.
        variable_id: str = str(
            st.selectbox(  # type: ignore
                label=f"Indicator id ({len(variable_ids)} variables available)",
                options=variable_ids,
                index=0,
            )
        )

    # For debugging, fix the value of variable id.
    # Energy variable that has both negative and positive values.
    # variable_id = 900950
    # The following may have nans that cause issues.
    # variable_id = 899976
    # The following also causes issues (because the latest year only has zeros).
    # variable_id = 899859
    # The following fails (because 5th and 95th percentiles coincide).
    # variable_id = 899768
    # The following fails for some reason (I haven't looked into it).
    # variable_id = 899774

    # Load additional configuration for this variable from the explorer file, if any and initialize map bracketer.
    if variable_id.isnumeric():
        additional_config = explorer.get_variable_config(variable_id=int(variable_id))
    else:
        additional_config = explorer.get_variable_config_from_catalog_path(catalog_path=variable_id)

    mb = MapBracketer(variable_id_or_path=variable_id)  # type: ignore

    edit_brackets = True
    if len(additional_config) > 0:
        edit_brackets = st.toggle("Edit indicator with already defined brackets", False)

    if edit_brackets:
        with st.sidebar:
            try:
                # Interact with map bracketer.
                map_bracketer_interactive(mb=mb)
            except Exception as e:
                log.error(e)
                st.error(e)
                st.stop()
    else:
        # NOTE: This additional_config is used to load custom configuration of a variable from an explorer file.
        #  In the future, when working with charts, it could also include the relevant part of the chart config.
        for key_explorer, value in additional_config.items():
            key_grapher = EXPLORER_TO_GRAPHER_KEYS.get(key_explorer, key_explorer)
            # Update values in "map" with the additional configuration.
            mb.chart_config["map"]["colorScale"][key_grapher] = value

    if DISPLAY_MAXIMUM_INSTANCES:
        st.info(_create_maximum_instances_message(mb))

    # Display the chart.
    grapher_chart(chart_config=mb.chart_config, owid_env=OWID_ENV, height=540)

    with st.sidebar:
        if edit_brackets and st.button("Save brackets in explorer file", type="primary"):
            update_explorer_file(mb=mb, explorer=explorer)
