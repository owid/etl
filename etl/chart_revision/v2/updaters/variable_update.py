"""Logic to update charts when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""


from typing import Any, Dict, List

from sqlmodel import Session, select

import etl.grapher_model as gm
from backport.datasync.data_metadata import variable_data_df_from_s3
from etl.chart_revision.v2.base import ChartUpdater
from etl.chart_revision.v2.schema import validate_chart_config_and_set_defaults, validate_chart_config_and_remove_defaults
from etl.db import get_engine


class ChartVariableUpdater(ChartUpdater):
    """Handle chart updates when there are updates on variables."""

    def __init__(self, variable_mapping: Dict[int, int]) -> None:
        """Constructor.

        Parameters
        ----------
        variable_mapping : Dict[int, int]
            Mapping between old and new variable IDs.
        """
        # Variable mapping dictionary: Old variable ID -> New variable ID
        self.variable_mapping = variable_mapping
        # Lists with variable IDs (old, new and all)
        self.variable_ids_old = list(variable_mapping.keys())
        self.variable_ids_new = list(variable_mapping.values())
        self.variable_ids_all = self.variable_ids_old + self.variable_ids_new
        # Variable metadata (e.g. min and max years)
        self.__variable_meta = None

    @property
    def variable_meta(self) -> Dict[str, Any]:
        """Variable metadata."""
        if self.__variable_meta is None:
            print("getting metadata")
            self.__variable_meta = self._get_variable_metadata()
        return self.__variable_meta

    def _get_variable_metadata(self) -> Dict[str, Any]:
        """Get variable metadata."""
        # Get variable metadata
        df = variable_data_df_from_s3(get_engine(), variable_ids=self.variable_ids_all, workers=10)
        variable_meta = (
            df.groupby("variableId").year.agg(["min", "max"]).rename(columns={"min": "minYear", "max": "maxYear"})
        )
        variable_meta = variable_meta.to_dict(orient="index")
        return variable_meta

    def find_charts_to_be_updated(self) -> List[gm.Chart]:
        """Find charts that use the variables that are being updated.

        Returns
        -------
        List[gm.Chart]
            List of charts that use the variables that are being updated.
        """
        return find_charts_from_variable_ids(self.variable_ids_old)

    def run(self, config: Dict[str, Any]) -> None:
        """Run the chart variable updater."""
        # Validate the configuration of the chart and add default values
        print(41)
        config_new = validate_chart_config_and_set_defaults(config)
        # Update map configuration
        print(42)
        config_new = update_config_map(
            config_new,
            self.variable_mapping,
            self.variable_meta,
            config_new["dimensions"][0]["variableId"],
        )
        # Update config time
        print(43)
        config_new = update_config_time(config_new, self.variable_mapping, self.variable_meta)
        # Update dimensions
        print(44)
        config_new = update_config_dimensions(config_new, self.variable_mapping)
        # Update sort
        print(45)
        config_new = update_config_sort(config_new, self.variable_mapping)
        # Validate the  configuration of the chart and remove default values (if any)
        print(46)
        config_new = validate_chart_config_and_remove_defaults(config_new)
        return config_new


def find_charts_from_variable_ids(variable_ids: List[int]) -> List[gm.Chart]:
    """Retrieve charts that use the given variables from their IDs.

    TODO: Currently we make two calls to the database. Can probably be reduced to one.

    Parameters
    ----------
    variable_ids : List[int]
        List with variable IDs.

    Returns
    -------
    List[gm.Chart]
        List of charts using the specified variables.
    """
    with Session(get_engine()) as session:
        # Find IDs of charts that use the given variable IDs
        chart_ids = (
            session.exec(select(gm.ChartDimensions.chartId).where(gm.ChartDimensions.variableId.in_(variable_ids)))
            .unique()
            .all()
        )
        # Retrieve charts from a given list of chart IDs
        return session.exec(select(gm.Chart).where(gm.Chart.id.in_(chart_ids))).all()


def update_config_map(
    config: Dict[str, Any],
    variable_mapping: Dict[int, int],
    variable_meta: Dict[str, Any],
    variable_id_default_for_map: int,
) -> Dict[str, Any]:
    """Update map config.

    This mainly involves updating the slider in the map view.

    Parameters
    ----------
    variable_id_default_for_map : int
        ID of the first variable in the old chart. Usually found at config["dimensions"][0]["variableId"]
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `map`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.
    variable_meta : Dict[str, Any]
        Variable metadata. Includes min and max years.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    # Proceed only if chart uses map
    if config["hasMapTab"]:
        print("chart uses map")
        # Get map_variable_id
        map_var_id = config["map"].get(
            "variableId", variable_id_default_for_map
        )  # chart.config["dimensions"][0]["variableId"]
        # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
        if map_var_id in variable_mapping:
            # Get and set new map variable ID in the chart config
            map_var_id_new = variable_mapping[map_var_id]
            config["map"]["variableId"] = map_var_id_new

            # Get year ranges from old and new variables
            year_range_new_min = variable_meta[map_var_id_new]["minYear"]
            year_range_new_max = variable_meta[map_var_id_new]["maxYear"]

            # Set year slider to new value based on new variable's year range
            # - If old time was set to "latest", keep it as it is.
            # - If old time is greater than maximum value in new range, set to 'latest'.
            # - If old time is lower than minimum value in new range, set to the min of the new range.
            # - If old time was set to a particular year, keep it as it is.
            current_time = config["map"]["time"]
            if isinstance(current_time, int):
                if current_time >= year_range_new_max:
                    print(f"{current_time} >= {year_range_new_max}")
                    config["map"]["time"] = "latest"
                elif current_time <= year_range_new_min:
                    print(f"{current_time} <= {year_range_new_min}")
                    config["map"]["time"] = year_range_new_min
    return config


def update_config_time(config: Dict[str, Any], variable_mapping: Dict[int, int], variable_meta: Dict[str, Any]) -> Dict[str, Any]:
    # Get affected variable IDs
    # Builds a dictionary, which has a list of the variable IDs affected categorised by their dimension.
    variables_affected = {
        "x": [],
        "y": [],
        "color": [],
        "size": [],
        "table": [],
    }
    for dimension in config["dimensions"]:
        if dimension["variableId"] in variable_mapping:
            variables_affected[dimension["property"]].append({
                "variableId": dimension["variableId"],
                "minYear": variable_meta[dimension["variableId"]]["minYear"],
                "maxYear": variable_meta[dimension["variableId"]]["maxYear"],
            })

    # # Get current year range for x
    # year_min_x_min = min(dim["minYear"] for dim in variables_affected["x"])
    # year_max_x_max = max(dim["maxYear"] for dim in variables_affected["x"])
    # # Get current year range for y
    # year_min_y_min = min(dim["minYear"] for dim in variables_affected["y"])
    # year_min_y_max = max(dim["minYear"] for dim in variables_affected["y"])
    return config

def update_config_dimensions(config: Dict[str, Any], variable_mapping: Dict[int, int]) -> Dict[str, Any]:
    """Update dimensions in the chart config.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `dimensions`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    # Update dimensions field
    for dimension in config["dimensions"]:
        if dimension["variableId"] in variable_mapping:
            dimension["variableId"] = variable_mapping[dimension["variableId"]]
    return config


def update_config_sort(config: Dict[str, Any], variable_mapping: Dict[int, int]) -> Dict[str, Any]:
    """Update sort in the chart config.

    There are three fields that deal with the sorting of bars in bar charts and marimekko.

        - sortBy: sorting criterium (column, total or entityName)
        - sortColumnSlug: if sortBy is "column", this is the slug of the column to sort by.
        - sortOrder: "asc" or "desc"

    This fields should remain the same. However, when the sorting criterium is set to 'column', the column slug (i.e. variable ID)
    may have changed due to dataset update.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `sort`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    if "sortBy" in config:
        if config["sortBy"] == "column":
            assert "sortColumnSlug" in config, "sortBy is 'column' but sortColumnSlug is not defined!"
            var_old_id = config["sortColumnSlug"]
            config["sortColumnSlug"] = str(variable_mapping.get(int(var_old_id), var_old_id))
    return config
