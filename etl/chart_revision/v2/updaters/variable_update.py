"""Logic to update charts when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""


from typing import Any, Dict, List, Optional, Set

from sqlmodel import Session, select
from structlog import get_logger

import etl.grapher_model as gm
from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from etl.chart_revision.v2.base import ChartUpdater
from etl.chart_revision.v2.schema import (
    fix_errors_in_schema,
    get_schema_chart_config,
    validate_chart_config_and_remove_defaults,
    validate_chart_config_and_set_defaults,
)
from etl.db import get_engine

# Logger
log = get_logger()
# When reviewing the charts with the new variables, we need to get the data points for all their variables.
# This is a costly operation that can lead to an error (https://github.com/owid/etl/issues/1137).
# To avoid this, we disable this whenever there are more than LIMIT_VARIABLES_SLIDER_CHECK variables.
# The consequence of this is that the sliders in the charts are not updated.

LIMIT_VARIABLES_SLIDER_CHECK = 50


class ChartVariableUpdater(ChartUpdater):
    """Handle chart updates when there are updates on variables."""

    def __init__(
        self,
        variable_mapping: Dict[int, int],
        schema: Optional[Dict[str, Any]] = None,
        skip_slider_check_limit: Optional[int] = None,
    ) -> None:
        """Constructor.

        Parameters
        ----------
        variable_mapping : Dict[int, int]
            Mapping between old and new variable IDs.
        schema : Optional[Dict[str, Any]]
            Schema of the chart configuration. Defaults to None.
        skip_slider_check_limit : int
            If the number of variables to be updated is greater than this value, the slider range check is disabled. That is, no changes to the slider are performed.
            This is to avoid errors when updating charts with many variables. Defaults to None.
        """
        # Variable mapping dictionary: Old variable ID -> New variable ID
        self.variable_mapping = variable_mapping
        # Lists with variable IDs (old, new and all)
        self.variable_ids_old = set(variable_mapping.keys())
        self.variable_ids_new = set(variable_mapping.values())
        self.variable_ids_all = self.variable_ids_old | self.variable_ids_new
        # Variable metadata (e.g. min and max years)
        self.__variable_meta = None
        # Set schema
        if schema is not None:
            self.schema = schema
        else:
            self.schema = get_schema_chart_config()
        # Check range for slider
        self.__slider_range_check = None
        if skip_slider_check_limit is None:
            self.num_variables_to_skip_slider_check = LIMIT_VARIABLES_SLIDER_CHECK
        else:
            self.num_variables_to_skip_slider_check = skip_slider_check_limit

    @property
    def variable_meta(self) -> Dict[int, Any]:
        """Variable metadata."""
        if self.__variable_meta is None:
            raise ValueError(
                "Variable metadata is not set. `variable_metadata` is set when `find_charts_to_be_updated` is called."
            )
        return self.__variable_meta

    @property
    def slider_range_check(self) -> bool:
        """Check if slider range check is enabled."""
        if self.__slider_range_check is None:
            raise ValueError(
                "Slider range check is not set. `slider_range_check` is set when `find_charts_to_be_updated` is called."
            )
        return self.__slider_range_check

    def find_charts_to_be_updated(self) -> List[gm.Chart]:
        """Find charts that use the variables that are being updated.

        Also, it gets metadata for variables in use in the charts.

        Returns
        -------
        List[gm.Chart]
            List of charts that use the variables that are being updated.
        """
        # Get charts to be updated
        charts = find_charts_from_variable_ids(self.variable_ids_old)
        # Get variables
        variable_ids = self._get_all_variables_from_charts(charts)
        if len(variable_ids) > self.num_variables_to_skip_slider_check:
            self.__slider_range_check = False
        else:
            self.__slider_range_check = True
        # Get metadata for variables in use in the charts
        if (self.__variable_meta is None) and self.slider_range_check:
            log.info(
                f"variable_update: getting variable metadata of {len(variable_ids)} variables from {len(charts)} charts"
            )
            self.__variable_meta = self._get_variable_metadata(variable_ids)
        return charts

    def _get_all_variables_from_charts(self, charts: List[gm.Chart]) -> List[int]:
        # Get variable IDs from all variables (dimensions x and y) in charts that will be updated
        variable_ids = set()
        for chart in charts:
            variable_ids |= set(c["variableId"] for c in chart.config["dimensions"] if c["property"] in {"x", "y"})
        # Combine with new variables (not yet used in charts)
        variable_ids = variable_ids | set(self.variable_ids_new)
        variable_ids = sorted({v for v in variable_ids if v not in self.variable_ids_old})
        return variable_ids

    def _get_variable_metadata(self, variable_ids: List[int]) -> Dict[int, Any]:
        """Get variable metadata.

        We need to get metadata for variables being updated (old and new), but also for variables that are not being updated, but are used in the charts.
        This is because we need to know the min and max years for all variables used in the charts to update the chart time configuration.
        """
        # Get metadata of variables from S3 (try twice)
        log.info(f"_get_variable_metadata: trying to get variable metadata from S3. Variables IDs: {variable_ids}")
        assert len(variable_ids) > 0, "No variables to get metadata from!"
        df = variable_data_df_from_s3(get_engine(), variable_ids=[int(v) for v in variable_ids], workers=10)

        # Reshape metadata, we want a dictionary!
        variable_meta = (
            df.groupby("variableId").year.agg(["min", "max"]).rename(columns={"min": "minYear", "max": "maxYear"})
        )
        variable_meta = variable_meta.to_dict(orient="index")
        # raise URLError("Could not connect to S3. If this persists, please report in #tech-issues channel on slack.")
        return variable_meta

    def run(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run the chart variable updater."""
        log.info("variable_update: updating configuration")
        # Validate the configuration of the chart and add default values
        config_new = fix_errors_in_schema(config)
        config_new = validate_chart_config_and_set_defaults(config_new, self.schema)
        # Update map configuration
        config_new = self.update_config_map(
            config_new,
            config_new["dimensions"][0]["variableId"],
        )
        # Update dimensions
        config_new = self.update_config_dimensions(config_new)
        # Update sort
        config_new = self.update_config_sort(config_new)
        # Validate the  configuration of the chart and remove default values (if any)
        config_new = validate_chart_config_and_remove_defaults(config_new, self.schema)
        return config_new

    def update_config_map(
        self,
        config: Dict[str, Any],
        variable_id_default_for_map: int,
    ) -> Dict[str, Any]:
        """Update map config.

        This mainly involves updating the slider in the map view.

        Parameters
        ----------
        config : Dict[str, Any]
            Configuration of the chart. It is assumed that no changed has occured under the property `map`.
        variable_id_default_for_map : int
            ID of the first variable in the old chart. Usually found at config["dimensions"][0]["variableId"]

        Returns
        -------
        Dict[str, Any]
            Updated chart configuration.
        """
        log.info("variable_update: updating map config")
        # Proceed only if chart uses map
        if config["hasMapTab"]:
            log.info("variable_update: chart uses map")
            # Get map.columnSlug
            map_var_id = config["map"].get(
                "columnSlug", variable_id_default_for_map
            )  # chart.config["dimensions"][0]["variableId"]
            # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
            if map_var_id in self.variable_mapping:
                # Get and set new map variable ID in the chart config
                map_var_id_new = self.variable_mapping[map_var_id]
                config["map"]["columnSlug"] = str(map_var_id_new)
                if self.slider_range_check:
                    # Get year ranges from new variables
                    year_range_new_min = self.variable_meta[map_var_id_new]["minYear"]
                    year_range_new_max = self.variable_meta[map_var_id_new]["maxYear"]

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

    def update_config_dimensions(
        self,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
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
        log.info("variable_update: updating dimensions")
        # Update dimensions field
        for dimension in config["dimensions"]:
            if dimension["variableId"] in self.variable_mapping:
                dimension["variableId"] = self.variable_mapping[dimension["variableId"]]
        return config

    def update_config_sort(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Update sort in the chart config.

        There are three fields that deal with the sorting of bars in bar charts and marimekko.

            - sortBy: sorting criterium ('column', 'total' or 'entityName')
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
            log.info("variable_update: updating sortBy property")
            if config["sortBy"] == "column":
                assert "sortColumnSlug" in config, "sortBy is 'column' but sortColumnSlug is not defined!"
                var_old_id = config["sortColumnSlug"]
                config["sortColumnSlug"] = str(self.variable_mapping.get(int(var_old_id), var_old_id))
        return config


def find_charts_from_variable_ids(variable_ids: Set[int]) -> List[gm.Chart]:
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
    log.info(f"variable_update: finding charts using old variables ({len(variable_ids)} variables)")
    with Session(get_engine()) as session:
        # Find IDs of charts that use the given variable IDs
        chart_ids = (
            session.exec(select(gm.ChartDimensions.chartId).where(gm.ChartDimensions.variableId.in_(variable_ids)))  # type: ignore
            .unique()
            .all()
        )
        # Retrieve charts from a given list of chart IDs
        charts = session.exec(select(gm.Chart).where(gm.Chart.id.in_(chart_ids))).all()  # type: ignore

    # some charts don't have ID in config, fix that here (should be ideally fixed in the database)
    for chart in charts:
        if "id" not in chart.config:
            log.warning(f"Chart {chart.id} does not have an ID in config.")
            chart.config["id"] = chart.id
        if "version" not in chart.config:
            log.warning(f"Chart {chart.id} does not have a version in config.")
            chart.config["version"] = 1

    return charts


def update_chart_config(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
    schema: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Update indicator references to the new ones.

    The chart config contains some fields that point to the indicators in use. In the attempt to migrating these, we should update all references to the new indicators.
    """
    # Fix errors in schema
    config_new = fix_errors_in_schema(config)
    # Validate config agains schema
    if schema is None:
        schema = get_schema_chart_config()
    config_new = validate_chart_config_and_set_defaults(config_new, schema)
    # Update map tab
    config_new = update_chart_config_map(config_new, indicator_mapping)
    # Update dimensions
    config_new = update_chart_config_dimensions(config_new, indicator_mapping)
    # Update sorting
    config_new = update_chart_config_sort(config_new, indicator_mapping)
    # Update indicator references
    return config_new


def update_chart_config_map(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
) -> Dict[str, Any]:
    """Update map config."""
    log.info("variable_update: updating map config")
    # Proceed only if chart uses map
    if config["hasMapTab"]:
        log.info("variable_update: chart uses map")
        # Get map.columnSlug
        map_var_id = config["map"].get(
            "columnSlug", config["dimensions"][0]["variableId"]
        )  # chart.config["dimensions"][0]["variableId"]
        # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
        if map_var_id in indicator_mapping:
            # Get and set new map variable ID in the chart config
            config["map"]["columnSlug"] = str(indicator_mapping[map_var_id])
    return config


def update_chart_config_dimensions(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
) -> Dict[str, Any]:
    """Update dimensions in the chart config."""
    log.info("variable_update: updating dimensions")
    # Update dimensions field
    for dimension in config["dimensions"]:
        if dimension["variableId"] in indicator_mapping:
            dimension["variableId"] = indicator_mapping[dimension["variableId"]]
    return config


def update_chart_config_sort(
    config: Dict[str, Any],
    indicator_mapping: Dict[int, int],
) -> Dict[str, Any]:
    """Update sort in the chart config.

    There are three fields that deal with the sorting of bars in bar charts and marimekko.

        - sortBy: sorting criterium ('column', 'total' or 'entityName')
        - sortColumnSlug: if sortBy is "column", this is the slug of the column to sort by.
        - sortOrder: "asc" or "desc"

    This fields should remain the same. However, when the sorting criterium is set to 'column', the column slug (i.e. variable ID)
    may have changed due to dataset update.
    """
    if "sortBy" in config:
        log.info("variable_update: updating sortBy property")
        if config["sortBy"] == "column":
            assert "sortColumnSlug" in config, "sortBy is 'column' but sortColumnSlug is not defined!"
            var_old_id = config["sortColumnSlug"]
            config["sortColumnSlug"] = str(indicator_mapping.get(int(var_old_id), var_old_id))
    return config
