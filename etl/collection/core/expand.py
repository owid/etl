from typing import Any, Dict, List, Literal

import pandas as pd
from owid.catalog import Table

from etl.collection.exceptions import MissingDimensionalIndicatorError
from etl.collection.utils import INDICATORS_SLUG


def expand_config(
    tb: Table,
    indicator_names: str | List[str] | None = None,
    dimensions: list[str] | dict[str, list[str] | str] | None = None,
    common_view_config: Dict[str, Any] | None = None,
    indicator_as_dimension: bool = False,
    indicators_slug: str | None = None,
    expand_path_mode: Literal["table", "dataset", "full"] = "table",
) -> Dict[str, Any]:
    """Create partial config (dimensions and views) from multi-dimensional indicator in table `tb`.

    This method returns the configuration generated from the table `tb`. You can select a subset of indicators with argument `indicator_names`, otherwise all indicators will be expanded.

    Also, it will expand all dimensions and their values, unless `dimensions` is provided. To tweak which dimensions or dimension values are expanded use argument `dimensions` (see below).

    There is also the option to add a common configuration for all views using `common_view_config`. In the future, it'd be nice to support more view-specific configurations. For now, if that's what you want, consider tweaking the output partial config or working on the input indicator metadata (e.g. tweak `grapher_config.title`).

    NOTE
    ----
    1) For more details, refer to class CollectionConfigExpander.

    2) This function generates PARTIAL configuration, you need to then combine it with the config loaded from a YAML file. You can do this combination as you consider. Currently this is mostly manual, but we can look into improving this space:

        ```python
        config = paths.load_collection_config("filename.yml)
        config_new = expand_config(tb=tb)
        config["views"] = config_new["views"]
        config["dimensions"] = config_new["dimensions"]
        ```

    HOWEVER, there is a helper function `combine_config_dimensions` that can help you with combining dimensions.


    3) List of future improvement candidates:
        - Add unit testing.
        - Out-of-the box sorting for dimensions
            - Example: This could be alphabetically ascending or descending.
            - IDEA: We could do this by passing string values directly to dimensions, e.g. dimensions='alphabetical_desc'
        - Out-of-the box sorting for dimension values.
            - Example: This could be alphabetically ascending or descending, or numerically ascending or descending.
            - IDEA: We could pass strings as values directly to the keys in dimensions dictionary, e.g. `dimensions={"sex": "alph_desc", "age": "numerical_desc", "cause": ["aids", "cancer"]}`. To some extent, we already support the function "*" (i.e. show all values without sorting).
        - Support using charts with 'x', 'size' and 'color' indicators. Also support display settings for each indicator.

    Parameters:
    -----------
    tb : Table
        Table with the data, including the indicator and its dimensions. The columns in the table are assumed to contain dimensional information. This can be checked in `tb[col].metadata.additional_info["dimensions"]`.
    indicator_names : str | None
        Name of the indicator to use. This is the actual indicator name, and not the indicator-dimension composite name. If None, it assumes there is only one indicator (and will use it), otherwise it will fail.
    dimensions : None | List[str] | Dict[str, List[str] | str]
        This parameter accepts three types:
            - None:
                - By default, all dimensions and their values are used.
                - The order of dropdowns in the MDIM page (and their values) will be arbitrary.
            - List[str]:
                - The order of dropdowns in the MDIM page will follow the order of the list.
                - If any dimension is missing from the list, this function will raise an error.
                - The order of the dimension values in each dropdown will be arbitrary.
            - Dict[str, str | List[str]]:
                - Keys represent the dimensions, and values are the set of choices to consider for each dimension (use '*' to use all of them).
                - The order of dropdowns in the MDIM page will follow the order of the dictionary.
                - If any dimension is missing from the dictionary keys, this function will raise an error.
                - The order of dimension values in the MDIM page dropdowns will follow that from each dictionary value (unless '*' is uses, which will be arbitrary).
            - See examples below for more details.
    common_view_config : Dict[str, Any] | None
        Additional config fields to add to each view, e.g.
        {"chartTypes": ["LineChart", "DiscreteBar"], "hasMapTab": True, "tab": "map"}
    indicator_as_dimension: bool
        Set to True to keep the indicator as a dimension. For instance, if you expand a table with multiple - dimensional - indicators (e.g. 'population', 'population_density'), a dimension is added in the config that specifies the indicator. If there are more than one indicators being expanded, the indicator information is kept as a dimension regardless of this flag.
    indicators_slug: str
        Name to use as the slug for the indicator dimension. Default is 'indicator'. This is used to identify the indicator in a view using dimensional information.

    EXAMPLES
    --------

    EXAMPLE 1: There are various indicators with dimensions, we want to expand all their dimensions and their values

    ```python
    config = expand_config(tb=tb)
    ```

    EXAMPLE 2: There are multiple indicators, but we focus on 'deaths'. There are dimensions 'sex', 'age' and 'cause' and we want to expand them all completely, in this order.

    ```python
    config = expand_config(
        tb=tb,
        indicator_name=["deaths"],
        dimensions=[
            "sex",
            "age",
            "cause",
        ]
    )

    EXAMPLE 3: Same as Example 2, but (i) we also consider indicator 'cases', and (ii) for 'cause' we only want to use values 'aids' and 'cancer', in this order.

    ```python
    config = expand_config(
        tb=tb,
        indicator_name=["deaths", "cases"],
        dimensions={
            "sex": "*",
            "age": "*",
            "cause": ["aids", "cancer"],
        }
    )
    """
    if indicators_slug is None:
        indicators_slug = INDICATORS_SLUG

    # Partial configuration
    config_partial = {}

    # Initiate expander object
    expander = CollectionConfigExpander(
        tb=tb,
        indicators_slug=indicators_slug,
        indicator_names=indicator_names,
        indicator_as_dimension=indicator_as_dimension,
        expand_path_mode=expand_path_mode,
    )

    # Combine indicator information with dimensions (only when multiple indicators are given)
    if (len(expander.indicator_names) > 1) or (indicator_as_dimension):
        if dimensions is None:
            dimensions = {dim: "*" for dim in expander.dimension_names}
        elif isinstance(dimensions, list):
            dimensions = {dim: "*" for dim in dimensions}
        dimensions = {
            indicators_slug: expander.indicator_names,
            **{k: v for k, v in dimensions.items() if k != indicators_slug},
        }

    # EXPAND CHART_DIMENSIONS
    config_partial["dimensions"] = expander.build_dimensions(
        dimensions=dimensions,
    )

    # Filter of dimensions
    dimension_choices = {}
    for dim in config_partial["dimensions"]:
        dimension_choices[dim["slug"]] = [choice["slug"] for choice in dim["choices"]]

    # EXPAND VIEWS
    config_partial["views"] = expander.build_views(
        common_view_config=common_view_config,
        dimension_choices=dimension_choices,
    )

    return config_partial


class CollectionConfigExpander:
    def __init__(
        self,
        tb: Table,
        indicators_slug: str,
        indicator_names: str | List[str] | None = None,
        indicator_as_dimension: bool = False,
        expand_path_mode: Literal["table", "dataset", "full"] = "table",
    ):
        self.indicators_slug = indicators_slug
        self.indicator_as_dimension = indicator_as_dimension
        self.build_df_dims(tb, indicator_names)
        self.tb = tb
        # Get table dimensions from metadata if available, exclude country, year, and date
        self.tb_dims = [d for d in (tb.m.dimensions or []) if d["slug"] not in ("country", "year", "date")]
        # Indicator expand mode
        self.expand_path_mode = expand_path_mode

    @property
    def dimension_names(self):
        return [col for col in self.df_dims.columns if col not in ["short_name"]]

    @property
    def table_name(self):
        return self.tb.m.short_name

    @property
    def dataset_name(self):
        assert self.tb.m.dataset is not None, "Can't get dataset name without dataset in table's metadata!"
        return self.tb.m.dataset.short_name

    @property
    def dataset_uri(self):
        assert self.tb.m.dataset is not None, "Can't get table URI without dataset in table's metadata!"
        return self.tb.m.dataset.uri

    def build_dimensions(
        self,
        dimensions: List[str] | Dict[str, List[str] | str] | None = None,
    ):
        """Create the specs for each dimension."""
        # Support dimension is None
        ## If dimensions is None, use a list with all dimension names (in no particular order)
        if dimensions is None:
            # Get list of all dimension names (in no particular order)
            dimensions = [col for col in self.df_dims.columns if col not in ["short_name"]]
            # If table defines dimensions, use them instead!
            if self.tb_dims:
                dimensions_tb = [str(d["slug"]) for d in self.tb_dims]
                assert (
                    set(dimensions) == set(dimensions_tb)
                ), f"Dimensions in given table are not complete! Expected: {dimensions}, found: {dimensions_tb}. This might be due to some manual adjustments done to the metadata."
                dimensions = dimensions_tb
            else:
                # If dimensions is None, use a list with all dimension names (in no particular order)
                dimensions = [col for col in self.df_dims.columns if col not in ["short_name"]]
        else:
            # log.warning("It's recommended to set dimensions in Table metadata.")
            pass

        # Support dimensions if it is a list/dict
        config_dimensions = []
        if isinstance(dimensions, (list, dict)):
            # Sanity check: All dimension names should be present in the list or dictionary
            _check_intersection_iters(
                self.dimension_names,
                dimensions,
                key_name="dimensions",
            )

            # Add dimension entry and add it to dimensions
            for dim in dimensions:
                dim_values_available = list(self.df_dims[dim].unique())

                # If list, we don't care about dimension value order
                if isinstance(dimensions, list):
                    dim_values = dim_values_available
                # If dictionary, let's use the order (unless '*' is used)!
                else:
                    dim_values = dimensions[dim]
                    if dim_values == "*":
                        dim_values = dim_values_available
                    elif not isinstance(dim_values, list):
                        # Sanity check: besides exceptions above (where we allow dim_values to be a string initially), dim_values should be a list
                        raise ValueError(
                            f"Unexpected value for dimension `{dim}`. Make sure that the dimension choices are in a list or '*'. Please review `dimensions`: '{dim_values}'!"
                        )

                # Sanity check: values in dim_values are expected
                _check_intersection_iters(
                    dim_values_available,
                    dim_values,
                    key_name=f"dimension={dim}",
                    check_missing=False,
                )

                # Build choices for given dimension
                choices = [
                    {
                        "slug": str(val),
                        "name": str(val),
                        "description": None,
                    }
                    for val in dim_values
                ]

                # Build dimension
                if self.tb_dims:
                    # Use full name from table if available
                    try:
                        dim_name = next(d["name"] for d in self.tb_dims if d["slug"] == dim)
                    except StopIteration:
                        dim_name = dim
                    dim_name = dim_name
                else:
                    # Otherwise use slug
                    dim_name = dim

                # Build dimension
                dimension = {
                    "slug": str(dim),
                    "name": str(dim_name),
                    "choices": choices,
                }

                # Add dimension to config
                config_dimensions.append(dimension)

        return config_dimensions

    def build_views(
        self,
        dimension_choices: Dict[str, List[str]] | None = None,
        common_view_config: Dict[str, Any] | None = None,
    ):
        """Generate one view for each indicator in the table."""
        df_dims_filt = self.df_dims.copy()

        # Keep only relevant dimensions
        if dimension_choices is not None:
            for dim_name, choices in dimension_choices.items():
                df_dims_filt = df_dims_filt[df_dims_filt[dim_name].isin(choices)]

        # Filter to only relevant dimensions
        config_views = []
        for _, indicator in df_dims_filt.iterrows():
            view = {
                "dimensions": {dim_name: indicator[dim_name] for dim_name in self.dimension_names},
                "indicators": {
                    "y": self._expand_indicator_path(
                        indicator.short_name
                    ),  # TODO: Add support for (i) support "x", "color", "size"; (ii) display settings
                },
            }
            if common_view_config:
                view["config"] = common_view_config
            config_views.append(view)

        return config_views

    def _expand_indicator_path(self, indicator_slug: str) -> str:
        if self.expand_path_mode == "table":
            table_path = self.table_name
        elif self.expand_path_mode == "dataset":
            table_path = f"{self.dataset_name}/{self.table_name}"
        elif self.expand_path_mode == "full":
            table_path = f"{self.dataset_uri}/{self.table_name}"
        else:
            raise ValueError(f"Unknown expand_path_mode: {self.expand_path_mode}")
        return f"{table_path}#{indicator_slug}"

    def build_df_dims(self, tb: Table, indicator_names: str | List[str] | None):
        """Build dataframe with dimensional information from table tb.

        It contains the following columns:
            - indicator: Values in this column refer to an 'actual' indicator.
            - dimension1Name: Values in this column provide the dimension value for dimension1Name. E.g. '10-20' in case dimension1Name is 'age'.
            - dimensionXName: Same as dimension1Name. There can be several...
            ...
            - short_name: Name of the column in the original table.

        Example:

        indicator	place	                short_name
        trend	    Grocery and pharmacy	trend__place_grocery_and_pharmacy
        trend	    Parks	                trend__place_parks
        trend	    Residential	            trend__place_residential
        trend	    Retail and recreation	trend__place_retail_and_recreation
        trend	    Transit stations	    trend__place_transit_stations
        trend	    Workplaces	            trend__place_workplaces
        """
        df_dims = self._build_df_dims(tb)

        # Ensure that indicator_name is a list, if any value is given
        if isinstance(indicator_names, str):
            indicator_names = [indicator_names]

        # SANITY CHECKS
        self.indicator_names = self._sanity_checks_df_dims(indicator_names, df_dims)

        # Keep dimensions only for relevant indicators
        self.df_dims = df_dims.loc[df_dims[self.indicators_slug].isin(self.indicator_names)]

        # Drop indicator column if indicator_names is of length 1
        if (len(self.indicator_names) == 1) & (not self.indicator_as_dimension):
            self.df_dims = self.df_dims.drop(columns=[self.indicators_slug])

        # Final checks
        assert all(
            isinstance(indicator_name, str) for indicator_name in self.indicator_names
        ), "Class attribute indicator_names should be a list of string!"
        assert not self.df_dims.empty, "df_dims can't be empty!"

    def _build_df_dims(self, tb):
        """Build dataframe with dimensional information from table tb."""
        records = []
        for col in tb.columns:
            dims = tb[col].m.dimensions
            if dims:
                assert tb[col].m.original_short_name, "Missing metadata.original_short_name for dimensions!"
                row = {
                    self.indicators_slug: tb[col].m.original_short_name,
                    "short_name": col,
                }
                # Add dimensional info
                for name in dims.keys():
                    if name in {self.indicators_slug, "short_name"}:
                        raise ValueError(f"Dimension name `{name}` is reserved. Please use another one!")

                row = {**row, **dims}

                # Add entry to records
                records.append(row)

        if len(records) == 0:
            raise MissingDimensionalIndicatorError(
                "No indicators with dimensions found in the table. Please check that the table has columns with metadata.dimensions set."
            )

        # Build dataframe with dimensional information
        df_dims = pd.DataFrame(records)

        # Re-order columns
        cols_dims = [col for col in df_dims.columns if col not in [self.indicators_slug, "short_name"]]
        df_dims = df_dims[[self.indicators_slug] + sorted(cols_dims) + ["short_name"]]

        # Set df_dims as string!
        df_dims = df_dims.astype(str)

        return df_dims

    def _sanity_checks_df_dims(self, indicator_names: List[str] | None, df_dims: pd.DataFrame):
        """Sanity checks of df_dims."""
        # List with names of indicators and dimensions
        indicator_names_available = list(df_dims[self.indicators_slug].unique())

        # If no indicator name is provided, there should only be one in the table!
        if indicator_names is None:
            if len(indicator_names_available) != 1:
                raise ValueError(
                    "There are multiple indicators, but no `indicator_name` was provided. Please specify at least one!"
                )
            # If only one indicator available, set it as the indicator name
            return indicator_names_available
        # Check that given indicator_names are available (i.e. are present in indicator_names_available)
        indicator_names_unknown = set(indicator_names).difference(set(indicator_names_available))
        if indicator_names_unknown:
            raise ValueError(
                f"Indicators `{', '.join(indicator_names_unknown)}` not found in the table. Available are: {', '.join(indicator_names_available)}"
            )

        return indicator_names


def _check_intersection_iters(
    items_expected,
    items_given,
    key_name: str | None = None,
    check_dups: bool = True,
    check_missing: bool = True,
    check_unexpected: bool = True,
):
    """Check that the intersection/overlap of items_expected and items_given is as expected.

    It checks that:
        - There are no duplicate items in items_given. Unless check_dups is False.
        - items_given cover all the expected items. Unless check_missing is False.
        - items_given don't cover unexpected items. Unless check_unexpected is False.
    """
    if key_name is None:
        key_name = "items_given"

    # Sanity check 1: No duplicate items
    if check_dups and (len(items_given) != len(set(items_given))):
        raise ValueError(f"Duplicate items are not allowed. Please review `{key_name}`!")

    # Sanity check 2: Items should cover all the expected items (there is none missing!)
    items_missing = set(items_expected) - set(items_given)
    if check_missing and items_missing:
        raise ValueError(f"Missing items: {', '.join([f'`{d}`' for d in items_missing])} Please review `{key_name}`!")

    # Sanity check 3: Items shouldn't cover unexpected items (more than needed!)
    items_unexpected = set(items_given) - set(items_expected)
    if check_unexpected and items_unexpected:
        raise ValueError(
            f"Unexpected items: {', '.join([f'`{d}`' for d in items_unexpected])}. Please review `{key_name}`, available are {items_expected}!"
        )
