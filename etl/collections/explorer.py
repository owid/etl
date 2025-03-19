from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from owid.catalog.utils import underscore

from etl.collections.common import INDICATORS_SLUG, combine_config_dimensions, expand_config, get_mapping_paths_to_id
from etl.collections.explorer_legacy import _create_explorer_legacy
from etl.collections.model import CHART_DIMENSIONS, Collection, Definitions, ExplorerView, pruned_json
from etl.collections.utils import (
    get_tables_by_name_mapping,
    validate_indicators_in_db,
)
from etl.config import OWID_ENV, OWIDEnv

__all__ = [
    "expand_config",
    "combine_config_dimensions",
]


@pruned_json
@dataclass
class Explorer(Collection):
    """Model for Explorer configuration."""

    views: List[ExplorerView]
    config: Dict[str, str]
    definitions: Optional[Definitions] = None

    # Internal use. For save() method.
    _catalog_path: Optional[str] = None

    @property
    def catalog_path(self) -> Optional[str]:
        return self._catalog_path

    @catalog_path.setter
    def catalog_path(self, value: str) -> None:
        assert "#" in value, "Catalog path should be in the format `path#name`."
        self._catalog_path = value

    def display_config_names(self):
        """Get display names for all dimensions and choices.

        The structure of the output is:

        {
            dimension_slug: {
                "widget_name": "...",
                "choices": {
                    choice_slug: choice_name,
                    ...
                }
            },
            ...
        }

        where `widget_name` is actually not displayed anywhere, but used as header name in explorer config.
        """
        mapping = {}
        for dim in self.dimensions:
            dix = {
                "widget_name": f"{dim.name} {dim.ui_type.title()}",
                "choices": {choice.slug: choice.name for choice in dim.choices},
            }
            # Add checkbox_true if it is of type checkbox
            if dim.ui_type == "checkbox":
                assert dim.presentation is not None
                dix["checkbox_true"] = dix["choices"][dim.presentation.choice_slug_true]
            mapping[dim.slug] = dix
        return mapping

    @property
    def explorer_name(self):
        if self.catalog_path is None:
            raise ValueError("Catalog path is not set. Please set it before saving.")

        _, name = self.catalog_path.split("#")
        return name

    def sort_indicators(self, order: Union[List[str], Callable], indicators_slug: Optional[str] = None):
        """Sort indicators in all views."""
        if indicators_slug is None:
            indicators_slug = INDICATORS_SLUG
        self.sort_choices({"indicator": order})

    def save(
        self, owid_env: Optional[OWIDEnv] = None, tolerate_extra_indicators: bool = False, prune_dimensions: bool = True
    ):
        # Ensure we have an environment set
        if owid_env is None:
            owid_env = OWID_ENV

        if self.catalog_path is None:
            raise ValueError("Catalog path is not set. Please set it before saving.")

        # Prune non-used dimensions
        if prune_dimensions:
            self.prune_dimension_choices()

        # Check that no choice name is repeated
        self.validate_choice_names()

        # Check that all indicators in mdim exist
        indicators = self.indicators_in_use(tolerate_extra_indicators)
        validate_indicators_in_db(indicators, owid_env.engine)

        # TODO: Below code should be replaced at some point with DB-interaction code, as in `etl.collections.multidim.upsert_mdim_data_page`.
        # Extract Explorer view rows. NOTE: This is for compatibility with current Explorer config structure.
        df_grapher, df_columns = extract_explorers_tables(self)

        # Transform to legacy format
        # TODO: this part is responsible for interacting with owid-content. Instead, it should be replaced with DB-interaction code, as with MDIMs.
        explorer_legacy = _create_explorer_legacy(
            explorer_path=f"export://explorers/{self.catalog_path}",
            explorer_name=self.explorer_name,
            config=self.config,
            df_graphers=df_grapher,
            df_columns=df_columns,
        )

        explorer_legacy.save()


def create_explorer(
    config: dict,
    dependencies: Set[str],
) -> Explorer:
    """Create an explorer object."""
    # Read configuration as structured data
    explorer = Explorer.from_dict(config)

    # Edit views
    process_views(explorer, dependencies)

    # Validate config
    # explorer.validate_schema(SCHEMAS_DIR / "explorer-schema.json")

    # Ensure that all views are in choices
    explorer.validate_views_with_dimensions()

    # Validate duplicate views
    explorer.check_duplicate_views()

    return explorer


def process_views(
    explorer: Explorer,
    dependencies: Set[str],
):
    """Process views in Explorer configuration.

    TODO: See if we can converge to one solution with etl.collections.multidim.process_views.
    """
    # Get table information (table URI) by (i) table name and (ii) dataset_name/table_name
    tables_by_name = get_tables_by_name_mapping(dependencies)

    for view in explorer.views:
        # Expand paths
        view.expand_paths(tables_by_name)

        # Combine metadata/config with definitions.common_views
        if (explorer.definitions is not None) and (explorer.definitions.common_views is not None):
            view.combine_with_common(explorer.definitions.common_views)


def extract_explorers_tables(
    explorer: Explorer,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    1. Obtain `dimensions_display` dictionary. This helps later when remixing the Explorer configuration.
    2. Obtain `tables_by_name`: This helps in expanding the indicator paths if incomplete (e.g. table_name#short_name -> complete URI based on dependencies).
    3. Obtain `df_grapher`: This is the final DataFrame that will be saved as the Explorer dataset. It is basically a different presentation of the config
    """
    # 1. Prepare Dimension display dictionary
    dimensions_display = explorer.display_config_names()

    # 2. Remix configuration to generate explorer-friendly grapher and columns tables.
    df_grapher, df_columns = _extract_explorers_tables(
        explorer=explorer,
        dimensions_display=dimensions_display,
    )
    columns_widgets = [props["widget_name"] for _, props in dimensions_display.items()]

    # 3. Order views
    df_grapher = _order_explorer_views(
        df=df_grapher,
        dimensions_display=dimensions_display,
    )
    # 4. Adapt tables for view-level indicator display settings
    df_grapher, df_columns = _add_indicator_display_settings(df_grapher, df_columns, columns_widgets)

    # 5. Order columns
    df_grapher = _order_columns(df_grapher, columns_widgets)

    # 6. Set checkbox columns (if any) as boolean
    df_grapher = _set_checkbox_as_boolean(df_grapher, dimensions_display)

    # Drop dimension columns
    df_columns = df_columns.drop(columns=columns_widgets + ["_axis"])
    # # Drop All-NA rows
    # df_columns = df_columns.dropna(subset=[col for col in df_columns.columns if col != "catalogPath"], how="all")
    # Drop duplicates, if any
    df_columns = df_columns.drop_duplicates()

    # Sanity check (even if all-NA, we keep it because otherwise Grapher complains!)
    assert df_columns["catalogPath"].isna().all(), "catalogPath should be all NA in df_columns."

    return df_grapher, df_columns


def _extract_explorers_tables(
    explorer: Explorer, dimensions_display: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    records_grapher = []
    records_columns = []
    for view in explorer.views:
        # Build dimensions dictionary for a view
        dimensions = bake_dimensions_view(
            dimensions_display=dimensions_display,
            view=view,
        )

        # Get list of indicators with their paths & dimension
        indicator_paths = view.indicators.to_records()

        # Get indicators
        indicators = bake_indicators_view(indicator_paths)

        # Tweak view: TODO: add function Collection.add_view_config()
        # name = view["dimensions"]["metric"]
        # if name in RELATED:
        #     view["relatedQuestionText"] = RELATED[name]["text"]
        #     view["relatedQuestionUrl"] = RELATED[name]["link"]

        # Get config
        config = {}
        if view.config:
            config = {k: str(v).lower() if isinstance(v, bool) else v for k, v in view.config.items()}

        # Build record (grapher)
        record_grapher = {
            **indicators,
            **dimensions,
            **config,
        }

        # Build record (columns)
        record_columns = [
            {
                "catalogPath": item["path"],
                "_axis": item["axis"],
                **dimensions,
                **(item["display"] if isinstance(item["display"], dict) else {}),
            }
            for item in indicator_paths
        ]

        # Add records
        records_grapher.append(record_grapher)
        records_columns.extend(record_columns)

    # Build DataFrame with records
    df_grapher = pd.DataFrame.from_records(records_grapher)
    df_columns = pd.DataFrame.from_records(records_columns)

    return df_grapher, df_columns


def _add_indicator_display_settings(df_grapher, df_columns, columns_widgets):
    """Add indicator display settings.

    Since we want to support different display settings for the same indicator across different views, we need to use some 'hacks' (transform column, slugs instead of paths, etc.).

    TODO: transform operations can only be applied to indicator IDs (hence we need DB-access!).
    """

    def _create_mapping(group):
        # Check for duplicates across axis and catalogPath combinations
        if group.duplicated(subset=["_axis", "catalogPath"]).any():
            raise ValueError(f"Duplicate ('catalogPath', 'axis') found in group:\n{group}")

        nested_mapping = (
            group.groupby("_axis")[["catalogPath", "slug"]]
            .apply(lambda g: dict(zip(g["catalogPath"], g["slug"])))
            .to_dict()
        )

        return pd.Series({"_slug_renames": nested_mapping})

    ## Drop duplicates, if any
    df_columns = df_columns.drop_duplicates()

    # Drop those that do not have any settings set
    cols_settings = [col for col in df_columns.columns if col not in columns_widgets + ["_axis", "catalogPath"]]
    df_columns = df_columns.dropna(how="all", subset=cols_settings)

    # If there is more than one definition for the same indicator, proceed to adapt tables
    # mask = df_columns.duplicated(subset=["catalogPath"])
    if not df_columns.empty:
        # Assign ID to each row, based on whether the indicator config is the same. This helps us reduce unnecessary duplication of display settings.
        columns_subset = [col for col in df_columns.columns if col not in columns_widgets + ["_axis"]]
        df_columns.loc[:, "_slug_id"] = df_columns.groupby(columns_subset, dropna=False).ngroup()

        # 1. Tweak df_columns to have a row for all the different display settings of each indicator
        catalog_paths = df_columns["catalogPath"].unique().tolist()
        mapping = get_mapping_paths_to_id(catalog_paths)
        df_columns.loc[:, "_variableId"] = df_columns["catalogPath"].map(mapping)

        # Add unique identifier
        df_columns.loc[:, "slug"] = (
            df_columns["catalogPath"].apply(lambda x: underscore(x.replace("/", "__").replace("#", "__")))
            + "__"
            + df_columns["_slug_id"].astype(str)
        )
        # Add transform column
        df_columns.loc[:, "transform"] = "duplicate " + df_columns["_variableId"].astype(str)

        # 3. Tweak df_grapher
        # Get dictionary for re-mapping
        # Generate mapping
        mapping_series = (
            df_columns.groupby(columns_widgets)[["_axis", "catalogPath", "slug"]].apply(_create_mapping).reset_index()
        )
        # Merge mapping
        df_grapher = df_grapher.merge(mapping_series, on=columns_widgets, how="left")
        # Add ySlugs, xSlug, colorSlug or sizeSlug.
        columns_slugs = ["xSlug", "ySlugs", "colorSlug", "sizeSlug"]
        df_grapher[columns_slugs] = None
        # Iterate over affected rows, add slugs / remove paths
        mask_2 = df_grapher["_slug_renames"].notna()
        for idx, row in df_grapher[mask_2].iterrows():
            renames = row["_slug_renames"]

            for axis, renames_axis in renames.items():
                if axis == "y":
                    col_id = "yVariableIds"
                    col_slug = "ySlugs"
                    # Sanity check
                    assert (col_id in row) and isinstance(row[col_id], list) and (len(row[col_id]) >= 1)
                else:
                    col_id = f"{axis}VariableId"
                    col_slug = f"{axis}Slug"
                    # Sanity check
                    assert (col_id in row) and isinstance(row[col_id], list) and (len(row[col_id]) == 1)

                # Get new values
                slugs = [renames_axis.get(p) for p in row[col_id] if p in renames_axis]
                paths = [p for p in row[col_id] if p not in renames_axis]
                # Set new values
                df_grapher.at[idx, col_slug] = slugs if slugs != [] else np.nan
                df_grapher.at[idx, col_id] = paths if paths != [] else np.nan

        # 3. Finalize df_columns and df_grapher
        # COLUMNS
        # Reorder
        columns_first = ["catalogPath", "slug", "transform"]
        df_columns = df_columns[[*columns_first, *df_columns.columns.difference(columns_first)]]
        # Set catalogPath to None
        df_columns.loc[:, "catalogPath"] = None
        # Drop auxiliary columns
        df_columns = df_columns.drop(columns=["_variableId", "_slug_id"])

        # GRAPHER
        # Drop all-NA columns (TODO: does something happen if there is 'xSlug' but no 'xVariableId'? or any other axis?)
        cols_variables = df_grapher.filter(regex=r"(y|x|color|size)VariableIds?").columns
        df_grapher_ids = df_grapher[cols_variables].copy()
        df_grapher = df_grapher.dropna(how="all", axis=1)
        df_grapher.loc[:, cols_variables] = df_grapher_ids
        df_grapher = df_grapher.drop(columns=["_slug_renames"])

    return df_grapher, df_columns


def _order_columns(df, columns_widgets):
    columns_first = columns_widgets + [
        "yVariableIds",
        "ySlugs",
        "xVariableId",
        "xSlug",
        "colorVariableId",
        "colorSlug",
        "sizeVariableId",
        "sizeSlug",
    ]
    columns_first = [col for col in columns_first if col in df.columns]
    df = df[columns_first + [col for col in df.columns if col not in columns_first]]
    return df


def _order_explorer_views(df: pd.DataFrame, dimensions_display: Dict[str, Any]) -> pd.DataFrame:
    ## Order rows
    for _, properties in dimensions_display.items():
        column = properties["widget_name"]
        choices_ordered = list(properties["choices"].values())
        # Check if all DataFrame values exist in the predefined lists
        if not set(df[column]).issubset(set(choices_ordered)):
            raise ValueError(f"Column `{column}` contains values not present in `choices_ordered`.")

        # Convert columns to categorical with the specified order
        df[column] = pd.Categorical(df[column], categories=choices_ordered, ordered=True)

    df = df.sort_values(by=[d["widget_name"] for _, d in dimensions_display.items()])

    return df


def _set_checkbox_as_boolean(df: pd.DataFrame, dimensions_display: Dict[str, Any]) -> pd.DataFrame:
    for _, properties in dimensions_display.items():
        if "checkbox_true" in properties:
            column = properties["widget_name"]
            true_label = properties["checkbox_true"]
            df[column] = df[column] == true_label

    return df


def bake_dimensions_view(dimensions_display: Dict[str, Any], view) -> Dict[str, str]:
    """Cinfgure dimension details for an Explorer view.

    Given is dimension_slug: choice_slug. We need to convert it to dimension_name: choice_name (using dimensions_display).
    """
    view_dimensions = {}
    for slug_dim, slug_choice in view.dimensions.items():
        widget_name = dimensions_display[slug_dim]["widget_name"]

        # Checkbox
        # if "checkbox_true" in dimensions_display[slug_dim]:
        #     view_dimensions[widget_name] = slug_choice == dimensions_display[slug_dim]["checkbox_true"]
        # else:
        view_dimensions[widget_name] = dimensions_display[slug_dim]["choices"][slug_choice]
    return view_dimensions


def bake_indicators_view(indicator_paths) -> Dict[str, List[str]]:
    """Configure the indicator details for an Explorer view."""
    # Format them
    indicators = defaultdict(list)
    for indicator in indicator_paths:
        if indicator["axis"] == "y":
            indicators["yVariableIds"].append(indicator["path"])
            continue
        for dim in CHART_DIMENSIONS:
            if indicator["axis"] == dim:
                indicators[f"{dim}VariableId"].append(indicator["path"])
                break
    return indicators
