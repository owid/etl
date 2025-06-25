from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import pandas as pd
from owid.catalog.utils import underscore
from sqlalchemy.orm import Session
from typing_extensions import Self

import etl.grapher.model as gm
from etl.collection.explorer.legacy import create_explorer_legacy
from etl.collection.model import Collection
from etl.collection.model.base import pruned_json
from etl.collection.utils import CHART_DIMENSIONS, INDICATORS_SLUG
from etl.config import OWID_ENV, OWIDEnv
from etl.paths import EXPORT_EXPLORER_DIR


@pruned_json
@dataclass
class Explorer(Collection):
    """Model for Explorer configuration."""

    config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """Coerce the dictionary into the expected shape before passing it to the parent class."""
        # Make a shallow copy so we don't mutate the user's dictionary in-place
        data = dict(d)

        # If dictionary contains field 'definitions', change it for '_definitions'
        if "title" not in data:
            data["title"] = {}
        if "default_selection" not in data:
            data["default_selection"] = []

        if "config" not in data:
            raise ValueError("Missing 'config' key in the dictionary.")

        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)

    def __post_init__(self):
        """We set it here because of simplicity.

        Adding a class attribute like `_collection_type: Optional[str] = "explorer"` leads to error `TypeError: non-default argument 'config' follows default argument`.
        Alternative would be to define the class attribute like `_collection_type: Optional[str] = field(init=False, default="explorer")` but feels a bit redundant with parent definition.
        """
        self._collection_type = "explorer"

    @property
    def local_config_path(self) -> Path:
        # energy/latest/energy_prices#energy_prices -> export/multidim/energy/latest/energy_prices/config.yml
        assert self.catalog_path
        if self._collection_type is None:
            raise ValueError("_collection_type must have a value!")
        return EXPORT_EXPLORER_DIR / (self.catalog_path.replace("#", "/") + ".config.json")

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

    def sort_indicators(self, order: List[str] | Callable, indicators_slug: str | None = None):
        """Sort indicators in all views."""
        if indicators_slug is None:
            indicators_slug = INDICATORS_SLUG
        self.sort_choices({"indicator": order})

    def upsert_to_db(self, owid_env: OWIDEnv):
        # TODO: Below code should be replaced at some point with DB-interaction code.
        # Extract Explorer view rows. NOTE: This is for compatibility with current Explorer config structure.
        df_grapher, df_columns = extract_explorers_tables(self)

        # Transform to legacy format
        explorer_legacy = create_explorer_legacy(
            catalog_path=self.catalog_path,
            explorer_name=self.short_name,
            config=self.config,
            df_graphers=df_grapher,
            df_columns=df_columns,
        )

        explorer_legacy.save(owid_env)

    # @classmethod
    # def from_dict(cls, d: Dict[str, Any]) -> T:


###################################################
# CODE TO EXTRACT TSV EXPLORER TABLES
# TODO: Maybe we can move this to explorer_legacy?
###################################################
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
    df_grapher, df_columns = _add_indicator_display_settings(
        df_grapher,
        df_columns,
        columns_widgets,
    )

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
    if "slug" in df_columns:
        mask = df_columns["catalogPath"].isna()
        assert df_columns.loc[mask, "slug"].notna().all(), "`slug` must be set whenever `catalogPath` is missing."
        assert (
            df_columns.loc[mask, "transform"].notna().all()
        ), "`transform` must be set whenever `catalogPath` is missing."

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


def get_mapping_paths_to_id(catalog_paths: List[str], owid_env: OWIDEnv | None = None) -> Dict[str, str]:
    # Check if given path is actually an ID
    # Get ID, assuming given path is a catalog path
    if owid_env is None:
        engine = OWID_ENV.engine
    else:
        engine = owid_env.engine
    with Session(engine) as session:
        db_indicators = gm.Variable.from_id_or_path(session, catalog_paths)  # type: ignore
        # scores = dict(zip(catalog_paths, range(len(catalog_paths))))
        # db_indicators.sort(key=lambda x: scores[x.catalogPath], reverse=True)
        return {indicator.catalogPath: indicator.id for indicator in db_indicators}


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

    # NOTE: I've commented the following lines because it was dropping rows where no metadata was set, and we want to keep these actually! Otherwise these are not properly assigned a _slug_id!
    # Drop those that do not have any settings set
    # cols_settings = [col for col in df_columns.columns if col not in columns_widgets + ["_axis", "catalogPath"]]
    # df_columns = df_columns.dropna(how="all", subset=cols_settings)

    # If there is more than one definition for the same indicator, proceed to adapt tables
    # mask = df_columns.duplicated(subset=["catalogPath"])
    if not df_columns.empty:
        # Assign ID to each row, based on whether the indicator config is the same. This helps us reduce unnecessary duplication of display settings.
        columns_subset = [col for col in df_columns.columns if col not in columns_widgets + ["_axis", "catalogPath"]]
        # df_columns.loc[:, "_slug_id"] = df_columns.groupby(columns_subset, dropna=False).ngroup()
        # Generate IDs per 'catalogPath' context, based on identical values in column_subset
        df_columns["_slug_id"] = df_columns.groupby(["catalogPath"] + columns_subset, sort=False, dropna=False).ngroup()
        # Optionally, reset IDs per catalogPath context (making them start from zero in each catalogPath)
        df_columns["_slug_id"] = df_columns.groupby("catalogPath")["_slug_id"].transform(lambda x: pd.factorize(x)[0])

        # Are there more than one indicator settings?
        # A) YES: Add slugs to df_columns and df_grapher!
        # B) NO: No need to add slug and all related complexity (skip)
        duplicate_indicators = df_columns["_slug_id"] != 0
        if duplicate_indicators.any():
            # 1. Get indicator IDs for duplicates! The way a duplicate works, is by referencing the variable ID (does not work with catalogPath). E.g. `duplicate 123`.
            catalog_paths = df_columns.loc[duplicate_indicators, "catalogPath"].unique().tolist()
            mapping = get_mapping_paths_to_id(catalog_paths)
            # Debugging
            # keys = set(catalog_paths)
            # values = [int(i) for i in range(1, len(keys))]
            # mapping = dict(zip(keys, values))
            df_columns.loc[duplicate_indicators, "_variableId"] = df_columns.loc[
                duplicate_indicators, "catalogPath"
            ].map(mapping)
            df_columns = df_columns.astype({"_variableId": "Int64"})

            # 2. Add unique indicator identifier (will be used as slug)
            df_columns.loc[duplicate_indicators, "slug"] = (
                df_columns.loc[duplicate_indicators, "catalogPath"].apply(
                    lambda x: underscore(x.replace("/", "__").replace("#", "__"))
                )
                + "__"
                + df_columns.loc[duplicate_indicators, "_slug_id"].astype(str)
            )

            # Add transform column
            df_columns.loc[duplicate_indicators, "transform"] = "duplicate " + df_columns["_variableId"].astype(str)

            # 3. Tweak df_grapher
            # Get dictionary for re-mapping
            mapping_series = (
                df_columns[duplicate_indicators]
                .groupby(columns_widgets)[["_axis", "catalogPath", "slug"]]
                .apply(_create_mapping)
                .reset_index()
            )
            # Merge mapping
            df_grapher = df_grapher.merge(mapping_series, on=columns_widgets, how="left")
            # Add ySlugs, xSlug, colorSlug or sizeSlug.
            columns_slugs = ["xSlug", "ySlugs", "colorSlug", "sizeSlug"]
            df_grapher[columns_slugs] = None
            # Iterate over affected rows, add slugs, AND set affected *VariableIds to NaN
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

            # Set catalogPath to None where applicable (where a slug is in use!)
            df_columns.loc[duplicate_indicators, "catalogPath"] = None

        # 3. Finalize df_columns and df_grapher
        # 3.1 COLUMNS table
        # Drop auxiliary columns
        cols_aux = ["_variableId", "_slug_id"]
        df_columns = df_columns.drop(columns=[col for col in cols_aux if col in df_columns.columns])
        # Reorder columns
        columns_first = ["catalogPath", "slug", "transform"]
        columns_first = [col for col in columns_first if col in df_columns.columns]
        df_columns = df_columns[[*columns_first, *df_columns.columns.difference(columns_first)]]

        # 3.2 GRAPHER table
        # Drop all-NA columns (TODO: does something happen if there is 'xSlug' but no 'xVariableId'? or any other axis?)
        ## Temporary copy *VariableIds in case we remove any of these columns, but we don't want?
        cols_variables = df_grapher.filter(regex=r"(y|x|color|size)VariableIds?").columns
        df_grapher_ids = df_grapher[cols_variables].copy()
        df_grapher = df_grapher.dropna(how="all", axis=1)  # Probably some *Slug columns are dropped here
        df_grapher.loc[:, cols_variables] = df_grapher_ids
        if "_slug_renames" in df_grapher.columns:
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
    """Configure dimension details for an Explorer view.

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
