import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from etl.collections.common import expand_config
from etl.collections.explorer_legacy import _create_explorer_legacy
from etl.collections.model import CHART_DIMENSIONS, Collection, Definitions, ExplorerView, pruned_json
from etl.collections.utils import (
    get_tables_by_name_mapping,
    validate_indicators_in_db,
)
from etl.config import OWID_ENV, OWIDEnv


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
            mapping[dim.slug] = {
                "widget_name": f"{dim.name} {dim.ui_type.title()}",
                "choices": {choice.slug: choice.name for choice in dim.choices},
            }
        return mapping

    @property
    def explorer_name(self):
        if self.catalog_path is None:
            raise ValueError("Catalog path is not set. Please set it before saving.")

        _, name = self.catalog_path.split("#")
        return name

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


__all__ = ["expand_config"]


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

    # 2. Remix configuration to generate explorer-friendly graphers table.
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
                "axis": item["axis"],
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

    # Order views
    ## Order rows
    for _, properties in dimensions_display.items():
        column = properties["widget_name"]
        choices_ordered = list(properties["choices"].values())
        # Check if all DataFrame values exist in the predefined lists
        if not set(df_grapher[column]).issubset(set(choices_ordered)):
            raise ValueError(f"Column `{column}` contains values not present in `choices_ordered`.")

        # Convert columns to categorical with the specified order
        df_grapher[column] = pd.Categorical(df_grapher[column], categories=choices_ordered, ordered=True)
    df_grapher = df_grapher.sort_values(by=[d["widget_name"] for _, d in dimensions_display.items()])

    ## Order columns
    cols_widgets = [d["widget_name"] for _, d in dimensions_display.items()]
    df_grapher = df_grapher[cols_widgets + [col for col in df_grapher.columns if col not in cols_widgets]]

    # TODO: Adjust df_grapher and df_columns to allow for multiple displays per-view
    # How?
    # Columns: Use transform=`duplicate <indicator_uri>` and slug=`<indicator_unique_identifier>`
    # Grapher: Reference the specific "tweaked" indicator via ySlugs, xSlug, colorSlug or sizeSlug.
    # reference: https://ourworldindata.org/war-and-peace-data-explorers (row ~318 in config)

    # Drop dimension columns
    pattern = re.compile(r"^.*\s(Dropdown|Radio|Checkbox)$")
    drop_columns = [col for col in df_columns.columns if pattern.match(col)]
    df_columns = df_columns.drop(columns=drop_columns + ["axis"])

    # Drop All-NA rows
    df_columns = df_columns.dropna(subset=[col for col in df_columns.columns if col != "catalogPath"], how="all")

    # Drop duplicates, if any
    df_columns = df_columns.drop_duplicates()

    # Sanity check
    mask = df_columns.duplicated(subset=["catalogPath"])
    if mask.any():
        raise ValueError(
            f"Different display settings cannot be set for the same indicators. We are working to make this possible so that you can define different settings in different views. This is possible in MDIMs already! Review indicators: {df_columns.loc[mask, 'catalogPath'].tolist()}"
        )
    return df_grapher, df_columns


def bake_dimensions_view(dimensions_display, view) -> Dict[str, str]:
    """Cinfgure dimension details for an Explorer view.

    Given is dimension_slug: choice_slug. We need to convert it to dimension_name: choice_name (using dimensions_display).
    """
    view_dimensions = {}
    for slug_dim, slug_choice in view.dimensions.items():
        widget_name = dimensions_display[slug_dim]["widget_name"]
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
