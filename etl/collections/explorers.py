from collections import defaultdict
from typing import Dict, List, Optional, Set

import pandas as pd

from etl.collections.common import validate_collection_config
from etl.collections.model import CHART_DIMENSIONS, Explorer
from etl.collections.utils import (
    get_tables_by_name_mapping,
)
from etl.config import OWID_ENV, OWIDEnv
from etl.helpers import PathFinder
from etl.helpers import create_explorer as create_explorer_main


def create_explorer(
    dest_dir: str,
    config: dict,
    paths: PathFinder,
    owid_env: Optional[OWIDEnv] = None,
    tolerate_extra_indicators: bool = False,
):
    """TODO: Replicate `etl.collections.multidim.upsert_mdim_data_page`."""
    # Read configuration as structured data
    explorer = Explorer.from_dict(config)

    # Edit views
    process_views(explorer, paths.dependencies)

    # Create explorer (TODO: this should rather push to DB! As in with `etl.collections.multidim.upsert_mdim_data_page`)
    return _create_explorer(dest_dir, explorer, tolerate_extra_indicators, owid_env)


def process_views(
    explorer: Explorer,
    dependencies: Set[str],
):
    """Process views in Explorer configuration.

    TODO: See if we can converge to one solution with etl.collections.multidim.process_views.
    """
    # Get table information by table name, and table URI
    tables_by_name = get_tables_by_name_mapping(dependencies)

    for view in explorer.views:
        # Expand paths
        view.expand_paths(tables_by_name)

        # Combine metadata/config with definitions.common_views
        if (explorer.definitions is not None) and (explorer.definitions.common_views is not None):
            view.combine_with_common(explorer.definitions.common_views)


def _create_explorer(
    dest_dir: str,
    explorer: Explorer,
    tolerate_extra_indicators: bool,
    owid_env: Optional[OWIDEnv] = None,
):
    # Ensure we have an environment set
    if owid_env is None:
        owid_env = OWID_ENV

    # Validate config
    # TODO: explorer.validate_schema(SCHEMAS_DIR / "explorer-schema.json")
    validate_collection_config(explorer, owid_env.engine, tolerate_extra_indicators)

    # TODO: Below code should be replaced at some point with DB-interaction code, as in `etl.collections.multidim.upsert_mdim_data_page`.
    # Extract Explorer view rows. NOTE: This is for compatibility with current Explorer config structure.
    df_grapher = extract_explorer_views(explorer)

    # Create explorer
    ds = create_explorer_main(
        dest_dir=dest_dir,
        config=explorer.config,
        df_graphers=df_grapher,
    )

    return ds


def extract_explorer_views(
    explorer: Explorer,
) -> pd.DataFrame:
    """
    1. Obtain `dimensions_display` dictionary. This helps later when remixing the Explorer configuration.
    2. Obtain `tables_by_name`: This helps in expanding the indicator paths if incomplete (e.g. table_name#short_name -> complete URI based on dependencies).
    3. Obtain `df_grapher`: This is the final DataFrame that will be saved as the Explorer dataset. It is basically a different presentation of the config
    """
    # 1. Prepare Dimension display dictionary
    dimensions_display = explorer.display_config_names()

    # 2. Remix configuration to generate explorer-friendly graphers table.
    records = []
    for view in explorer.views:
        # Build dimensions dictionary for a view
        dimensions = bake_dimensions_view(
            dimensions_display=dimensions_display,
            view=view,
        )

        # Get indicators
        indicators = bake_indicators_view(view)

        # Tweak view: TODO: add function Collection.add_view_config()
        # name = view["dimensions"]["metric"]
        # if name in RELATED:
        #     view["relatedQuestionText"] = RELATED[name]["text"]
        #     view["relatedQuestionUrl"] = RELATED[name]["link"]

        # Get config
        config = {}
        if view.config:
            config = {k: str(v).lower() if isinstance(v, bool) else v for k, v in view.config.items()}

        # Build record
        record = {
            **indicators,
            **dimensions,
            **config,
        }

        # Add record
        records.append(record)

    # Build DataFrame with records
    df_grapher = pd.DataFrame.from_records(records)
    return df_grapher


def bake_dimensions_view(dimensions_display, view) -> Dict[str, str]:
    """Cinfgure dimension details for an Explorer view.

    Given is dimension_slug: choice_slug. We need to convert it to dimension_name: choice_name (using dimensions_display).
    """
    view_dimensions = {}
    for slug_dim, slug_choice in view.dimensions.items():
        widget_name = dimensions_display[slug_dim]["widget_name"]
        view_dimensions[widget_name] = dimensions_display[slug_dim]["choices"][slug_choice]
    return view_dimensions


def bake_indicators_view(view) -> Dict[str, List[str]]:
    """Configure the indicator details for an Explorer view."""
    # Get list of indicators with their paths & dimension
    indicator_paths = view.indicators.to_records()
    # Format them
    indicators = defaultdict(list)
    for indicator in indicator_paths:
        for dim in CHART_DIMENSIONS:
            if indicator["dimension"] == "y":
                indicators[f"{dim}VariableIds"].append(indicator["path"])
                break
            if indicator["dimension"] == dim:
                indicators[f"{dim}VariableId"].append(indicator["path"])
                break
    return indicators
