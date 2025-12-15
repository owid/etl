"""Load a grapher dataset and create a multidim explorer."""

from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix used for "share" columns.
# NOTE: This must coincide with the same variable as defined in the garden minerals step.
SHARE_OF_GLOBAL_PREFIX = "share of global "

# Columns where map tab should be disabled due to sparse data.
COLUMNS_WITHOUT_MAP_TAB = {
    "production_cesium_mine_tonnes",
    "production_diamond_mine_and_synthetic__industrial_tonnes",
    "share_of_global_production_diamond_mine_and_synthetic__industrial_tonnes",
    "reserves_kyanite_mine__kyanite_and_sillimanite_tonnes",
    "production_soda_ash_synthetic_tonnes",
    "reserves_zeolites_mine_tonnes",
    "production_mica_mine__sheet_tonnes",
    "share_of_global_production_mica_mine__sheet_tonnes",
    "production_bismuth_mine_tonnes",
    "share_of_global_production_bismuth_mine_tonnes",
    "production_boron_mine_tonnes",
    "share_of_global_production_boron_mine_tonnes",
    "production_gallium_refinery_tonnes",
    "share_of_global_production_gallium_refinery_tonnes",
    "production_sand_and_gravel_mine__construction_tonnes",
    "share_of_global_production_sand_and_gravel_mine__construction_tonnes",
}


def _get_column_from_view(view):
    """Extract column name from view's indicator catalog path."""
    if view.indicators.y:
        catalog_path = view.indicators.y[0].catalogPath
        if "#" in catalog_path:
            return catalog_path.split("#")[-1]
    return None


def run() -> None:
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # Load minerals grapher dataset and read its main table.
    ds = paths.load_dataset("minerals")
    tb = ds.read("minerals")

    #
    # Process data.
    #
    # Track data for sparse-data handling (bar chart vs line chart).
    # Only store minTime for columns with sparse data (<5 years range).
    sparse_data_min_year = {}

    # Track choice renames for mineral and type dimensions (slug -> display name).
    mineral_names: dict[str, str] = {}
    type_names: dict[str, str] = {}

    for column in tb.drop(columns=["country", "year"]).columns:
        # Check if column has data
        years = tb["year"][tb[column].notnull()]
        if len(years) == 0:
            continue

        # Parse metadata title: metric|commodity|sub_commodity|unit
        metric, commodity, sub_commodity, unit = tb[column].metadata.title.split("|")

        # Handle "share of global" prefix
        if metric.startswith(SHARE_OF_GLOBAL_PREFIX):
            metric = metric.replace(SHARE_OF_GLOBAL_PREFIX, "")
            measure = "share_of_global"
        else:
            measure = "absolute"

        # Clean up metric name
        metric = metric.replace("_", " ").lower()

        # Create slugs for dimensions
        mineral_slug = underscore(commodity)
        type_slug = underscore(sub_commodity)

        # Track original names for display (slug -> proper display name)
        mineral_names[mineral_slug] = commodity
        type_names[type_slug] = sub_commodity

        # Set dimensions on column metadata
        tb[column].m.dimensions = {
            "mineral": mineral_slug,
            "metric": underscore(metric),
            "type": type_slug,
            "measure": measure,
        }
        tb[column].m.original_short_name = "value"

        # Only set minTime for sparse data (to show bar chart instead of line chart with few points)
        if (years.max() - years.min()) < 5:
            sparse_data_min_year[column] = int(years.max())

    # Common view configuration
    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart", "DiscreteBar"],
        "hasMapTab": True,
        "yAxis": {"min": 0},
    }

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["value"],
        dimensions=["mineral", "metric", "type", "measure"],
        common_view_config=common_view_config,
        choice_renames={
            "mineral": mineral_names,
            "type": type_names,
        },
    )

    # Helper function to determine if map tab should be shown
    def _has_map_tab(view):
        column = _get_column_from_view(view)
        # Disable for specific columns with sparse geographic data
        if column in COLUMNS_WITHOUT_MAP_TAB:
            return False
        # Disable for "Unit value" metric
        if view.dimensions.get("metric") == "unit_value":
            return False
        return True

    # Helper function to get minTime for sparse data
    def _get_min_time(view):
        column = _get_column_from_view(view)
        # Only set minTime for sparse data columns
        if column in sparse_data_min_year:
            return sparse_data_min_year[column]
        # Return None to not set minTime (use default behavior)
        return None

    # Apply dynamic config using set_global_config with lambdas
    c.set_global_config(
        {
            "hasMapTab": _has_map_tab,
            "minTime": _get_min_time,
        }
    )

    #
    # Save outputs.
    #
    c.save()
