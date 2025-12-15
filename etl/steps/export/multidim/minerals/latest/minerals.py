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
COLUMNS_WITHOUT_MAP_TAB = [
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
]


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
    min_year_by_column = {}

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

        # Clean up sub_commodity for display
        # NOTE: Unit handling - remove "(tonnes)" suffix where it appears
        if unit.startswith("tonnes"):
            sub_commodity_display = sub_commodity
        else:
            sub_commodity_display = sub_commodity

        # Set dimensions on column metadata
        tb[column].m.dimensions = {
            "mineral": underscore(commodity),
            "metric": underscore(metric),
            "type": underscore(sub_commodity),
            "measure": measure,
        }
        tb[column].m.original_short_name = "value"

        # Determine minTime based on data availability (sparse data -> bar chart)
        if (years.max() - years.min()) < 5:
            # If there are only a few data points, show only the latest year (as a bar chart).
            min_year_by_column[column] = int(years.max())
        else:
            # Otherwise, show all years (as a line chart).
            min_year_by_column[column] = int(years.min())

    # Common view configuration
    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart"],
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
    )

    # Post-processing: Apply custom view configurations
    for view in c.views:
        # Get the indicator catalog path to identify the column
        if view.indicators.y:
            catalog_path = view.indicators.y[0].catalogPath
            # Extract column name from catalog path (format: uri#column)
            column = catalog_path.split("#")[-1] if "#" in catalog_path else None

            if column:
                # Disable map tab for specific columns with sparse geographic data
                if column in COLUMNS_WITHOUT_MAP_TAB:
                    view.config["hasMapTab"] = False

                # Disable map tab for "Unit value" metric
                if view.dimensions.get("metric") == "unit_value":
                    view.config["hasMapTab"] = False

                # Set minTime for sparse data (to show bar chart instead of line chart with few points)
                if column in min_year_by_column:
                    view.config["minTime"] = min_year_by_column[column]

    #
    # Save outputs.
    #
    c.save()
