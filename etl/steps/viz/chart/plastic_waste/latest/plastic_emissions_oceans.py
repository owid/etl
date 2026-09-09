"""MDIM step for plastic emissions into ocean via rivers."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "chartTypes": ["StackedDiscreteBar"],
}

# Indicator name constant
INDICATOR_NAME = "plastic_emissions_rivers"

# Define dimensions for all variables
DIMENSIONS_DETAILS = {
    "me_tons_per_year": {
        "dimensions": {
            "measure": "total",
        },
    },
    "me_tons_per_year_per_capita": {
        "dimensions": {
            "measure": "per_capita",
        },
    },
    "me_tons_per_year_share_of_global": {
        "dimensions": {
            "measure": "share_of_global",
        },
    },
}


def run() -> None:
    """
    Main function to process plastic emissions data and create multidimensional data views.
    """
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file
    config = paths.load_collection_config()

    # Load grapher dataset
    ds_grapher = paths.load_dataset("meijer_2021")
    tb = ds_grapher.read("meijer_2021", load_data=False)

    #
    # Process data.
    #
    # Add dimension metadata to columns based on their measure type
    for col, details in DIMENSIONS_DETAILS.items():
        if col in tb.columns:
            # Set dimensions
            tb[col].m.dimensions = details["dimensions"]
            # Set original_short_name
            tb[col].m.original_short_name = INDICATOR_NAME

    # Create collection - this will automatically generate views from dimensions
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=[INDICATOR_NAME],
        dimensions=["measure"],
        common_view_config=MULTIDIM_CONFIG,
    )

    #
    # Save outputs.
    #
    c.save()
