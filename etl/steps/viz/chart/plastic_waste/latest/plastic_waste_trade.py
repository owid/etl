"""MDIM step for plastic waste trade data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "chartTypes": ["LineChart", "DiscreteBar"],
}

# Indicator name constant
INDICATOR_NAME = "plastic_waste_trade"

# Define dimensions for all variables
DIMENSIONS_DETAILS = {
    # Exports
    "export_total_mot": {
        "dimensions": {
            "metric": "exports",
            "rate": "total",
        },
    },
    "export_total_mot_per_capita": {
        "dimensions": {
            "metric": "exports",
            "rate": "per_capita",
        },
    },
    # Imports
    "import_total_mot": {
        "dimensions": {
            "metric": "imports",
            "rate": "total",
        },
    },
    "import_total_mot_per_capita": {
        "dimensions": {
            "metric": "imports",
            "rate": "per_capita",
        },
    },
    # Net exports
    "net_export": {
        "dimensions": {
            "metric": "net_exports",
            "rate": "total",
        },
    },
    "net_export_per_capita": {
        "dimensions": {
            "metric": "net_exports",
            "rate": "per_capita",
        },
    },
}


def run() -> None:
    """
    Main function to process plastic waste trade data and create multidimensional data views.
    """
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file
    config = paths.load_collection_config()

    # Load grapher dataset
    ds_grapher = paths.load_dataset("plastic_waste_2023_2024")
    tb = ds_grapher.read("plastic_waste_2023_2024", load_data=False)

    #
    # Process data.
    #
    # Filter to only the columns we need for the MDim
    cols_to_keep = list(DIMENSIONS_DETAILS.keys())
    tb = tb[cols_to_keep]

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
        dimensions=["metric", "rate"],
        common_view_config=MULTIDIM_CONFIG,
    )

    #
    # Save outputs.
    #
    c.save()
