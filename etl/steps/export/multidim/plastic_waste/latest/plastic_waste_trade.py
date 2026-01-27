"""MDIM step for plastic waste trade data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "chartTypes": ["LineChart"],
}

# Define dimensions for all variables
DIMENSIONS_DETAILS = {
    # Exports
    "export_total_mot": {
        "metric": "exports",
        "rate": "total",
        "original_short_name": "plastic_waste_trade",
    },
    "export_total_mot_per_capita": {
        "metric": "exports",
        "rate": "per_capita",
        "original_short_name": "plastic_waste_trade",
    },
    # Imports
    "import_total_mot": {
        "metric": "imports",
        "rate": "total",
        "original_short_name": "plastic_waste_trade",
    },
    "import_total_mot_per_capita": {
        "metric": "imports",
        "rate": "per_capita",
        "original_short_name": "plastic_waste_trade",
    },
    # Net exports
    "net_export": {
        "metric": "net_exports",
        "rate": "total",
        "original_short_name": "plastic_waste_trade",
    },
    "net_export_per_capita": {
        "metric": "net_exports",
        "rate": "per_capita",
        "original_short_name": "plastic_waste_trade",
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
    tb = ds_grapher.read("plastic_waste_2023_2024", reset_index=False)

    #
    # Process data.
    #
    # Add dimension metadata to columns based on their measure type
    for col, details in DIMENSIONS_DETAILS.items():
        if col in tb.columns:
            # Set dimensions (all keys except original_short_name)
            tb[col].m.dimensions = {k: v for k, v in details.items() if k != "original_short_name"}
            # Set original_short_name if present
            if "original_short_name" in details:
                tb[col].m.original_short_name = details["original_short_name"]

    # Create collection - this will automatically generate views from dimensions
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["plastic_waste_trade"],
        dimensions=["metric", "rate"],
        common_view_config=MULTIDIM_CONFIG,
    )

    #
    # Save outputs.
    #
    c.save()
