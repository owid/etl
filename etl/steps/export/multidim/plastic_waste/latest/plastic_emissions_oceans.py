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

# Define dimensions for all variables
DIMENSIONS_DETAILS = {
    "me_tons_per_year": {
        "measure": "total",
        "original_short_name": "plastic_emissions_rivers",
    },
    "me_tons_per_year_per_capita": {
        "measure": "per_capita",
        "original_short_name": "plastic_emissions_rivers",
    },
    "me_tons_per_year_share_of_global": {
        "measure": "share_of_global",
        "original_short_name": "plastic_emissions_rivers",
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
    tb = ds_grapher.read("meijer_2021", reset_index=False)

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
        indicator_names=["plastic_emissions_rivers"],
        dimensions=["measure"],
        common_view_config=MULTIDIM_CONFIG,
    )

    #
    # Save outputs.
    #
    c.save()
