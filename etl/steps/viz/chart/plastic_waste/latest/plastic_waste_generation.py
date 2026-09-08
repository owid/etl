"""MDIM step for plastic waste generation data."""

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
    "pwg": {
        "measure": "total",
        "original_short_name": "waste_generation",
    },
    "pwg_per_cap": {
        "measure": "per_person",
        "original_short_name": "waste_generation",
    },
}


def run() -> None:
    """
    Main function to process plastic waste generation data and create multidimensional data views.
    """
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file
    config = paths.load_collection_config()

    # Load grapher dataset
    ds_grapher = paths.load_dataset("cottom_plastic_waste")
    tb = ds_grapher.read("cottom_plastic_waste", reset_index=False)

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
        indicator_names=["waste_generation"],
        dimensions=["measure"],
        common_view_config=MULTIDIM_CONFIG,
    )

    #
    # Save outputs.
    #
    c.save()
