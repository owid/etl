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

    # Properly managed waste - total
    if "pwg" in tb.columns:
        tb["pwg"].m.dimensions = {
            "measure": "total",
        }
        tb["pwg"].m.original_short_name = "waste_generation"

    # Properly managed waste - per person
    if "pwg_per_cap" in tb.columns:
        tb["pwg_per_cap"].m.dimensions = {
            "measure": "per_person",
        }
        tb["pwg_per_cap"].m.original_short_name = "waste_generation"

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
