"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("plastic_environment_leakage_projections")

    # Read table from garden dataset.
    tb = ds_garden["plastic_environment_leakage_projections"]

    #
    # Process data.
    #
    tb = tb.reset_index()

    # Replace scenario type to be the country column for visualisation purposes
    tb = tb.drop("country", axis=1)
    tb = tb.rename(columns={"scenario_type": "country"})
    tb = tb.set_index(["year", "country", "plastic_type"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
