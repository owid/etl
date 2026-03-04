"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ghsl_countries")

    # Read table from garden dataset.
    tb = ds_garden["ghsl_countries"]
    tb_dominant_urbanization_level = ds_garden["ghsl_countries_dominant_type"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.

    ds_grapher = paths.create_dataset(
        tables=[tb, tb_dominant_urbanization_level], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
