"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("marriages")

    # Read table from garden dataset.
    tb = ds_garden.read("marriages", reset_index=True)

    # Filter to keep only years 1900, 1910, 1920, etc.
    tb = tb[tb["year"] % 10 == 0]

    tb = tb.rename(columns={"year": "birth_cohort", "age": "year", "gender": "country"})

    tb = tb.format(["country", "birth_cohort", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
