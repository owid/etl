"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("plastic_waste_2019_by_polymer")

    # Read table from garden dataset.
    tb = ds_garden["plastic_waste_2019"]

    #
    # Process data.
    #
    tb = tb.reset_index()
    # Replace application type to be the country column for visualisation purposes
    tb = tb.rename(columns={"country": "entity"})

    tb = tb.rename(columns={"polymer": "country"})
    tb = tb.set_index(["country", "year", "entity"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
