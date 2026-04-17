"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("us_egg_production")
    tb = ds_meadow.read("us_egg_production")

    #
    # Process data.
    #
    # Convert millions of hens to hens.
    hen_columns = [col for col in tb.columns if col != "year"]
    for col in hen_columns:
        tb[col] = tb[col] * 1e6

    # Add country column.
    tb["country"] = "United States"

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save new garden dataset.
    ds_garden.save()
