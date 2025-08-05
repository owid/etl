"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("air_traffic")

    # Read table from meadow dataset.
    tb = ds_meadow.read("air_traffic")

    #
    # Process data.
    #
    # Convert columns ending in '__mils' to million
    for col in tb.columns[tb.columns.str.endswith("__mils")]:
        tb[col.replace("__mils", "")] = tb[col] * 1000000
        tb = tb.drop(columns=[col])

    # Convert columns ending in '__000' to thousand
    for col in tb.columns[tb.columns.str.endswith("__000")]:
        tb[col.replace("__000", "")] = tb[col] * 1000
        tb = tb.drop(columns=[col])

    # Convert passenger load factor from fraction to percentage
    tb["plf"] = tb["plf"] * 100
    tb["plf_empty"] = 100 - tb["plf"]

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
