"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("nasa_budget")

    # Read table from meadow dataset.
    tb = ds_meadow.read("nasa_budget")

    #
    # Process data.
    #
    error = "Units of dollars have changed."
    assert set(tb["type"]) == set(["FY20 Dollars", "Then-Year Dollars"]), error

    # Select constant dollars.
    tb = tb[tb["type"] == "FY20 Dollars"].drop(columns=["type"]).reset_index(drop=True)

    # Convert from millions to dollars.
    tb["value"] = tb["value"].astype("Float64") * 1e6

    # For now we will only keep the total annual budget, and ignore categories.
    tb = tb.groupby("year", as_index=False).agg({"value": "sum"}).rename(columns={"value": "budget"}, errors="raise")

    # Add a country column.
    tb["country"] = "United States"

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
