"""Load meadow dataset and create garden dataset with enhanced indicators."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset with additional calculated indicators."""
    # Load inputs.
    ds_meadow = paths.load_dataset("operating_data")

    # Load table
    tb = ds_meadow.read("operating_data")

    # Multiply values by 100 for categories that aren't shipments or capacity
    mask = ~tb["category"].isin(["shipments", "capacity"])
    tb.loc[mask, "value"] = tb.loc[mask, "value"] * 100

    tb["metric"] = tb["metric"].replace({"Smartphone": "Smartphones"})

    # Add country column for OWID's grapher
    tb["country"] = "Taiwan"
    # Set index with country
    tb = tb.format(["country", "date", "category", "metric"])

    tb = tb.drop(columns=["year"])

    # Save outputs.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
