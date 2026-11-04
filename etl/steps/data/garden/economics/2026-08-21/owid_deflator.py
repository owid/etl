"""OWID Deflator dataset.

This is meant to be an auxiliary dataset with indicators commonly used to do inflation adjustments.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load WDI auxiliary dataset with prices-related indicators, and read its table.
    ds_wdi = paths.load_dataset("wdi_prices")
    tb_wdi = ds_wdi.read("wdi_prices")

    #
    # Process data.
    #
    # Combine tables.
    tb = tb_wdi.copy()

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_wdi.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
