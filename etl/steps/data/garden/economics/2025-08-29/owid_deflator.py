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
    # Load WDI auxiliary dataset for Consumer Price Index, and read its table.
    ds_wdi_cpi = paths.load_dataset("wdi_cpi")
    tb_wdi_cpi = ds_wdi_cpi.read("wdi_cpi")

    #
    # Process data.
    #
    # Combine tables.
    # TODO: Include other helpful tables.
    tb = tb_wdi_cpi.copy()

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_wdi_cpi.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
