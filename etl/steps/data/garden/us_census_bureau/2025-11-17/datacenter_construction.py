"""Garden step for datacenter construction spending data."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("datacenter_construction")

    # Read table from meadow dataset.
    tb = ds_meadow.read("datacenter_construction")

    # Load PPI data for inflation adjustment (monthly only, not annual)
    ds_ppi = paths.load_dataset("us_ppi_construction")
    tb_ppi = ds_ppi.read("us_ppi_construction")
    tb_ppi = tb_ppi[tb_ppi["month"].notna()].reset_index()

    #
    # Process data.
    #

    # Convert spending from millions to actual dollars for consistency
    tb["datacenter_construction_spending"] = tb["datacenter_construction_spending"] * 1_000_000

    # Merge with PPI data using pr.merge to preserve metadata
    tb = pr.merge(tb, tb_ppi[["date", "ppi_new_office_construction"]], on=["date"], how="left")

    # Adjust for inflation using PPI (normalize to base year, typically 2009=100)
    # Get the most recent PPI value for normalization
    latest_ppi = tb["ppi_new_office_construction"].max()
    tb["datacenter_construction_spending_real"] = tb["datacenter_construction_spending"] * (
        latest_ppi / tb["ppi_new_office_construction"]
    )

    # Drop the PPI column as it's not needed in output
    tb = tb.drop(columns=["ppi_new_office_construction"])

    # Add country column (this is U.S. data)
    tb["country"] = "United States"

    # Set appropriate format and metadata
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
