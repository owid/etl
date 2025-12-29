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

    # Rebase PPI to January 2014 = 100
    ppi_jan_2014 = tb_ppi[tb_ppi["date"] == "2014-01-01"]["ppi_new_office_construction"].values[0]
    tb["ppi_rebased"] = (tb["ppi_new_office_construction"] / ppi_jan_2014) * 100

    # Adjust for inflation using PPI (base year January 2014=100)
    tb["datacenter_construction_spending_real"] = tb["datacenter_construction_spending"] * (100 / tb["ppi_rebased"])

    # Drop the PPI columns as they're not needed in output
    tb = tb.drop(columns=["ppi_new_office_construction", "ppi_rebased"])

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
