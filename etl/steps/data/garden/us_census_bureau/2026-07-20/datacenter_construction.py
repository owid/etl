"""Garden step for datacenter construction spending data."""

import owid.catalog.processing as pr
from owid.catalog import Table

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

    sanity_check_inputs(tb=tb, tb_ppi=tb_ppi)

    #
    # Process data.
    #

    # Convert spending from millions to actual dollars for consistency
    tb["datacenter_construction_spending"] = tb["datacenter_construction_spending"] * 1_000_000
    tb["general_office_construction_spending"] = tb["general_office_construction_spending"] * 1_000_000

    # Merge with PPI data using pr.merge to preserve metadata
    tb = pr.merge(tb, tb_ppi[["date", "ppi_new_office_construction"]], on=["date"], how="left")

    # Rebase PPI to January 2021 = 100
    ppi_jan_2021 = tb_ppi[tb_ppi["date"] == "2021-01-01"]["ppi_new_office_construction"].values[0]
    tb["ppi_rebased"] = (tb["ppi_new_office_construction"] / ppi_jan_2021) * 100

    # Adjust for inflation using PPI (base year January 2021=100)
    tb["datacenter_construction_spending_real"] = tb["datacenter_construction_spending"] * (100 / tb["ppi_rebased"])
    tb["general_office_construction_spending_real"] = tb["general_office_construction_spending"] * (
        100 / tb["ppi_rebased"]
    )

    # Drop the PPI columns as they're not needed in output
    tb = tb.drop(columns=["ppi_new_office_construction", "ppi_rebased"])

    # Add country column (this is U.S. data)
    tb["country"] = "United States"

    sanity_check_outputs(tb=tb)

    # Set appropriate format and metadata
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table, tb_ppi: Table) -> None:
    """Check assumptions about the meadow spending table and the PPI table."""
    expected_columns = {"date", "datacenter_construction_spending", "general_office_construction_spending"}
    assert set(tb.columns) == expected_columns, f"Unexpected meadow columns: {set(tb.columns) ^ expected_columns}"
    assert not tb["date"].duplicated().any(), "Duplicate dates in meadow spending table."

    spending_columns = ["datacenter_construction_spending", "general_office_construction_spending"]
    assert tb[spending_columns].notna().all().all(), "NaN spending values in meadow table."
    assert (tb[spending_columns] > 0).all().all(), "Non-positive spending values in meadow table."
    # Values arrive in millions of dollars; monthly spending should be between $100 million and $100 billion.
    assert 100 < tb[spending_columns].max().max() < 100_000, (
        "Meadow spending magnitude looks wrong — expected millions of dollars."
    )

    assert not tb_ppi["date"].duplicated().any(), "Duplicate dates in monthly PPI table."
    assert (tb_ppi["ppi_new_office_construction"] > 0).all(), "Non-positive PPI values."
    assert (tb_ppi["date"] == "2021-01-01").any(), "PPI base month (January 2021) is missing."
    # The PPI must cover every spending month, otherwise the inflation-adjusted series silently ends early.
    uncovered = set(tb["date"]) - set(tb_ppi["date"])
    assert not uncovered, f"Spending months without PPI coverage (real series would be NaN): {sorted(uncovered)}"


def sanity_check_outputs(tb: Table) -> None:
    """Check the output table right before formatting."""
    value_columns = [
        "datacenter_construction_spending",
        "general_office_construction_spending",
        "datacenter_construction_spending_real",
        "general_office_construction_spending_real",
    ]
    assert tb[value_columns].notna().all().all(), "NaN values in output — PPI coverage gap or broken merge."
    assert (tb[value_columns] > 0).all().all(), "Non-positive values in output."
    # After the millions-to-dollars conversion, monthly spending should be between $100 million and $100 billion.
    assert 1e8 < tb[value_columns].max().max() < 1e11, (
        "Output spending magnitude looks wrong — millions-to-dollars conversion may be missing or doubled."
    )
