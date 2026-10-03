"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


CONTRIBUTORS = [
    "domestic_resources",
    "g7_countries__and__european_commission",
    "multilateral_sector",
    "non_g7_oecd_countries",
    "other_donor_countries",
    "private_sector__non_governmental_donors",
]
# WDI only has data up to 2021, so adding this manually until we have the data for 2022
CPI_2022 = 134.21


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gpei_funding")
    ds_wdi = paths.load_dataset("wdi")
    # Read table from meadow dataset.
    tb = ds_meadow["gpei_funding"].reset_index()
    tb_wdi = ds_wdi["wdi"].reset_index()

    #
    # Process data.
    assert all(col in tb.columns for col in CONTRIBUTORS), "Missing columns in the table."
    tb["total"] = tb[CONTRIBUTORS].sum(axis=1)
    tb["total"] = tb["total"].copy_metadata(tb["domestic_resources"])
    # Adjust funding values for inflation.
    tb = adjust_for_inflation(tb, tb_wdi, inflation_base_year=2021)
    # Format the table.
    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def adjust_for_inflation(tb: Table, tb_wdi: Table, inflation_base_year: int) -> Table:
    """Adjust the funding values for inflation."""
    # Grab the consumer price index data for the US
    tb_cpi_us = tb_wdi.loc[
        (tb_wdi["country"] == "United States"),
        ["country", "year", "fp_cpi_totl"],
    ]
    if tb_cpi_us["year"].max() < 2022:
        tb_cpi_2022 = Table({"country": "United States", "year": 2022, "fp_cpi_totl": CPI_2022}, index=[0])
        tb_cpi_us = pr.concat([tb_cpi_us, tb_cpi_2022]).reset_index(drop=True)
    # Adjust CPI values so that 2021 is the reference year (2021 = 1)
    cpi_base = tb_cpi_us.loc[tb_cpi_us["year"] == inflation_base_year, "fp_cpi_totl"].values[0]
    # Divide the base year by the other years to get the inflation factor
    tb_cpi_us[f"cpi_adj_{inflation_base_year}"] = cpi_base / tb_cpi_us["fp_cpi_totl"]
    tb_cpi_us = tb_cpi_us.drop(["country", "fp_cpi_totl"], axis=1)
    # Merge the CPI data with the funding data
    tb_inf = pr.merge(tb, tb_cpi_us, on="year", how="inner")
    # Columns to adjust for inflation
    cols_to_adjust = CONTRIBUTORS + ["total"]
    # Adjust the funding values.
    for col in cols_to_adjust:
        tb_inf[col] = tb_inf[col] * tb_inf[f"cpi_adj_{inflation_base_year}"]
    # Drop the inflation factor column.
    tb_inf = tb_inf.drop(columns=[f"cpi_adj_{inflation_base_year}"])
    return tb_inf
