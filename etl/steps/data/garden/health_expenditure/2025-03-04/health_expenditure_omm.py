"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define constants: variables to process and references years where merge is done.
VAR_LIST = ["gdp", "gdp_per_capita"]
YEAR_OECD_OECD93 = 1970
YEAR_OECD93_LINDERT = 1960


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_oecd = paths.load_dataset("health_expenditure")
    ds_oecd_1993 = paths.load_dataset("health_expenditure_1993")
    ds_lindert = paths.load_dataset("lindert")

    # Read table from meadow dataset.
    tb_oecd = ds_oecd.read("health_expenditure")
    tb_oecd_1993 = ds_oecd_1993.read("health_expenditure_1993")
    tb_lindert = ds_lindert.read("lindert")

    #
    # Process data.
    #
    # Select the right financing scheme we need from the OECD Health Expenditure and Financing Database
    tb_oecd = tb_oecd[tb_oecd["financing_scheme"] == "Government/compulsory schemes"].reset_index(drop=True)

    # Keep only the necessary columns
    tb_oecd = tb_oecd[["country", "year", "share_gdp"]]

    # Save the countries available in the OECD dataset
    countries_oecd = list(tb_oecd["country"].unique())

    # Merge the three tables
    tb = pr.merge(tb_oecd, tb_oecd_1993, on=["country", "year"], how="outer", suffixes=("", "_oecd_1993"))
    tb = pr.merge(tb, tb_lindert, on=["country", "year"], how="outer", suffixes=("", "_lindert"))

    # Rename share_gdp to share_gdp_oecd
    tb = tb.rename(columns={"share_gdp": "share_gdp_oecd"})

    # Keep only countries available in the OECD dataset
    tb = tb[tb["country"].isin(countries_oecd)].reset_index(drop=True)

    print(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
    ds_garden.save()
