"""Load a meadow dataset and create a garden dataset."""

from itertools import product

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers.geo import harmonize_countries, list_members_of_region
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

LATEST_YEAR = 2023
# Due to relatively recent cases in Mozambique and Malawi, they are considered affected countries. This was confirmed with Vikram from GPEI in April 2024.
AFFECTED_COUNTRIES = ["Mozambique", "Malawi"]
ENDEMIC_COUNTRIES = ["Afghanistan", "Pakistan"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("polio_free_countries")
    tb = ds_meadow["polio_free_countries"].reset_index()
    tb = tb[["country", "year"]]
    # Adding south sudan value
    south_sudan = {"country": "South Sudan", "year": 2004}
    tb.loc[len(tb)] = south_sudan
    tb = tb.reset_index(drop=True)
    ###### Confirmed that the value for Palestine NA is the correct one with GPEI

    tb = tb[tb["country"] != "West Bank and Gaza"]
    ##### There are also two values for Somalia, I will drop the least recent one - confirmed with GPEI that Somalia's last wild polio case was in 2002
    tb = tb[~((tb["country"] == "Somalia") & (tb["year"] == "2000"))]

    # Loading the polio status data for WHO regions
    ds_region_status = paths.load_dataset(short_name="polio_status", channel="meadow")
    tb_region_status = ds_region_status["polio_status"].reset_index()

    # Loading in the regions table so we know which countries are in each WHO region
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"].reset_index()
    who_regions = tb_regions[(tb_regions["defined_by"] == "who") & (tb_regions["region_type"] == "aggregate")]

    tb = harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Assign polio free countries.
    tb = define_polio_free(tb, latest_year=LATEST_YEAR)
    tb = add_polio_region_certification(tb, tb_region_status, who_regions, ds_regions)
    tb = add_affected_and_endemic_countries(tb)
    tb = tb[tb["year"] <= LATEST_YEAR]
    # Set an index and sort.
    tb = tb.format()
    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()


def add_affected_and_endemic_countries(tb: Table) -> Table:
    """
    We'll code affected countries as 3000 and endemic countries as 4000, so that we can continue to use this variable in the same way in grapher (numeric)
    """
    tb.loc[(tb["country"].isin(AFFECTED_COUNTRIES)) & (tb["year"] == LATEST_YEAR), "latest_year_wild_polio_case"] = 3000
    tb.loc[(tb["country"].isin(ENDEMIC_COUNTRIES)) & (tb["year"] == LATEST_YEAR), "latest_year_wild_polio_case"] = 4000

    return tb


def add_polio_region_certification(
    tb: Table, tb_region_status: Table, who_regions: Table, ds_regions: Dataset
) -> Table:
    # Append "(WHO)" suffix to the "who_region" to match the region names in the who_regions table
    tb_region_status["who_region"] = tb_region_status["who_region"].astype(str) + " (WHO)"

    # Correct mapping of regions to status updates by ensuring 'region' matches the modified 'who_region' entries
    for region in who_regions["name"]:
        # Generate country list for the current region
        country_list = list_members_of_region(region=region, ds_regions=ds_regions)
        if not country_list:
            raise ValueError(f"No countries found for region {region}")

        # Find the year of certification for the current region
        year_certified = tb_region_status.loc[tb_region_status["who_region"] == region, "year_certified_polio_free"]

        # Check if there is a valid year of certification
        if not year_certified.empty and year_certified.notna().all():
            year_certified_int = int(year_certified.iloc[0])
            print(year_certified_int)
            tb_who_region = Table(
                product(country_list, range(year_certified_int, LATEST_YEAR + 1)), columns=["country", "year"]
            )
            # tb_who_region["status"] = "WHO Region certified polio-free"
            tb = pr.merge(tb, tb_who_region, on=["country", "year"], how="outer")
            # Set the status for all relevant countries and years
            tb.loc[
                tb["country"].isin(country_list) & (tb["year"] >= year_certified_int), "status"
            ] = "WHO Region certified polio-free"

    return tb


def define_polio_free(tb: Table, latest_year: int) -> Table:
    """Define the polio free countries table."""
    tb = tb.copy()
    # Clean the data
    tb["year"] = tb["year"].astype(str)

    # Drop countries with missing values explicitly copying to avoid setting on a slice warning
    tb = tb[tb["year"] != "data not available"]

    # Change 'pre 1985' to 1984 and 'ongoing' to LATEST_YEAR + 1
    tb.loc[tb["year"] == "pre 1985", "year"] = "1984"
    tb.loc[tb["year"] == "ongoing", "year"] = str(latest_year)

    tb["year"] = tb["year"].astype(int)
    # Rename year to latest year
    tb = tb.rename(columns={"year": "latest_year_wild_polio_case"})
    tb["year"] = latest_year
    # Create a product of all countries and all years from 1910 to LATEST_YEAR
    tb_prod = Table(product(tb["country"].unique(), range(1910, latest_year + 1)), columns=["country", "year"])
    tb_prod = tb_prod.copy_metadata(from_table=tb)

    # Define polio status based on the year comparison
    tb_prod["status"] = tb_prod.apply(
        lambda row: "Endemic"
        if row["year"] <= tb[tb["country"] == row["country"]]["latest_year_wild_polio_case"].min()
        else "Polio-free (not certified)",
        axis=1,
    )
    # Merge the two tables
    tb = pr.merge(tb, tb_prod, on=["country", "year"], how="right")
    # Issues with status not having origins or source, not sure this is the best way to solve
    tb["status"] = tb["status"].copy_metadata(tb["latest_year_wild_polio_case"])
    return tb
