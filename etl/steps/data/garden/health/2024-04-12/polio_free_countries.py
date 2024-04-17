"""Load a meadow dataset and create a garden dataset."""

from itertools import product

from owid.catalog import Table

from etl.data_helpers.geo import harmonize_countries
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

LATEST_YEAR = 2023


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("polio_free_countries")
    tb = ds_meadow["polio_free_countries"].reset_index()

    ##### Temporary fix - we remove West Bank and Gaza as there is both data for West Bank and Gaza _and_ Palestine N.A (national authority).
    ##### I'm not sure how we should treat these but for now I will just stick with the entity that has the latest value, so Palestine N.A.

    tb = tb[tb["country"] != "West Bank and Gaza"]
    ##### There are also two values for Somalia, I will drop the least recent one
    tb = tb[~((tb["country"] == "Somalia") & (tb["year"] == 2000))]

    # Adding the regional status to the polio free countries table
    ds_region_status = paths.load_dataset(short_name="polio_status", channel="meadow")
    tb_region_status = ds_region_status["polio_status"].reset_index()

    # Adding regions data
    # ds_regions = paths.load_dataset("regions")
    # Assign polio free countries.

    tb = harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb, tb_status = define_polio_free_new(tb, latest_year=LATEST_YEAR)
    # Set an index and sort.
    tb = tb.format()
    tb = tb.set_index(["country"]).sort_index()
    tb_status = tb_status.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_status], check_variables_metadata=True)
    ds_garden.save()


def define_polio_free_new(tb: Table, latest_year: int) -> Table:
    """Define the polio free countries table."""
    # Make a copy of the DataFrame to avoid modifying the original DataFrame
    tb = tb.copy()

    # Clean the data
    tb["year"] = tb["year"].astype(str)

    # Drop countries with missing values explicitly copying to avoid setting on a slice warning
    tb = tb[tb["year"] != "data not available"].copy()

    # Change 'pre 1985' to 1984 and 'ongoing' to LATEST_YEAR + 1
    tb.loc[tb["year"] == "pre 1985", "year"] = "1984"
    tb.loc[tb["year"] == "ongoing", "year"] = str(latest_year + 1)

    tb["year"] = tb["year"].astype(int)
    # Rename year to latest year
    tb = tb.rename(columns={"year": "latest_year_wild_polio_case"})
    tb["year"] = latest_year
    # Create a product of all countries and all years from 1910 to LATEST_YEAR
    tb_prod = Table(product(tb["country"].unique(), range(1910, latest_year + 1)), columns=["country", "year"])

    # Define polio status based on the year comparison
    tb_prod["status"] = tb_prod.apply(
        lambda row: "Endemic"
        if row["year"] < tb[tb["country"] == row["country"]]["latest_year_wild_polio_case"].min()
        else "Polio-free (not certified)",
        axis=1,
    )

    return tb, tb_prod
