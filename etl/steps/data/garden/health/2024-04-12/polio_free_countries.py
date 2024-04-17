"""Load a meadow dataset and create a garden dataset."""

from itertools import product

from owid.catalog import Table

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
    ds_region_status = paths.load_dataset(short_name="polio_status", channel="meadow")
    tb = ds_meadow["polio_free_countries"].reset_index()

    # Assign polio free countries.
    tb = define_polio_free_new(tb, latest_year=LATEST_YEAR)
    # Set an index and sort.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
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
    # Create a product of all countries and all years from 1910 to LATEST_YEAR
    tb_prod = Table(product(tb["country"].unique(), range(1910, latest_year + 1)), columns=["country", "year"])

    # Define polio status based on the year comparison
    tb_prod["status"] = tb_prod.apply(
        lambda row: "Endemic"
        if row["year"] < tb[tb["country"] == row["country"]]["latest_year_wild_polio_case"].min()
        else "Polio-free (not certified)",
        axis=1,
    )

    tb = tb.merge(tb_prod, on="country")

    return tb
