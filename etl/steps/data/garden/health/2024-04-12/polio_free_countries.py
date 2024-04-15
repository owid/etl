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
    tb = define_polio_free(tb)
    # Set an index and sort.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()


def define_polio_free(tb: Table) -> Table:
    """Define the polio free countries table."""

    # Clean the data
    tb["year"] = tb["year"].astype(str)
    # Drop countries with missing values
    tb = tb[tb["year"] != "data not available"]
    # Change pre 1985 to 1984
    tb["year"] = tb["year"].replace("pre 1985", "1984")
    # Change ongoing to LATEST_YEAR + 1
    tb["year"] = tb["year"].replace("ongoing", LATEST_YEAR + 1)
    tb["year"] = tb["year"].astype(int)

    years = list(range(1910, LATEST_YEAR + 1))
    tb_prod = Table(product(tb["country"], years), columns=["country", "year"])

    tb_prod["status"] = tb_prod.apply(
        lambda row: "Endemic"
        if row["year"] < tb[tb["country"] == row["country"]]["year"].values[0]
        else "Polio-free (not certified)",
        axis=1,
    )

    return tb_prod
