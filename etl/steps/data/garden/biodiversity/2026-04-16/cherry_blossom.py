import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Naming conventions
paths = PathFinder(__file__)


def run() -> None:
    log.info("cherry_blossom.start")

    # Read dataset from meadow.
    ds_meadow = paths.load_dataset("cherry_blossom")
    tb = ds_meadow.read("cherry_blossom")

    # Calculate a 20, 30 and 50 year average
    tb = calculate_multiple_year_average(tb)

    # Select and rename columns
    tb = tb[["country", "year", "full_flowering_date", "average_30_years_10"]]
    tb = tb.rename(columns={"average_30_years_10": "average_last_30_years"})

    # drop all columns where both data points are missing
    tb = tb.dropna(subset=["full_flowering_date", "average_last_30_years"], how="all")

    # Trim origins to only the first one.
    tb["full_flowering_date"].m.origins = [tb["full_flowering_date"].m.origins[0]]
    tb["average_last_30_years"].m.origins = [tb["average_last_30_years"].m.origins[0]]

    #
    # Save outputs.
    #
    tb = tb.format(["country", "year"])
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def calculate_multiple_year_average(tb: Table) -> Table:
    """
        Calculate moving averages over multiple years for the cherry blossom dataset.
    Args:
        tb (Table): The input table containing cherry blossom data.
    Returns:
        Table: The modified table with additional columns for moving averages.
    """
    tb = tb.sort_values("year")
    tb["country"] = "Japan"

    tb["average_20_years_prev"] = tb["full_flowering_date"].rolling(20, min_periods=5).mean()

    # add rows for all years with missing data to ensure the rolling average is calculated correctly
    all_years = Table(pd.DataFrame({"year": range(tb["year"].min(), tb["year"].max() + 1), "country": "Japan"}))
    tb = pr.merge(all_years, tb, on=["year", "country"], how="left").copy_metadata(tb)

    tb["average_20_years"] = tb["full_flowering_date"].rolling(20, min_periods=5).mean()
    tb["average_30_years_5"] = tb["full_flowering_date"].rolling(30, min_periods=5).mean()
    tb["average_30_years_10"] = tb["full_flowering_date"].rolling(30, min_periods=10).mean()
    return tb
