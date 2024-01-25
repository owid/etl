from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # read dataset from meadow
    ds_meadow = cast(Dataset, paths.load_dependency("cherry_blossom"))
    tb = ds_meadow["cherry_blossom"].reset_index()

    # Calculate a 20,40 and 50 year average
    tb = calculate_multiple_year_average(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=ds_meadow.metadata)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def calculate_multiple_year_average(tb: Table) -> Table:
    tb = tb.set_index("year").sort_values("year").reset_index()
    tb["country"] = "Japan"

    tb["average_20_years"] = tb["full_flowering_date"].rolling(20, min_periods=5).mean()
    tb["average_20_years"].metadata = tb["full_flowering_date"].metadata

    return tb
