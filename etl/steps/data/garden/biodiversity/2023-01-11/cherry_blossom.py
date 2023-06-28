import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/biodiversity/2023-01-11/cherry_blossom")
    tb_meadow = ds_meadow["cherry_blossom"].reset_index()

    df = pd.DataFrame(tb_meadow)

    # Calculate a 20,40 and 50 year average
    df = calculate_multiple_year_average(df)

    tb = Table(df.reset_index(drop=True), short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=ds_meadow.metadata)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def calculate_multiple_year_average(df: pd.DataFrame) -> pd.DataFrame:
    min_year = df["year"].min()
    max_year = df["year"].max()

    df_year = pd.DataFrame()
    df_year["year"] = pd.Series(range(min_year, max_year))
    df_year["country"] = "Japan"
    df_comb = pd.merge(df, df_year, how="outer", on=["country", "year"])

    df_comb = df_comb.sort_values("year")

    df_comb["average_20_years"] = df_comb["full_flowering_date"].rolling(20, min_periods=5).mean()

    return df_comb
