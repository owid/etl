import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/biodiversity/2023-01-11/cherry_blossom")
    tb_meadow = ds_meadow["cherry_blossom"]

    df = pd.DataFrame(tb_meadow)

    # Calculate a 20,40 and 50 year average
    df = calculate_multiple_year_average(df)

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with metadata from meta.yml
    df = df.reset_index(drop=True)
    tb_garden = Table(df, short_name=tb_meadow.metadata.short_name)
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("cherry_blossom.end")


def calculate_multiple_year_average(df: pd.DataFrame) -> pd.DataFrame:
    min_year = df["year"].min()
    max_year = df["year"].max()

    df_year = pd.DataFrame()
    df_year["year"] = pd.Series(range(min_year, max_year))
    df_year["country"] = "Japan"
    df_comb = pd.merge(df, df_year, how="outer", on=["country", "year"])

    df_comb = df_comb.sort_values("year")

    df_comb["average_20_years"] = df_comb["full_flowering_date"].rolling(20, min_periods=1).mean()

    df_comb["average_40_years"] = df_comb["full_flowering_date"].rolling(40, min_periods=1).mean()

    df_comb["average_50_years"] = df_comb["full_flowering_date"].rolling(50, min_periods=1).mean()

    df_comb["average_100_years"] = df_comb["full_flowering_date"].rolling(100, min_periods=1).mean()

    # experimenting with splines
    # df_nona = df_comb.dropna(subset="full_flowering_date")
    # spl = UnivariateSpline(x=df_nona["year"], y=df_nona["full_flowering_date"], s=200, k=1)
    # df_comb["uni_spline"] = spl(df_comb["year"])

    return df_comb
