import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)
MEADOW_DATASET = "meadow/health/2022-12-28/deaths_karlinsky"
TABLE_NAME = "deaths"


def run(dest_dir: str) -> None:
    # read dataset from meadow
    log.info("deaths_karlinsky: loading meadow table...")
    ds_meadow = Dataset(DATA_DIR / MEADOW_DATASET)
    tb_meadow = ds_meadow[TABLE_NAME]

    # clean dataframe
    log.info("karlinsky: cleaning dataframe...")
    df = clean_dataframe(tb_meadow)

    # sanity checks
    log.info("karlinsky: sanity checking...")
    sanity_check(df)

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = Table(df, short_name=TABLE_NAME)
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("deaths_karlinsky.end")


def clean_dataframe(tb: Table) -> pd.DataFrame:
    # convert table to dataframe
    df = pd.DataFrame(tb)
    # drop and rename columns
    df = df.drop(columns=["continent", "source"])
    df = df.rename(columns={"country_name": "country"})
    # harmonize country names
    df = harmonize_countries(df)
    # set indexes
    df = df.set_index(["country", "year"]).sort_index()
    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(
        df=df,
        countries_file=str(N.country_mapping_path),
        warn_on_missing_countries=True,
        make_missing_countries_nan=True,
    )

    missing_countries = set(unharmonized_countries[df["country"].isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def sanity_check(df: pd.DataFrame) -> None:
    # check columns
    columns_expected = {
        "death_comp",
        "expected_deaths",
        "expected_gbd",
        "expected_ghe",
        "expected_wpp",
        "reg_deaths",
    }
    columns_new = set(df.columns).difference(columns_expected)
    if columns_new:
        raise ValueError(f"Unexpected columns {columns_new}")

    # ensure percentages make sense (within range [0, 100])
    columns_perc = ["death_comp"]
    for col in columns_perc:
        assert all(df[col] <= 100), f"{col} has values larger than 100%"
        assert all(df[col] >= 0), f"{col} has values lower than 0%"

    # ensure absolute values make sense (positive, lower than population)
    columns_absolute = [col for col in df.columns if col not in columns_perc]
    df_ = df.reset_index()
    df_ = geo.add_population_to_dataframe(df_)
    for col in columns_absolute:
        x = df_.dropna(subset=[col])
        assert all(
            x[col] < 0.2 * x["population"]
        ), f"{col} contains values that might be too large (compared to population values)!"
