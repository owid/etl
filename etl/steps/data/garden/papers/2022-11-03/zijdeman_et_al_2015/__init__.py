import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)
COUNTRY_MAPPING_PATH = N.directory / "countries.json"
EXCLUDED_COUNTRIES_PATH = N.directory / "excluded_countries.json"
METADATA_PATH = N.directory / "meta.yml"


def run(dest_dir: str) -> None:
    log.info("zijdeman_et_al_2015.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/papers/2022-11-03/zijdeman_et_al_2015")
    tb_meadow = ds_meadow["zijdeman_et_al_2015"]

    # Create table
    tb_garden = make_table(tb_meadow)

    # Initiate dataset
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.update_from_yaml(METADATA_PATH)

    # Add table
    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("zijdeman_et_al_2015.end")


def make_table(tb_meadow: Table) -> Table:
    df = pd.DataFrame(tb_meadow)

    # Create table
    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    # Clean countries
    tb_garden = format_columns(tb_garden)
    tb_garden = clean_countries(tb_garden)

    # Set index
    tb_garden = tb_garden.set_index(["country", "year"], verify_integrity=True)

    # Metadata
    tb_garden.update_metadata_from_yaml(METADATA_PATH, "zijdeman_et_al_2015")
    return tb_garden


def clean_countries(df: pd.DataFrame) -> pd.DataFrame:
    df = harmonize_countries(df)
    return df


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = {
        "country_name": "country",
        "year": "year",
        "value": "life_expectancy",
    }
    df = df[columns.keys()].rename(columns=columns)
    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(COUNTRY_MAPPING_PATH))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            f"names; or (b) add them to {EXCLUDED_COUNTRIES_PATH}."
            f"Raw country names: {missing_countries}"
        )

    return df
