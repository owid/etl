import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("ghe.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/who/2022-09-30/ghe")
    tb_meadow = ds_meadow["ghe"]

    df = pd.DataFrame(tb_meadow)

    log.info("ghe.harmonize_countries")
    df = harmonize_countries(df)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = clean_data(df)
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "ghe")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("ghe.end")


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def clean_data(df: pd.DataFrame) -> Table:
    df["sex"] = df["sex"].map({"BTSX": "Both sexes", "MLE": "Male", "FMLE": "Female"})
    df = df.set_index(["country", "year", "age_group", "sex", "cause"])
    df = df.round({"daly_rate100k": 2, "daly_count": 2, "death_rate100k": 2, "death_count": 0})
    df["death_count"] = df["death_count"].astype(int)
    df = underscore_table(Table(df))
    return df
