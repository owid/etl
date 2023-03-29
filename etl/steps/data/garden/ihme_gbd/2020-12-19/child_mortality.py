import json
from typing import List, cast

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


def run(dest_dir: str) -> None:
    log.info("child_mortality.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/ihme_gbd/2020-12-19/child_mortality")
    tb_meadow = ds_meadow["child_mortality"]

    df = pd.DataFrame(tb_meadow)
    df = df.drop(columns="index")
    log.info("child_mortality.exclude_countries")
    df = exclude_countries(df)

    log.info("child_mortality.harmonize_countries")
    df = harmonize_countries(df)

    # Selecting only Both sex values for now as need this data quite quickly - Also dropping Rate metrics as it is not clear what this means.
    df = df[(df["sex"] == "Both") & (df["metric_name"] != "Rate")]

    df_p = df.pivot(index=["country", "year"], columns=["measure_name", "age_group_name"], values="value")

    df_p.columns = ["_".join(col).strip() for col in df_p.columns.values]
    df_p = df_p.reset_index()

    # Ensuring there is appropriate rounding for the different metrics
    num_cols = [col for col in df_p.columns if "Deaths" in col]
    prob_cols = [col for col in df_p.columns if "Probability of death" in col]
    df_p[num_cols] = df_p[num_cols].round(0).astype(int)
    df_p[prob_cols] = 100 * df_p[prob_cols]
    df_p[prob_cols] = df_p[prob_cols].round(2)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df_p))
    tb_garden.metadata = tb_meadow.metadata

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "child_mortality")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("child_mortality.end")


def load_excluded_countries() -> List[str]:
    with open(N.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


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
