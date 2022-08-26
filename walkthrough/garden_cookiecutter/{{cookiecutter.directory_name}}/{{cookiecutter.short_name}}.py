import json
from typing import List, cast

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
    log.info("{{cookiecutter.short_name}}.start")

    # read dataset from meadow
    ds_meadow = Dataset(
        DATA_DIR
        / "meadow/{{cookiecutter.namespace}}/{{cookiecutter.version}}/{{cookiecutter.short_name}}"
    )
    tb_meadow = ds_meadow["{{cookiecutter.short_name}}"]

    df = pd.DataFrame(tb_meadow)

    log.info("{{cookiecutter.short_name}}.exclude_countries")
    df = exclude_countries(df)

    log.info("{{cookiecutter.short_name}}.harmonize_countries")
    df = harmonize_countries(df)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata
    {% if cookiecutter.include_metadata_yaml == "True" %}
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "{{cookiecutter.short_name}}")
    {% endif %}
    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("{{cookiecutter.short_name}}.end")


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
