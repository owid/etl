import json
from typing import List

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
    excluded_countries = load_excluded_countries()
    df = df.loc[~df.country.isin(excluded_countries)]

    log.info("{{cookiecutter.short_name}}.harmonize_countries")
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

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
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
