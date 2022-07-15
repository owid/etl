import json
from pathlib import Path
from typing import Dict, List

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.paths import DATA_DIR

log = get_logger()


COUNTRY_MAPPING_PATH = (
    Path(__file__).parent / "{{cookiecutter.short_name}}.country_mapping.json"
)
EXCLUDED_COUNTRIES_PATH = (
    Path(__file__).parent / "{{cookiecutter.short_name}}.country_exclude.json"
)
{% if cookiecutter.include_metadata_yaml == "True" %}
METADATA_PATH = (
    Path(__file__).parent / "{{cookiecutter.short_name}}.meta.yml"
)
{% endif %}

def run(dest_dir: str) -> None:
    log.info("{{cookiecutter.short_name}}.start")

    # read dataset from meadow
    ds_meadow = Dataset(
        DATA_DIR
        / "meadow/{{cookiecutter.namespace}}/{{cookiecutter.version}}/{{cookiecutter.short_name}}"
    )
    tb_meadow = ds_meadow["{{cookiecutter.short_name}}"]

    df = pd.DataFrame(tb_meadow)

    log.info("{{cookiecutter.short_name}}.harmonize_countries")
    country_mapping = load_country_mapping()
    excluded_countries = load_excluded_countries()
    df = df[~df.country.isin(excluded_countries)]
    assert df["country"].notnull().all()
    countries = df["country"].map(country_mapping)
    if countries.isnull().any():
        missing_countries = [
            x for x in df["country"].drop_duplicates() if x not in country_mapping
        ]
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            "names; or (b) remove these country names from the raw table."
            f"Raw country names: {missing_countries}"
        )
    df["country"] = countries

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata
    {% if cookiecutter.include_metadata_yaml == "True" %}
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    tb_garden.update_metadata_from_yaml(METADATA_PATH, "{{cookiecutter.short_name}}")
    {% endif %}
    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("{{cookiecutter.short_name}}.end")


def load_country_mapping() -> Dict[str, str]:
    with open(COUNTRY_MAPPING_PATH, "r") as f:
        mapping = json.load(f)
        assert isinstance(mapping, dict)
    return mapping


def load_excluded_countries() -> List[str]:
    with open(EXCLUDED_COUNTRIES_PATH, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data
