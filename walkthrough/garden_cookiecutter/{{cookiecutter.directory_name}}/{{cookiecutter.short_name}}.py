"""Load a meadow dataset and create a garden dataset."""

import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("{{cookiecutter.short_name}}.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dependency("{{cookiecutter.short_name}}")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["{{cookiecutter.short_name}}"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("{{cookiecutter.short_name}}.exclude_countries")
    df = exclude_countries(df)

    log.info("{{cookiecutter.short_name}}.harmonize_countries")
    df = harmonize_countries(df)

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)
    {% if cookiecutter.include_metadata_yaml == "True" %}

    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)
    {% endif %}

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("{{cookiecutter.short_name}}.end")


# TODO: These functions should be removed once harmonize_countries is improved.
def load_excluded_countries() -> List[str]:
    with open(paths.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(paths.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {paths.country_mapping_path} to include these country "
            f"names; or (b) add them to {paths.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df
