"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference: Dataset = paths.load_dependency("reference")
    tb_countries_regions = ds_reference["countries_regions"]

    return tb_countries_regions


def load_population() -> Table:
    """Load population table from population OMM dataset."""
    ds_indicators: Dataset = paths.load_dependency(channel="garden", namespace="demography", short_name="population")
    tb_population = ds_indicators["population"]

    return tb_population


def run(dest_dir: str) -> None:
    log.info("equaldex.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("equaldex")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["equaldex"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index(drop=False)

    #
    # Process data.

    # Make the dataframe wide by pivoting on the issue column.
    df = df.pivot(
        index=["country", "year"], columns="issue", values=["id", "value", "value_formatted", "description"]
    ).reset_index(drop=False)

    # Flatten the multi-index column names, but keep country and year as separate columns.
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    df = df.rename(columns={"country_": "country", "year_": "year"})

    # replace all column names with - with _
    df.columns = df.columns.str.replace("-", "_")

    print(df.columns)

    log.info("equaldex.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Create a new table with the processed data.
    tb_garden = Table(
        df,
        short_name=paths.short_name,
    )

    # Change id columns to number
    tb_garden["id_homosexuality"] = tb_garden["id_homosexuality"].astype("Int64")
    tb_garden["id_marriage"] = tb_garden["id_marriage"].astype("Int64")
    tb_garden["id_changing_gender"] = tb_garden["id_changing_gender"].astype("Int64")

    # Verify index and order them
    tb_garden = tb_garden.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("equaldex.end")
