"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("gbd_mental_health_prevalence_rate.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("gbd_mental_health_prevalence_rate")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["gbd_mental_health_prevalence_rate"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Rename countries
    log.info("gbd_mental_health_prevalence_rate.harmonize_countries")
    df = df.reset_index()
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Pivot
    log.info("gbd_mental_health_prevalence_rate.pivot")
    df = df.pivot(index=["country", "year", "sex", "age"], columns="cause", values="prevalence_rate")
    # Rename columns
    df = rename_columns(df)
    # Add share of population metrics
    df = add_share_of_population(df)
    # Sort index
    df = df.sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("gbd_mental_health_prevalence_rate.end")


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns"""
    log.info("gbd_mental_health_prevalence_rate.rename_columns")
    column_mapping = {
        "Anxiety disorders": "prevalence_rate_anxiety_disorders",
        "Bipolar disorder": "prevalence_rate_bipolar_disorders",
        "Depressive disorders": "prevalence_rate_depressive_disorders",
        "Eating disorders": "prevalence_rate_eating_disorders",
        "Schizophrenia": "prevalence_rate_schizophrenia_disorders",
    }
    assert set(df.columns) == set(column_mapping), f"Unexpected columns. Expected were {column_mapping}"
    df = df.rename(columns=column_mapping)
    return df


def add_share_of_population(df: pd.DataFrame) -> pd.DataFrame:
    """Add columns for share of population. Estimated from rates."""
    df_perc = 100 * df / 100000
    df_perc = df_perc.rename(columns={col: col.replace("prevalence_rate", "share") for col in df_perc.columns})
    df = df.merge(df_perc, left_index=True, right_index=True)
    return df
