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
    log.info("sdg.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("sdg")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["sdg"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("sdg.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    df = label_projections(df)

    df = df.pivot(
        index=["country", "year"],
        columns=["indicator_name", "age_group_name", "sex_label", "scenario_label"],
        values=["mean_estimate"],
    )
    df.columns = [" ".join(col).strip() for col in df.columns.values]
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("sdg.end")


def label_projections(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the first year for projected data and label it and all later years as projections,
    so that they can be plotted as projections in grapher.
    """
    # Find the minimum year for projections
    min_year_projection = df.loc[df["scenario_label"] == "Worse", "year"].min()

    # Select rows before and after the minimum year
    df_estimate = df.loc[df["year"] < min_year_projection, :]
    df_projection = df.loc[df["year"] >= min_year_projection, :]

    # Map the scenario labels to projections
    label_map_est = {
        "Reference": "Reference Estimate",
    }

    label_map_proj = {
        "Reference": "Reference Projection",
        "Worse": "Worse Projection",
        "Better": "Better Projection",
    }
    df_estimate = df_estimate.replace({"scenario_label": label_map_est})
    df_projection = df_projection.replace({"scenario_label": label_map_proj})

    # Concatenate the DataFrames and check the shape
    df_replaced = pd.DataFrame(pd.concat([df_estimate, df_projection]))
    assert df_replaced.shape == df.shape

    return df_replaced
