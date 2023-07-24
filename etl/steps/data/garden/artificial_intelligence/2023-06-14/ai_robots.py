"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ai_robots.start")

    #
    # Load inputs.
    #
    # Load Snapshot
    ds_meadow = cast(Dataset, paths.load_dependency("ai_robots"))

    # Read table from meadow dataset.
    tb = ds_meadow["ai_robots"]
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Iterate over the columns
    for column in tb.columns:
        # Check if the column includes "in_thousands"
        if "__in_thousands" in column:
            # Multiply the values by 1000
            tb[column] = tb[column].astype(float) * 1000
            new_column_name = column.replace("__in_thousands", "")

            # Rename the column
            tb.rename(columns={column: new_column_name}, inplace=True)

            # Convert the column values to numeric
            tb[new_column_name] = pd.to_numeric(tb[new_column_name], errors="coerce")

    # Convert categorical column to string
    tb["country"] = tb["country"].astype(str)
    tb["country"] = tb["country"].replace("nan", "World")

    tb.rename(
        columns={
            "installed_countries__number_of_industrial_robots_installed": "number_of_industrial_robots_installed_2021"
        },
        inplace=True,
    )
    # Because of the way data was concatenated in snapshot, need to clean up the aggregate columns to avoid duplicates
    # Select aggregate columns and clean duplicated indices
    cols_agg = [
        "year",
        "country",
        "cumulative_operational__number_of_industrial_robots",
        "number_of_industrial_robots_installed_2021",
        "annual_count__number_of_industrial_robots_installed",
        "new_robots_installed__number_of_industrial_robots_installed",
    ]
    df_agg_clean = tb[cols_agg]
    df_agg_clean.set_index(["country", "year"], inplace=True)
    df_agg_clean = df_agg_clean.groupby(level=[0, 1]).last()

    # Generate pivot table for professional service robots
    df_pivot_serv_appl = generate_pivot(
        tb,
        ["year", "country"],
        "professional_service_robots__number_of_professional_service_robots_installed",
        "professional_service_robots__application_area",
    )

    # Generate pivot table for installed sectors
    df_pivot_sect = generate_pivot(
        tb,
        ["year", "country"],
        "installed_sectors__number_of_industrial_robots_installed",
        "installed_sectors__sector",
    )

    # Generate pivot table for installed application
    df_pivot_app = generate_pivot(
        tb,
        ["year", "country"],
        "installed_application__number_of_industrial_robots_installed",
        "installed_application__application",
    )

    # Merge pivot tables for application and sectors
    merge_app_sec = pd.merge(df_pivot_app, df_pivot_sect, on=["year", "country"], how="outer")
    merge_app_sec.rename(
        columns={"Unspecified_x": "Unspecified Application", "Unspecified_y": "Unspecified Sector"}, inplace=True
    )

    # Merge pivot table for professional service robots with the merged app and sector pivot
    merge_service = pd.merge(merge_app_sec, df_pivot_serv_appl, on=["year", "country"], how="outer")

    # Merge pivot table for professional service robots, application area and sector with aggregates
    merge_all = pd.merge(merge_service, df_agg_clean, on=["year", "country"], how="outer")
    merge_all["unspecified_others"] = merge_all["Unspecified Sector"] + merge_all["All Others"]

    # Set the index as 'country' and 'year'
    merge_all.set_index(["country", "year"], inplace=True)

    tb = Table(merge_all, short_name=paths.short_name, underscore=True)

    #
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_robots.end")


def generate_pivot(df, index_cols, value_col, pivot_col):
    """
    Generate a pivot table from the given DataFrame.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        index_cols (list): List of column names to be used as the index for the pivot table.
        value_col (str): Name of the column containing the values for the pivot table.
        pivot_col (str): Name of the column to pivot.

    Returns:
        pandas.DataFrame: The resulting pivot table.

    """
    df_pivot = df[index_cols + [value_col, pivot_col]]
    df_pivot_na = df_pivot.dropna(subset=[value_col, pivot_col], how="all")
    df_pivot = pd.pivot(df_pivot_na, index=index_cols, columns=pivot_col, values=value_col).reset_index()

    return df_pivot
