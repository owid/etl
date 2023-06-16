"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_math_code_language.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("papers_with_code_math_code_language"))

    # Read table from meadow dataset.
    tb = ds_meadow["papers_with_code_math_code_language"]
    df = pd.DataFrame(tb)

    df["days_since_2019"] = (
        pd.to_datetime(df["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days
    df = df.drop("date", axis=1)
    pivot_df = select_best_on_date(df)
    print(pivot_df)

    #
    # Process data.
    #
    tb = Table(pivot_df, short_name="papers_with_code_math_code_language")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_math_code_language.end")


def select_best_on_date(df):
    max_df = []

    # Loop through each year
    for year in df["days_since_2019"].unique():
        # Filter the DataFrame for the current year
        year_data = df[df["days_since_2019"] == year]
        # Loop through each column (excluding 'name' and 'days_since_2019')
        for column in year_data.columns:
            print(year_data)
            if column not in ["name", "days_since_2019"]:
                # Check if there is at least one non-NaN value in the column for the current year
                if not year_data[column].isnull().all():
                    print(year_data[column])
                    # Find the model with the highest performance and its value for the current column
                    max_value = year_data[column].max()
                    # Find the name that corresponds to this maximum value
                    max_name = year_data.loc[year_data[column].idxmax(), "name"]
                    # Append the year, column (performance), name, and maximum value to max_df
                    max_df.append(
                        {"days_since_2019": year, "performance": column, "name": max_name, "accuracy": max_value}
                    )

    # Convert the result to DataFrame
    result_df_max = pd.DataFrame(max_df)

    pivot_df = result_df_max.pivot_table(
        values="accuracy", index=["days_since_2019", "name"], columns="performance", aggfunc=max
    )

    pivot_df.reset_index(inplace=True)
    pivot_df.columns.name = None  # remove the name of columns
    print(pivot_df.columns)

    return pivot_df
