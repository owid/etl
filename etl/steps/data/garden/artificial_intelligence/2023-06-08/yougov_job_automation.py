"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_job_automation.start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("yougov_job_automation"))

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_job_automation"]
    df = pd.DataFrame(tb)
    # First, create a mapping of the worry responses to the new categories.
    worry_mapping = {
        "Very worried": "Expressed worry",
        "Fairly worried": "Expressed worry",
        "Not very worried": "Not expressed worry",
        "Not worried at all": "Not expressed worry",
    }

    # Apply the mapping to create a new column for the combined categories.
    df["worry_category"] = df[
        "how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime"
    ].map(worry_mapping)

    # Now we can drop the rows where 'worry_category' is NaN (i.e., the 'Don't know' rows).
    df = df.dropna(subset=["worry_category"])

    # Group by the 'date', 'group', and new 'worry_category' columns, and sum the 'value' column within each group.
    df = df.groupby(["date", "group", "worry_category"]).value.sum().reset_index()

    #
    # Process data.
    #

    # Create a date column (counting days since 2021-01-01)
    df["days_since_2021"] = (
        pd.to_datetime(df["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2021-01-01")
    ).dt.days
    df = df.drop("date", axis=1)
    df = assign_indicator(df, "group")

    df = reshape_dataframe(df, "indicator_group", "value")

    tb = Table(df, short_name="yougov_job_automation", underscore=True)
    tb.set_index(["days_since_2021", "group"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_job_automation.end")


# Adjusted function to assign indicator
def assign_indicator(df, column_name):
    conditions = []
    categories = []

    for method, mapping in condition_category_mapping.items():
        for category, values in mapping.items():
            if method == "isin":
                conditions.append(df[column_name].isin(values))
            elif method == "contains":
                conditions.append(df[column_name].str.contains("|".join(values)))
            categories.append(category)

    df["indicator_group"] = np.select(conditions, categories, default="All adults")
    return df


condition_category_mapping = {
    "isin": {
        "Age": ["18-29", "30-44", "45-64", "65+"],
        "Race": [
            "White",
            "Black",
            "Hispanic",
            "Asian",
            "Native American",
            "Middle Eastern",
            "Two or more races",
            "Other",
        ],
        "Gender": ["Male", "Female"],
        "Education": ["No HS", "High school graduate", "Some college", "2-year", "4-year", "Post-grad"],
        "Political Affiliation": ["Democrat", "Republican", "Independent"],
    }
}


def reshape_dataframe(df, col_categories, col_values):
    reshaped_df = df.pivot(
        index=[
            "days_since_2021",
            "group",
        ],
        columns=[
            col_categories,
            "worry_category",
        ],
        values=col_values,
    )
    reshaped_df.reset_index(inplace=True)

    reshaped_df.columns = [" ".join(col).strip() for col in reshaped_df.columns.values]

    return reshaped_df
