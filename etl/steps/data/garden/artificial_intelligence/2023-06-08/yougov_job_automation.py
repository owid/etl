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
    log.info("yougov_job_automation.start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("yougov_job_automation"))

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_job_automation"]
    df = pd.DataFrame(tb)
    #
    # Process data.
    #

    # Create a date column (counting days since 2021-01-01)
    df["days_since_2021"] = (
        pd.to_datetime(df["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2021-01-01")
    ).dt.days
    df = df.drop("date", axis=1)

    # Create a pivot table for each demographic group
    pivot_df = df.pivot(
        index=["group", "days_since_2021"],
        columns="how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime",
        values="value",
    ).reset_index()
    pivot_df = pivot_df.rename_axis(None, axis=1)
    rename_entries = {
        "18-29": "18-29 years",
        "2-year": "2-year post-secondary education",
        "30-44": "30-44 years",
        "4-year": "4-year post-secondary education",
        "45-64": "45-64 years",
        "65+": "65+ years",
        "High school graduate": "High school graduates",
        "No HS": "No high school education",
        "Post-grad": "Post-graduate education",
    }
    pivot_df["group"] = pivot_df["group"].replace(rename_entries)

    tb = Table(pivot_df, short_name="yougov_job_automation", underscore=True)
    tb.set_index(["group", "days_since_2021"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_job_automation.end")
