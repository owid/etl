"""Load a meadow dataset and create a garden dataset."""


import pandas as pd
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
    ds_meadow = paths.load_dataset("yougov_job_automation")

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_job_automation"].reset_index()
    #
    # Process data.
    #
    # Create a date column (counting days since 2021-01-01)
    tb["days_since_2021"] = (
        pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2021-01-01")
    ).dt.days
    tb = tb.drop("date", axis=1)

    # Create a pivot table for each demographic group
    pivot_tb = tb.pivot(
        index=["group", "days_since_2021"],
        columns="how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime",
        values="value",
    ).reset_index()

    pivot_tb = pivot_tb.rename_axis(None, axis=1)
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
    pivot_tb["group"] = pivot_tb["group"].replace(rename_entries)

    pivot_tb = pivot_tb.underscore().set_index(["group", "days_since_2021"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[pivot_tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_job_automation.end")
