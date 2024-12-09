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
    log.info("happiness.start")

    #
    # Load inputs.
    #
    # Load meadow datasets for all previous reports.
    versions = [
        "2023-03-20",
        "2022-03-20",
        "2021-03-20",
        "2020-03-20",
        "2019-03-20",
        "2018-03-20",
        "2017-03-20",
        "2016-03-20",
        "2015-03-20",
        "2012-03-20",
    ]

    df = pd.DataFrame()
    for version in versions:
        ds_meadow: Dataset = paths.load_dependency(short_name="happiness", version=version)
        # Read table from meadow dataset.
        tb_meadow = ds_meadow["happiness"]
        df = pd.concat([df, tb_meadow])

    # The report give values from survey data of the three previous years, which we report as the last survey year - consistently 1 year prior to the publication year
    df["year"] = df["report_year"] - 1
    df = df.drop(columns=["report_year"])
    # Create a dataframe with data from the table.
    #
    # Process data.
    #
    log.info("happiness.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    df = df.reset_index(drop=True)
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="happiness")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("happiness.end")
