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
    log.info("epoch_aggregates.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("epoch"))

    # Read table from meadow dataset.
    df = pd.DataFrame(ds_meadow["epoch"])
    # Get year and date
    df["publication_date"] = pd.to_datetime(df["publication_date"])
    df["year"] = df["publication_date"].dt.year
    # Get domain counts
    domain_counts = df.groupby(["year", "domain"]).size().reset_index(name="count")
    df_pivot_domain = domain_counts.pivot(index="year", columns="domain", values="count")
    # Get organization categorisation counts
    organization_categorization_counts = (
        df.groupby(["year", "organization_categorization"]).size().reset_index(name="count")
    )
    df_pivot_org = organization_categorization_counts.pivot(
        index="year", columns="organization_categorization", values="count"
    )

    merged_df = pd.merge(df_pivot_domain, df_pivot_org, on="year").reset_index()
    # Creating a cumulative column
    for column in merged_df.columns:
        if column not in ["year", "country"]:
            merged_df[f"{column}_cumsum"] = merged_df[column].cumsum()
    # Create table
    tb = Table(merged_df, short_name=paths.short_name, underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates.end")
