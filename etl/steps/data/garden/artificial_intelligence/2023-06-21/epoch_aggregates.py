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
    tb = ds_meadow["epoch"]
    print("K")
    df = pd.DataFrame(tb)
    df["publication_date"] = pd.to_datetime(df["publication_date"])
    df["year"] = df["publication_date"].dt.year
    domain_counts = df.groupby(["year", "domain"]).size().reset_index(name="count")
    organization_categorization_counts = (
        df.groupby(["year", "organization_categorization"]).size().reset_index(name="count")
    )
    df_pivot_domain = domain_counts.pivot(index="year", columns="domain", values="count")
    df_pivot_org = organization_categorization_counts.pivot(
        index="year", columns="organization_categorization", values="count"
    )
    merged_df = pd.merge(df_pivot_domain, df_pivot_org, on="year")
    merged_df.reset_index(inplace=True)

    # Create table
    tb = Table(merged_df, short_name="epoch_aggregates", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates.end")
