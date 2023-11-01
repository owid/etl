"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
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
    # Load garden dataset with no aggregations.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"]
    tb = tb.reset_index()
    # First, assert that the publication_date column is of type datetime64
    assert tb["publication_date"].dtype == "datetime64[ns]", "publication_date column is not of type datetime64"
    # Create a year column
    tb["year"] = tb["publication_date"].dt.year
    # Get domain counts
    domain_counts = tb.groupby(["year", "domain"]).size().reset_index(name="count")

    # Pivot the table to get the counts for each domain in a separate column
    df_pivot_domain = domain_counts.pivot(index="year", columns="domain", values="count").reset_index()
    # Get organization categorisation counts
    organization_categorization_counts = (
        tb.groupby(["year", "organization_categorization"]).size().reset_index(name="count")
    )
    # Pivot the table to get the counts for each organization type in a separate column
    df_pivot_org = organization_categorization_counts.pivot(
        index="year", columns="organization_categorization", values="count"
    ).reset_index()

    # Get approach counts
    approach_counts = tb.groupby(["year", "approach"]).size().reset_index(name="count")

    # Pivot the table to get the counts for each approach type in a separate column
    df_pivot_approach = approach_counts.pivot(index="year", columns="approach", values="count").reset_index()

    # Rename columns to avoid confusion with the original columns
    df_pivot_domain = df_pivot_domain.rename(columns={"Not specified": "not_specified_domain", "Other": "other_domain"})
    df_pivot_org = df_pivot_org.rename(
        columns={
            "Not specified": "not_specified_organization_categorization",
            "Other": "other_organization_categorization",
        }
    )

    df_pivot_approach = df_pivot_approach.rename(columns={"Not specified": "not_specified_approach"})
    # Merge the dataframes
    merged_df = pr.merge(df_pivot_domain, df_pivot_org, on="year").copy_metadata(from_table=tb)
    merged_df = pr.merge(df_pivot_approach, merged_df, on="year").copy_metadata(from_table=tb)
    # Create cumulative columns
    for column in merged_df.columns:
        if column not in ["year"]:
            merged_df[f"{column}_cumsum"] = merged_df[column].cumsum()
    # Create table
    tb_agg = merged_df.underscore().set_index("year", verify_integrity=True)

    # Add origins metadata to the aggregated table
    for column in tb_agg:
        tb_agg[column].metadata.origins = tb["domain"].metadata.origins
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates.end")
