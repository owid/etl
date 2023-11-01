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

    # Rename columns to avoid confusion with the original columns
    df_pivot_domain = df_pivot_domain.rename(columns={"Not specified": "not_specified_domain", "Other": "other_domain"})

    # Melting the dataframe
    melted_df = df_pivot_domain.melt(
        id_vars=["year"],
        value_vars=[
            "Drawing",
            "Games",
            "Language",
            "Multimodal",
            "not_specified_domain",
            "other_domain",
            "Recommendation",
            "Speech",
            "Vision",
        ],
        var_name="domain",
        value_name="yearly_count",
    )

    # Replace with cumulative columns
    for column in df_pivot_domain.columns:
        if column not in ["year"]:
            df_pivot_domain[f"{column}"] = df_pivot_domain[column].cumsum()
    # Melting the dataframe
    melted_df_cumulative = df_pivot_domain.melt(
        id_vars=["year"],
        value_vars=[
            "Drawing",
            "Games",
            "Language",
            "Multimodal",
            "not_specified_domain",
            "other_domain",
            "Recommendation",
            "Speech",
            "Vision",
        ],
        var_name="domain",
        value_name="cumulative_count",
    )

    df_merged = pr.merge(melted_df_cumulative, melted_df, on=["year", "domain"]).copy_metadata(from_table=tb)
    # Create table
    tb_agg = df_merged.underscore().set_index(["year", "domain"], verify_integrity=True)

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
