"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Generate aggregated table for total yearly and cumulative number of notable AI systems for each machine learning approach.

    This function performs the following steps:
    1. Load the epoch dataset.
    2. Assert that the 'publication_date' column is of type datetime64.
    3. Create a 'year' column derived from the 'publication_date' column.
    4. Group the data by year and 'approach' and calculate the counts.
    5. Pivot the table to get counts for each approach type in separate columns.
    6. Melt the dataframe to long format for yearly and cumulative counts.
    7. Merge the yearly and cumulative counts.
    8. Set metadata for the aggregated table.
    9. Create a new dataset with the aggregated table.
    10. Save the new dataset.
    """
    log.info("epoch_aggregates_approach.start")

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
    # Get approach counts
    approach_counts = tb.groupby(["year", "approach"]).size().reset_index(name="count")

    # Pivot the table to get the counts for each approach type in a separate column
    df_pivot_approach = approach_counts.pivot(index="year", columns="approach", values="count").reset_index()

    # Melting the dataframe
    melted_df = df_pivot_approach.melt(
        id_vars=["year"],
        value_vars=[
            "Not specified",
            "Reinforcement learning",
            "Self-supervised learning",
            "Supervised",
            "Unsupervised",
        ],
        var_name="approach",
        value_name="yearly_count",
    )

    # Replace with cumulative columns
    for column in df_pivot_approach.columns:
        if column not in ["year"]:
            df_pivot_approach[f"{column}"] = df_pivot_approach[column].cumsum()
    # Melting the dataframe
    melted_df_cumulative = df_pivot_approach.melt(
        id_vars=["year"],
        value_vars=[
            "Not specified",
            "Reinforcement learning",
            "Self-supervised learning",
            "Supervised",
            "Unsupervised",
        ],
        var_name="approach",
        value_name="cumulative_count",
    )

    df_merged = pr.merge(melted_df_cumulative, melted_df, on=["year", "approach"]).copy_metadata(from_table=tb)
    # Create table
    tb_agg = df_merged.underscore().set_index(["year", "approach"], verify_integrity=True)

    # Add origins metadata to the aggregated table
    for column in tb_agg:
        tb_agg[column].metadata.origins = tb["approach"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates_approach.end")
