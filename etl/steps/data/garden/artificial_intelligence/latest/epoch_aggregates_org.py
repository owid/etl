"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Generate aggregated table for total yearly and cumulative number of notable AI systems for each domain.

    This function performs the following steps:
    1. Load the epoch dataset.
    2. Assert that the 'publication_date' column is of type datetime64.
    3. Create a 'year' column derived from the 'publication_date' column.
    4. Group the data by year and 'domain' and calculate the counts.
    5. Pivot the table to get counts for each organization type in separate columns.
    6. Melt the dataframe to long format for yearly and cumulative counts.
    7. Merge the yearly and cumulative counts.
    8. Set metadata for the aggregated table.
    9. Create a new dataset with the aggregated table.
    10. Save the new dataset.
    """
    log.info("epoch_aggregates_domain.start")

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

    org_counts = tb.groupby(["year", "organization"]).size().reset_index(name="count")

    # Pivot the table to get the counts for each domain in a separate column
    df_pivot_org = org_counts.pivot(index="year", columns="organization", values="count").reset_index()
    # Replace with cumulative columns
    for column in df_pivot_org.columns:
        if column not in ["year"]:
            df_pivot_org[f"{column}"] = df_pivot_org[column].cumsum()
    # Sum columns except 'year' to get the total number of notable systems
    df_pivot_org["Total"] = df_pivot_org.drop(columns="year").sum(axis=1)
    # Filter the dataframe for the year 2023
    df_2023 = df_pivot_org[df_pivot_org["year"] == 2023]

    # Drop the 'year' column
    df_2023 = df_2023.drop(columns="year")

    # First, let's check how many columns we have, excluding 'Year'
    columns_excluding_year = df_2023.columns.tolist()
    number_of_columns = len(columns_excluding_year)
    # Sorting the columns based on their maximum value in 2023
    sorted_columns = df_2023.max().sort_values(ascending=False).index.tolist()

    # Selecting the top 10 columns, or all columns if there are less than 10
    top_columns = sorted_columns[: min(11, number_of_columns)]

    df_pivot_org = df_pivot_org[["year"] + top_columns]

    # Calculate the shares for each column (excluding 'year' and 'Total')
    df_pivot_org[df_pivot_org.columns.difference(["year", "Total"])] = df_pivot_org[
        df_pivot_org.columns.difference(["year", "Total"])
    ].div(df_pivot_org["Total"], axis=0)

    # Assuming you have already calculated the shares
    df_pivot_org[df_pivot_org.columns.difference(["year", "Total"])] = (
        df_pivot_org[df_pivot_org.columns.difference(["year", "Total"])] * 100
    )
    # List of variables to melt (excluding 'year')
    variables_to_melt = df_pivot_org.columns.difference(["year"]).tolist()

    # Melting the dataframe
    melted_df_cumulative = df_pivot_org.melt(
        id_vars=["year"],
        value_vars=variables_to_melt,
        var_name="organization",
        value_name="cumulative_count",
    )
    # Create table
    tb_agg = melted_df_cumulative.underscore().set_index(["year", "organization"], verify_integrity=True)

    # Add origins metadata to the aggregated table
    for column in tb_agg:
        tb_agg[column].metadata.origins = tb["organization"].metadata.origins
    tb_agg.metadata.short_name = "epoch"
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates_domain.end")
