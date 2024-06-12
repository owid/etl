from typing import List

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def calculate_aggregates(tb: Table, agg_column: str, short_name: str, unused_columns: List[str]) -> Table:
    """
    This function calculates aggregates for a given column in a Table. It is used to calculate the total yearly and cumulative number of notable AI systems for each domain or country.

    Parameters:
    tb (Table): The input Table.
    agg_column (str): The column to aggregate on.
    short_name (str): The short name to set for the table.
    unused_columns (List[str]): The list of columns to drop from the table.

    Returns:
    Table: The output Table with calculated aggregates.
    """
    # Select the rows where the 'notability_criteria' column is not null (only consider notable systems)
    tb = tb[tb["notability_criteria"].notna()].reset_index(drop=True)

    # Store the origins metadata for later use
    origins = tb[agg_column].metadata.origins

    # Drop the unused columns
    tb = tb.drop(unused_columns, axis=1)

    # Convert the 'publication_date' column to datetime format and extract the year
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])
    tb["year"] = tb["publication_date"].dt.year

    # Split the column to be aggregated by comma (several countries/domains can exist in each cell)
    tb[agg_column] = tb[agg_column].str.split(",")

    # Explode the table to create separate rows for each country or domain
    tb_exploded = tb.explode(agg_column)

    # Drop duplicates where the year, system and country/domain are the same
    tb_unique = tb_exploded.drop_duplicates(subset=["year", "system", agg_column])

    # Replace system domains with less than 20 notable systems with 'Other'
    if agg_column == "domain":
        # Replace domains with less than 20 systems with 'Other'
        domain_counts = tb_unique["domain"].value_counts()

        tb_unique["domain"] = tb_unique["domain"].where(tb_unique["domain"].map(domain_counts) >= 20, "Other")
        # Get the domains that were reclassified to 'Other'
        reclassified_domains = domain_counts[domain_counts < 20].index.tolist()
        domain_counts = tb_unique["domain"].value_counts()

        paths.log.info(
            f"Domains with less than 20 notable systems that were reclassified to 'Other': {', '.join(reclassified_domains)}"
        )
    # Convert the column to category type so that the missing values will be considered as 0
    tb_unique[agg_column] = tb_unique[agg_column].astype("category")

    # Group by year and country/domain and count the number of systems (consider all categories which will assume 0 for missing values)
    tb_agg = tb_unique.groupby(["year", agg_column], observed=False).size().reset_index(name="yearly_count")

    # Add the origins metadata to the 'number_of_systems' column
    tb_agg["yearly_count"].metadata.origins = origins

    # Calculate the cumulative count (consider all categories which will assume 0 for missing values)
    tb_agg["cumulative_count"] = tb_agg.groupby(agg_column, observed=False)["yearly_count"].cumsum()

    # Add the origins metadata to the columns
    for col in ["yearly_count", "cumulative_count"]:
        tb_agg[col].metadata.origins = origins

    # Set the short_name metadata of the table
    tb_agg.metadata.short_name = short_name

    return tb_agg
