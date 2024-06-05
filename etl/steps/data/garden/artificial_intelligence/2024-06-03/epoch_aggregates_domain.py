"""Load a meadow dataset and create a garden dataset."""
import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Short name
SHORT_NAME = paths.short_name


def run(dest_dir: str) -> None:
    """
    Generate aggregated table for total yearly and cumulative number of notable AI systems for each domain.
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
    #
    # Process data.
    #
    # Filter out rows where 'notability_criteria' is NaN and reset the index
    tb = tb[tb["notability_criteria"].notna()].reset_index(drop=True)

    # Define the columns that are not needed
    unused_columns = [
        "authors",
        "country__from_organization",
        "organization",
        "organization_categorization",
        "parameters",
        "training_compute__flop",
        "training_dataset_size__datapoints",
        "training_time__hours",
        "notability_criteria",
    ]
    # Store the origins metadata for later use
    origins = tb["domain"].metadata.origins

    # Drop the unused columns
    tb = tb.drop(unused_columns, axis=1)

    # Convert the 'publication_date' column to datetime format and extract the year
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])
    tb["year"] = tb["publication_date"].dt.year

    # Split the 'country__from_organization' column by comma (several countries can exist in each cell)
    tb["domain"] = tb["domain"].str.split(",")

    # Explode the table to create separate rows for each country
    tb_exploded = tb.explode("domain")

    # Drop duplicates where the year, system and country are the same
    tb_unique = tb_exploded.drop_duplicates(subset=["year", "system", "domain"])

    # Group by year and country and count the number of systems
    tb_agg = tb_unique.groupby(["year", "domain"]).size().reset_index(name="yearly_count")

    # Sort the DataFrame by year and domain
    tb_agg = tb_agg.sort_values(["domain", "year"])

    # Calculate the cumulative count
    tb_agg["cumulative_count"] = tb_agg.groupby("domain")["yearly_count"].cumsum()

    # Add the origins metadata to the columns
    for col in ["yearly_count", "cumulative_count"]:
        tb_agg[col].metadata.origins = origins

    # Set the short_name metadata of the table
    tb_agg.metadata.short_name = SHORT_NAME

    # Set the index to year and country
    tb_agg = tb_agg.set_index(["year", "domain"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates_domain.end")
