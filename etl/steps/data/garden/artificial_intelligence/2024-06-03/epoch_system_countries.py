"""Load a meadow dataset and create a garden dataset."""
import pandas as pd
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Short name
SHORT_NAME = paths.short_name


def run(dest_dir: str) -> None:
    log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
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
        "domain",
        "authors",
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
    tb["country__from_organization"] = tb["country__from_organization"].str.split(",")

    # Explode the table to create separate rows for each country
    tb_exploded = tb.explode("country__from_organization")

    # Drop duplicates where the year, system and country are the same
    tb_unique = tb_exploded.drop_duplicates(subset=["year", "system", "country__from_organization"])

    # Group by year and country and count the number of systems
    country_year_tb = (
        tb_unique.groupby(["year", "country__from_organization"]).size().reset_index(name="number_of_systems")
    )

    # Rename the 'country__from_organization' column to 'country'
    country_year_tb = country_year_tb.rename(columns={"country__from_organization": "country"})

    # Add the origins metadata to the 'number_of_systems' column
    country_year_tb["number_of_systems"].metadata.origins = origins

    # Set the short_name metadata of the table
    country_year_tb.metadata.short_name = SHORT_NAME

    # Harmonize the country names
    country_year_tb = geo.harmonize_countries(df=country_year_tb, countries_file=paths.country_mapping_path)

    # Set the index to year and country
    country_year_tb = country_year_tb.set_index(["year", "country"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[country_year_tb])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch.end")
