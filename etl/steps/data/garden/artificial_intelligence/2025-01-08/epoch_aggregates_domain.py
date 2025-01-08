"""Generate aggregated table for total yearly and cumulative number of notable AI systems for each domain."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset, last_date_accessed

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("epoch_aggregates_domain.start")

    #
    # Load inputs.
    #
    # Load the ds_meadow dataset.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"]
    tb = tb.reset_index()

    #
    # Process data.
    #

    # Store the origins metadata for later use
    origins = tb["domain"].metadata.origins

    # Select the rows where the 'notability_criteria' column is not null (only consider notable systems)
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
        "notability_criteria",
    ]
    # Drop the unused columns
    tb = tb.drop(unused_columns, axis=1)

    # Convert the 'publication_date' column to datetime format and extract the year
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])
    tb["year"] = tb["publication_date"].dt.year

    # Split the column to be aggregated by comma (several countries/domains can exist in each cell)
    tb["domain"] = tb["domain"].str.split(",")

    # Explode the table to create separate rows for each country or domain
    tb_exploded = tb.explode("domain")

    # Drop duplicates where the year, model and country/domain are the same
    tb_unique = tb_exploded.drop_duplicates(subset=["year", "model", "domain"])

    # Replace domains with less than 10 systems with 'Other'
    domain_counts = tb_unique["domain"].value_counts()

    tb_unique["domain"] = tb_unique["domain"].where(tb_unique["domain"].map(domain_counts) >= 10, "Other")
    # Get the domains that were reclassified to 'Other'
    reclassified_domains = domain_counts[domain_counts < 10].index.tolist()
    domain_counts = tb_unique["domain"].value_counts()

    paths.log.info(
        f"Domains with less than 10 notable systems that were reclassified to 'Other': {', '.join(reclassified_domains)}"
    )
    # Convert the column to category type so that the missing values will be considered as 0
    tb_unique["domain"] = tb_unique["domain"].astype("category")

    # Group by year and country/domain and count the number of systems (consider all categories which will assume 0 for missing values)
    tb_agg = tb_unique.groupby(["year", "domain"], observed=False).size().reset_index(name="yearly_count")

    # Calculate the cumulative count (consider all categories which will assume 0 for missing values)
    tb_agg["cumulative_count"] = tb_agg.groupby("domain", observed=False)["yearly_count"].cumsum()

    # Add the origins metadata to the columns
    for col in ["yearly_count", "cumulative_count"]:
        tb_agg[col].metadata.origins = origins

    # Set the short_name metadata of the table
    tb_agg.metadata.short_name = paths.short_name
    # Set the index to year and domain
    tb_agg = tb_agg.format(["year", "domain"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_agg],
        yaml_params={"date_accessed": last_date_accessed(tb), "year": last_date_accessed(tb)[-4:]},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch_aggregates_domain.end")
