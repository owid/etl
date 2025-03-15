"""Generate aggregated table for total yearly and cumulative number of notable AI systems in each category of researcher affiliation."""

from etl.catalog_helpers import last_date_accessed
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("epoch_aggregates_affiliation.start")

    #
    # Load inputs.
    #
    # Load the the garden dataset without aggregations.
    ds_garden = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_garden["epoch"]
    tb = tb.reset_index()

    #
    # Process data.
    #
    # Store the origins metadata for later use
    origins = tb["organization_categorization"].metadata.origins

    # Define the columns that are not needed
    unused_columns = [
        "days_since_1949",
        "parameters",
        "training_dataset_size__datapoints",
        "domain",
        "training_computation_petaflop",
    ]
    # Drop the unused columns
    tb = tb.drop(unused_columns, axis=1)

    # Ensure 'publication_date' column type is datetime64
    assert tb["publication_date"].dtype == "datetime64[ns]", "publication_date column is not of type datetime64"

    # Extract the year from the 'publication_date' column
    tb["year"] = tb["publication_date"].dt.year

    # Group by year and country and count the number of systems
    tb_agg = tb.groupby(["year", "organization_categorization"], observed=False).size().reset_index(name="yearly_count")

    # Calculate the cumulative count
    tb_agg["cumulative_count"] = tb_agg.groupby("organization_categorization", observed=False)["yearly_count"].cumsum()

    # Add the origins metadata to the columns
    for col in ["yearly_count", "cumulative_count"]:
        tb_agg[col].metadata.origins = origins

    # Set the short_name metadata of the table
    tb_agg.metadata.short_name = paths.short_name

    # Set the index to year and country
    tb_agg = tb_agg.format(["year", "organization_categorization"])

    #
    # Save outputs.
    #
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_agg],
        yaml_params={"date_accessed": last_date_accessed(tb), "year": last_date_accessed(tb)[-4:]},
    )

    ds_garden.save()

    paths.log.info("epoch_aggregates_affiliation.end")
