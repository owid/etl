"""Generate aggregated table for total yearly and cumulative number of notable AI systems for each organization."""

import shared as sh

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("epoch_aggregates_organizations.start")

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

    # Define the columns that are not needed
    unused_columns = [
        "domain",
        "authors",
        "country__from_organization",
        "organization_categorization",
        "parameters",
        "training_compute__flop",
        "training_dataset_size__datapoints",
        "notability_criteria",
    ]

    # Aggregate the data by organization
    tb_agg = sh.calculate_aggregates(tb, "organization", paths.short_name, unused_columns)

    # Set the index to year and organization
    tb_agg = tb_agg.format(["year", "organization"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch_aggregates_organizations.end")
