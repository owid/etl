"""Generate aggregated table for total yearly and cumulative number of compute intensive AI systems for each domain."""

import shared as sh

from etl.catalog_helpers import last_date_accessed
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("epoch_compute_intensive_domain.start")

    #
    # Load inputs.
    #
    # Load the ds_meadow dataset.
    ds_meadow = paths.load_dataset("epoch_compute_intensive")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch_compute_intensive"]
    tb = tb.reset_index()

    #
    # Process data.
    #
    # Define the columns that are not needed
    unused_columns = [
        "authors",
        "country__from_organization",
        "organization",
        "parameters",
        "training_compute__flop",
        "training_dataset_size__datapoints",
    ]

    # Aggregate the data by domain
    tb_agg = sh.calculate_aggregates(tb, "domain", paths.short_name, unused_columns)

    # Set the index to year and domain
    tb_agg = tb_agg.format(["year", "domain"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_agg],
        yaml_params={"date_accessed": last_date_accessed(tb), "year": last_date_accessed(tb)[-4:]},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch_compute_intensive_domain.end")
