"""Load a meadow dataset and create a garden dataset."""
import shared as sh
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
    short_name = SHORT_NAME

    # Aggregate the data by domain
    tb_agg = sh.calculate_aggregates(tb, "domain", short_name, unused_columns)

    # Set the index to year and domain
    tb_agg = tb_agg.set_index(["year", "domain"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch_aggregates_domain.end")
