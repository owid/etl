"""Load World Inequality Database meadow dataset and create a garden dataset."""


from owid.catalog import Dataset, Table
from shared import add_metadata_vars, add_metadata_vars_distribution
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Data processing function (cleaning and small transformations)
def data_processing(tb: Table) -> Table:
    # Multiply shares by 100
    tb[list(tb.filter(like="share"))] *= 100

    # Delete age and pop, two one-value variables
    tb = tb.drop(columns=["age", "pop", "age_extrapolated", "pop_extrapolated"])

    # Delete some share ratios we are not using, and also the p0p40 (share) variable only available for pretax
    drop_list = ["s90_s10_ratio", "s90_s50_ratio", "p0p40"]

    for var in drop_list:
        tb = tb[tb.columns.drop(list(tb.filter(like=var)))]

    return tb


def run(dest_dir: str) -> None:
    log.info("world_inequality_database.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("world_inequality_database")

    # Read table from meadow dataset.
    tb = ds_meadow["world_inequality_database"]

    #
    # Process data.
    # Change units and drop unnecessary columns
    tb = data_processing(tb)

    # Add metadata by code
    tb = add_metadata_vars(tb)

    ########################################
    # Percentile data
    ########################################

    # Read table from meadow dataset.
    tb_percentiles = ds_meadow["world_inequality_database_distribution"]

    #
    # Process data.
    # Multiple share and share_extrapolated columns by 100
    tb_percentiles[["share", "share_extrapolated"]] *= 100

    # Add metadata by code
    tb_percentiles = add_metadata_vars_distribution(tb_percentiles)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset and add the garden table.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_percentiles], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("world_inequality_database.end")
