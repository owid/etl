"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("dummy.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("dummy")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["dummy"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("dummy.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("dummy.end")
