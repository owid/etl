"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unwto_environment.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("unwto_environment")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["unwto_environment"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("unwto_environment.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # replace '<NA>' with np.nan
    df = df.replace("<NA>", np.nan)
    # drop rows with np.nan values
    df = df.dropna()
    df.rename(
        columns={df.columns[2]: "seea_tables", df.columns[3]: "tsa_tables", df.columns[4]: "total_tables"}, inplace=True
    )
    # Create a new table with the processed data.
    tb_garden = Table(df.reset_index(drop=True), short_name="unwto_environment")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("unwto_environment.end")
