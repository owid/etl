"""Load a meadow dataset and create a garden dataset.

- Load meadow dataset
- Harmonize countries
- Cap vaccination coverage values at 100%
- Sort values and set index
- Create Table and Dataset
"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("flu_elderly.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("flu_elderly")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["flu_elderly"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    log.info("flu_elderly.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # Coverage values over 100% are capped at 100%
    df.loc[df["coverage"] > 100, "coverage"] = 100

    df = df.sort_values(["country", "year"])
    df = df.set_index(["country", "year"], verify_integrity=True)

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("flu_elderly.end")
