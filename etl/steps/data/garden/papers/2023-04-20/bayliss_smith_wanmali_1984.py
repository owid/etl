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
    log.info("bayliss_smith_wanmali_1984.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("bayliss_smith_wanmali_1984")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["long_term_wheat_yields"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    # Years are given as strings of intervals, e.g. "1909-1913". Convert them into the average year.
    df["year"] = [np.array(interval.split("-")).astype(int).mean().astype(int) for interval in df["year"]]

    # Convert from 100kg per hectare to tonnes per hectare.
    df["wheat_yield"] *= 0.1

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("bayliss_smith_wanmali_1984.end")
