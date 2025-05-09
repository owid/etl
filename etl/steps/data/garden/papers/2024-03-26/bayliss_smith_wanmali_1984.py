"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("bayliss_smith_wanmali_1984")
    tb = ds_meadow.read("long_term_wheat_yields")

    #
    # Process data.
    #
    # Years are given as strings of intervals, e.g. "1909-1913". Convert them into the average year.
    tb["year"] = [np.array(interval.split("-")).astype(int).mean().astype(int) for interval in tb["year"]]

    # Convert from 100kg per hectare to tonnes per hectare.
    tb["wheat_yield"] *= 0.1

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
