"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets.
    ds_wto = paths.load_dataset("wto_trade_growth")
    ds_historic = paths.load_dataset("historic_trade")

    # Read tables from meadow datasets.
    tb_wto = ds_wto.read("wto_trade_growth")
    tb_historic = ds_historic.read("historic_trade")

    #
    # Process data.
    #

    # Build overlap

    # Get the 1950 baseline value from historic data (now indexed to 1800 = 1)
    baseline_1950 = tb_historic[tb_historic["year"] == 1950]["volume_index"].iloc[0]

    # Calculate WTO adjusted values for years after 1950
    # WTO data is indexed to 100, so divide by 100 to get the multiplier
    tb_wto_adj = tb_wto.copy()
    tb_wto_adj["volume_index"] = baseline_1950 * tb_wto_adj["volume_index"] / 100

    # Combine historic data (up to 1950) with adjusted WTO data (after 1950)
    tb_combined = pr.concat(
        [tb_wto_adj[tb_wto_adj["year"] > 1950], tb_historic[tb_historic["year"] <= 1950]],
        ignore_index=True,
    ).sort_values("year")

    # Re-index to 1800 = 1 instead of 1913 = 100
    # This makes values directly interpretable as "X times larger than 1800"
    baseline_1800 = tb_combined[tb_combined["year"] == 1800]["volume_index"].iloc[0]
    tb_combined["volume_index"] = tb_combined["volume_index"] / baseline_1800

    # Combine the datasets
    tb_combined = tb_combined.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined], default_metadata=ds_wto.metadata)

    # Save garden dataset.
    ds_garden.save()


def power(x, a, b):
    return a * (x**b)


def expo(x, a, b):
    return a * np.exp(b * x)
