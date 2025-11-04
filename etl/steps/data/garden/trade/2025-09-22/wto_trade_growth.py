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
    print(tb_historic)
    print(tb_wto)

    # First, re-index historic data from 1913 = 100 to 1800 = 1
    baseline_1800_historic = tb_historic[tb_historic["year"] == 1800]["volume_index"].iloc[0]
    tb_historic_reindexed = tb_historic.copy()
    tb_historic_reindexed["volume_index"] = tb_historic_reindexed["volume_index"] / baseline_1800_historic

    # Get the 1950 value from the re-indexed historic data (now 1800 = 1)
    baseline_1950_reindexed = tb_historic_reindexed[tb_historic_reindexed["year"] == 1950]["volume_index"].iloc[0]

    # Now scale WTO data (1950 = 100) to match the re-indexed scale (1800 = 1)
    tb_wto_adj = tb_wto.copy()
    tb_wto_adj["volume_index"] = tb_wto_adj["volume_index"] * baseline_1950_reindexed / 100

    # Combine: both are now on the same 1800 = 1 scale
    tb_combined = pr.concat(
        [tb_wto_adj[tb_wto_adj["year"] > 1950], tb_historic_reindexed[tb_historic_reindexed["year"] <= 1950]],
        ignore_index=True,
    ).sort_values("year")
    print(tb_combined)

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
