"""Thousand-bins global income distribution from World Bank PIP.

Published as CSV and JSON for use by Grapher interactive charts and static charts.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load garden dataset.
    ds_garden = paths.load_dataset("thousand_bins_distribution")

    # Read table from garden dataset.
    tb = ds_garden.read("thousand_bins_distribution", reset_index=False)

    # Initialize a new external dataset.
    ds = paths.create_dataset(tables=[tb])

    # Save external dataset.
    ds.save()
