"""Historical thousand-bin distributions (1820–present), hybrid and all-lognormal variants."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load garden dataset.
    ds_garden = paths.load_dataset("historical_poverty")

    # Read the two thousand-bin tables from garden dataset.
    tables = [
        ds_garden.read("thousand_bins_interpolated_ginis", reset_index=False),
        ds_garden.read("thousand_bins_interpolated_ginis_all_lognormal", reset_index=False),
    ]

    # Initialize a new external dataset.
    # Garden tables are already repacked, so skip it here to avoid the cost.
    ds = paths.create_dataset(tables=tables, repack=False)

    # Save external dataset.
    ds.save()
