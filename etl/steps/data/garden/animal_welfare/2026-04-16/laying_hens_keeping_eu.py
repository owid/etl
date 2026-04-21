"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("laying_hens_keeping_eu")
    tb = ds_meadow.read("laying_hens_keeping_eu")

    ds_dashboard = paths.load_dataset("eu_eggs_dashboard_2026_04")
    tb_dashboard = ds_dashboard.read("eu_eggs_dashboard_2026_04")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Combine historical data (2011-2021) with newer data from the dashboard.
    # Old data takes priority for any duplicate (country, year) pairs.
    tb = pr.concat([tb, tb_dashboard], ignore_index=True, short_name=paths.short_name)
    tb = tb.drop_duplicates(subset=["country", "year"], keep="first")

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
