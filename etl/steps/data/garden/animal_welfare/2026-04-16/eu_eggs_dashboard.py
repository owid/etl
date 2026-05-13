"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_2025_01 = paths.load_dataset("eu_eggs_dashboard_2025_01")
    tb_2025_01 = ds_2025_01.read("eu_eggs_dashboard_2025_01")

    ds_2026_04 = paths.load_dataset("eu_eggs_dashboard_2026_04")
    tb_2026_04 = ds_2026_04.read("eu_eggs_dashboard_2026_04")

    #
    # Process data.
    #
    # Harmonize country names.
    tb_2025_01 = paths.regions.harmonize_names(tb_2025_01)
    tb_2026_04 = paths.regions.harmonize_names(tb_2026_04)

    # Combine both dashboard versions. More recent data takes priority for any duplicate (country, year) pairs.
    tb = pr.concat([tb_2026_04, tb_2025_01], ignore_index=True, short_name=paths.short_name)
    tb = tb.drop_duplicates(subset=["country", "year"], keep="first")

    # Convert percentage columns into absolute hen counts.
    # Conventional cages have been banned in the EU since 2012; dashboard cage counts are enriched cages.
    tb["not_enriched_cage"] = tb["total"] * 0
    tb["enriched_cage"] = (tb["total"] * tb["pct_enriched_cage"] / 100).round().astype(int)
    tb["barn"] = (tb["total"] * tb["pct_barn"] / 100).round().astype(int)
    tb["free_range"] = (tb["total"] * tb["pct_free_range"] / 100).round().astype(int)
    tb["organic"] = (tb["total"] * tb["pct_organic"] / 100).round().astype(int)
    tb = tb.drop(columns=["pct_enriched_cage", "pct_barn", "pct_free_range", "pct_organic"])

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_2026_04.metadata)
    ds_garden.save()
