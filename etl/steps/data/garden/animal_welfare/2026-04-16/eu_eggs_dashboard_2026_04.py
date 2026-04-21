"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("eu_eggs_dashboard_2026_04")
    tb = ds_meadow.read("eu_eggs_dashboard_2026_04")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Convert percentage columns into absolute hen counts.
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
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
