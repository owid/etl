"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("semiconductors_cset")
    tb = ds_meadow.read("semiconductors_cset")

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)
    tb["year"] = 2021

    tb = tb.pivot(
        index=["country", "year"], columns="provided_name", values="share_provided", join_column_levels_with="_"
    )
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
