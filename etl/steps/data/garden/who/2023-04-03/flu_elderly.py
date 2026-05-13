"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("flu_elderly")
    tb = ds_meadow.read("flu_elderly")

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Cap coverage at 100% (admin estimates can exceed it for the reasons noted in meta.yml).
    tb.loc[tb["coverage"] > 100, "coverage"] = 100

    tb = tb.sort_values(["country", "year"])
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
