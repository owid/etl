"""Load snapshot and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data from snapshot.
    #
    ds_eu_agg = paths.load_dataset("eu_wild_bird_populations")
    ds_farmland = paths.load_dataset("farmland_bird_index")

    tb = ds_eu_agg.read("eu_wild_bird_populations")
    tb_farmland = ds_farmland.read("farmland_bird_index")

    #
    # Process EU aggregated data.
    #
    # rename species
    tb["species"] = tb["species"].replace(
        {"CO_ALL": "All common species", "CO_FARM": "Common farmland species", "CO_FOR": "Common forest species"}
    )
    # turn wide to long
    tb = tb.melt(id_vars=["country", "species", "index_year"], value_name="bird_population_index", var_name="year")
    # change year from _1990 to 1990
    tb["year"] = tb["year"].str.replace("_", "").astype(int)

    #
    # Process farmland bird index data.
    #
    tb_farmland = paths.regions.harmonize_names(tb_farmland)
    tb_farmland = tb_farmland.melt(id_vars=["country"], value_name="farmland_bird_index", var_name="year")
    tb_farmland["year"] = tb_farmland["year"].str.replace("_", "").astype(int)

    # drop na values
    tb_farmland = tb_farmland.replace({":": pd.NA})
    tb_farmland = tb_farmland.dropna(subset=["farmland_bird_index"])

    # Format tables
    tb = tb.format(["country", "species", "index_year", "year"])
    tb_farmland = tb_farmland.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb, tb_farmland], default_metadata=ds_eu_agg.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
