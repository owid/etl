"""Garden step for SDG 9.1.1 - Rural Road Access.

Proportion of the rural population who live within 2 km of an all-season road.
Reads directly from the latest un_sdg garden dataset.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("sdg_9_1_1")
    tb = ds_meadow.read("sdg_9_1_1", reset_index=True)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    tb["rural_road_access"] = tb["value"].copy()
    tb = tb.drop(columns=["value"])
    tb = tb.format(["country", "year"], short_name="sdg_9_1_1")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
