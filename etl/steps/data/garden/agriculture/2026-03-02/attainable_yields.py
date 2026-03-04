"""Combine attainable yields from Mueller et al. (2012) with the latest FAOSTAT yields data.

The resulting dataset contains:
1. item_yield: Yield from the Long-term crop yields dataset (e.g. barley_yield).
2. item_attainable_yield: Maximum attainable yield from Mueller et al. (2012) (e.g. barley_attainable_yield).
3. item_yield_gap: Yield gap, which is the difference between the previous two (e.g. barley_yield_gap).

Elements 2 and 3 are provided only for items that were included in Mueller et al. (2012), whereas element 1 is
provided also for other items.

This dataset will be imported by the crop_yields explorers step, which feeds our Crop Yields explorer:
https://ourworldindata.org/explorers/crop-yields
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# How to adjust items in the Long-term crop yields dataset to coincide with the names in Mueller et al. (2012).
COLUMNS = {
    "almonds_yield": "almond_yield",
    "bananas_yield": "banana_yield",
    "beans__dry_yield": "bean_yield",
    "cereals_yield": "cereal_yield",
    "cocoa_beans_yield": "cocoa_yield",
    "coffee__green_yield": "coffee_yield",
    "oranges_yield": "orange_yield",
    "peas__dry_yield": "pea_yield",
    "tomatoes_yield": "tomato_yield",
    "seed_cotton_yield": "cotton_yield",
    "groundnuts_yield": "groundnut_yield",
    "palm_fruit_oil_yield": "oilpalm_yield",
    "potatoes_yield": "potato_yield",
    "soybeans_yield": "soybean_yield",
    "sugar_beet_yield": "sugarbeet_yield",
    "sugar_cane_yield": "sugarcane_yield",
    "sunflower_seed_yield": "sunflower_yield",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load attainable yields data from Mueller et al. (2012), and read its main table.
    ds_mueller = paths.load_dataset("mueller_et_al_2012")
    tb_mueller = ds_mueller.read("mueller_et_al_2012")

    # Load long-term crop yields dataset and read its main table.
    ds_yields = paths.load_dataset("long_term_crop_yields")
    tb_yields = ds_yields.read("long_term_crop_yields")

    #
    # Process data.
    #
    # Rename columns from the long-term crop yields dataset, to coincide with the names of Mueller et al. (2012).
    tb_yields = tb_yields.rename(columns=COLUMNS, errors="raise")

    # Combine both tables.
    tb = tb_yields.merge(tb_mueller.drop(columns=["year"], errors="raise"), on=["country"], how="outer")

    # Remove rows that only appeared in Mueller et al. (2012), since they would have no yearly data.
    tb = tb.dropna(subset="year").reset_index(drop=True)

    # Add the yield gap (difference between maximum attainable yields minus actual yield).
    for item in [
        column.replace("_attainable_yield", "") for column in tb.columns if column.endswith("_attainable_yield")
    ]:
        # Clip the series at zero (negative values mean that the yield has been attained).
        tb[f"{item}_yield_gap"] = (tb[f"{item}_attainable_yield"] - tb[f"{item}_yield"]).clip(0)
        # Check that origins are propagated correctly.
        assert set(tb[f"{item}_attainable_yield"].m.origins) | set(tb[f"{item}_yield"].m.origins) == set(
            tb[f"{item}_yield_gap"].m.origins
        )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
