"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Item code for "Meat, total" (used only for sanity checks).
MEAT_TOTAL_ITEM_CODE = "00001765"
MEAT_TOTAL_LABEL = "all land animals"

# Item codes that should add up to "Meat, Total", as well as meat total itself.
MEAT_TOTAL_ITEM_CODES = {
    MEAT_TOTAL_ITEM_CODE: MEAT_TOTAL_LABEL,  # Meat, total.
    "00001058": "chickens",  # 'Meat of chickens, fresh or chilled',
    "00001069": "ducks",  # 'Meat of ducks, fresh or chilled',
    "00001035": "pigs",  # 'Meat of pig with the bone, fresh or chilled',
    "00001073": "geese",  # 'Meat of geese, fresh or chilled',
    "00000977": "sheep",  # 'Meat of sheep, fresh or chilled',
    "00001141": "rabbits",  # 'Meat of rabbits and hares, fresh or chilled',
    "00001080": "turkeys",  # 'Meat of turkeys, fresh or chilled',
    "00001017": "goats",  # 'Meat of goat, fresh or chilled',
    "00000867": "cattle",  # 'Meat of cattle with the bone, fresh or chilled',
    "00001151": "other rodents",  # 'Meat of other domestic rodents, fresh or chilled',
    "00001089": "pigeons",  # 'Meat of pigeons and other birds n.e.c., fresh, chilled or frozen',
    "00000947": "buffaloes",  # 'Meat of buffalo, fresh or chilled',
    "00001097": "horses",  # 'Horse meat, fresh or chilled',
    "00001127": "camels",  # 'Meat of camels, fresh or chilled',
    "00001108": "donkeys",  # 'Meat of asses, fresh or chilled',
    "00001158": "other camelids",  # 'Meat of other domestic camelids, fresh or chilled',
    "00001111": "mules",  # 'Meat of mules, fresh or chilled',
    "00001166": "other non mammals",  # 'Other meat n.e.c. (excluding mammals), fresh, chilled or frozen',
    "00001163": "game",  # 'Game meat, fresh, chilled or frozen',
    # "00001176": "snails",  # 'Snails, fresh, chilled, frozen, dried, salted or in brine, except sea snails',
    # Items that were in the list of "Meat, Total", but were not in the data:
    # "00001083",  # 'Other birds',
}

# Label for wild-caught fish.
WILD_FISH_LABEL = "wild-caught fish"

# Label for farmed fish.
FARMED_FISH_LABEL = "Farmed fish"

# List of item codes that should add up to the total stocks of animals.
STOCK_ITEM_CODES = {
    "00000866": "cattle",  # Cattle
    "00000946": "buffaloes",  # Buffalo
    # '00001746': 'cattle_and_buffaloes',  # Cattle and Buffaloes
    "00001057": "chickens",  # Chickens
    "00001068": "ducks",  # Ducks
    "00001079": "turkeys",  # Turkeys
    # '00002029': 'poultry',  # Poultry
    "00000976": "sheep",  # Sheep
    "00001016": "goats",  # Goats
    # '00001749': 'sheep_and_goats',  # Sheep and goats
    "00001034": "pigs",  # Swine / pigs
    "00001096": "horses",  # Horses
    "00001107": "donkeys",  # Asses
    "00001110": "mules",  # Mules and hinnies
    "00001140": "rabbits",  # Rabbits
    "00001181": "bees",  # Bees
    "00001126": "camels",  # Camels
    "00001072": "geese",  # Geese
    "00001150": "other rodents",  # Other rodents
    "00001157": "other camelids",  # Other camelids
}

# List of element codes for "Producing or slaughtered animals" (they have different items assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]

# List of element codes for "Producing or slaughtered animals" per capita.
SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT_CODES = ["5320pc", "5321pc"]

# Element code for the stocks of animals.
STOCK_ANIMALS_ELEMENT_CODES = ["005111", "005112", "005114"]

# Element code for the stock of animals per capita.
STOCK_ANIMALS_PER_CAPITA_ELEMENT_CODES = ["5111pc", "5112pc", "5114pc"]

# Name of index columns in the final table.
INDEX_COLUMNS = ["country", "year", "animal", "per_capita"]


def sanity_check_inputs(tb_killed, tb_stock, tb_qcl):
    assert set(tb_killed["unit"]) == {"animals"}, "Units may have changed."

    # Check that the sum of the different animals killed for food adds up to the total.
    # NOTE: This should be true by construction, as the "Meat, total" was created in the faostat_qcl garden step.
    # If this is not fulfilled, it may be because the list of items in that step differs from the one defined here.
    tb_killed_global_sum = (
        tb_killed[(tb_killed["item_code"] != MEAT_TOTAL_ITEM_CODE) & (tb_killed["country"] == "World")]
        .groupby("year", as_index=False)
        .agg({"value": "sum"})
    )
    tb_killed_global = (
        tb_qcl[
            (tb_qcl["country"] == "World")
            & (tb_qcl["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES))
            & (tb_qcl["item_code"] == MEAT_TOTAL_ITEM_CODE)
        ]
        .groupby("year", as_index=False)
        .agg({"value": "sum"})
    )
    compared = tb_killed_global_sum.merge(tb_killed_global, on="year", suffixes=("_sum", "_global"))
    error = "The sum of the different animals killed for food do not add up to the total."
    assert ((100 * (compared["value_sum"] - compared["value_global"]) / compared["value_global"]) < 0.001).all(), error

    # Sanity check for stock.
    assert set(tb_stock["unit"]) == {"animals"}, "Units may have changed."

    # TODO: It would be good to check that the sum of all values for Stocks (for all animals) agrees (within a certain percentage) with the Stocks for "Meat, Total". However, it doesn't. I just checked directly on https://www.fao.org/faostat/en/#data/QCL that Stocks for Meat, Total for World is 23449 in 2023 (in 1000 An). This means 23 million animals. This number is at least a factor of 1000 too small! I will try to figure out what this number means.


def run() -> None:
    #
    # Load inputs.
    #
    # Load faostat qcl dataset, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl")

    # Load number of wild-caught fish.
    ds_wild_fish = paths.load_dataset("number_of_wild_fish_killed_for_food")
    tb_wild_fish = ds_wild_fish.read("number_of_wild_fish_killed_for_food")

    # Load number of farmed fish.
    ds_farmed_fish = paths.load_dataset("number_of_farmed_fish")
    tb_farmed_fish = ds_farmed_fish.read("number_of_farmed_fish")

    #
    # Process data.
    #
    # Prepare wild-caught fish data.
    # TODO: Add dimension for estimate (low, midpoint or high).
    with pr.ignore_warnings():
        tb_fish = pr.concat(
            [
                tb_wild_fish[["country", "year", "n_wild_fish"]]
                .assign(**{"per_capita": False, "animal": WILD_FISH_LABEL})
                .rename(columns={"n_wild_fish": "n_animals_killed"}),
                tb_wild_fish[["country", "year", "n_wild_fish_per_capita"]]
                .assign(**{"per_capita": True, "animal": WILD_FISH_LABEL})
                .rename(columns={"n_wild_fish_per_capita": "n_animals_killed"}),
                tb_farmed_fish[["country", "year", "n_farmed_fish"]]
                .assign(**{"per_capita": False, "animal": FARMED_FISH_LABEL})
                .rename(columns={"n_farmed_fish": "n_animals_killed"}),
                tb_farmed_fish[["country", "year", "n_farmed_fish_per_capita"]]
                .assign(**{"per_capita": True, "animal": FARMED_FISH_LABEL})
                .rename(columns={"n_farmed_fish_per_capita": "n_animals_killed"}),
            ],
            ignore_index=True,
        )

    # Create a table for the number of killed animals of each kind.
    tb_killed = (
        tb_qcl[
            tb_qcl["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)
            & tb_qcl["item_code"].isin(MEAT_TOTAL_ITEM_CODES.keys())
        ]
        .assign(**{"per_capita": False})
        .reset_index(drop=True)
    )

    # Create a table for the number of killed animals of each kind per person.
    tb_killed_per_capita = (
        tb_qcl[
            tb_qcl["element_code"].isin(SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT_CODES)
            & tb_qcl["item_code"].isin(MEAT_TOTAL_ITEM_CODES.keys())
        ]
        .assign(**{"per_capita": True})
        .reset_index(drop=True)
    )

    # Create a table for the number of animals in stock.
    tb_stock = (
        tb_qcl[tb_qcl["element_code"].isin(STOCK_ANIMALS_ELEMENT_CODES) & tb_qcl["item_code"].isin(STOCK_ITEM_CODES)]
        .assign(**{"per_capita": False})
        .reset_index(drop=True)
    )

    # Create a table for the number of animals in stock per person.
    tb_stock_per_capita = (
        tb_qcl[
            tb_qcl["element_code"].isin(STOCK_ANIMALS_PER_CAPITA_ELEMENT_CODES)
            & tb_qcl["item_code"].isin(STOCK_ITEM_CODES)
        ]
        .assign(**{"per_capita": True})
        .reset_index(drop=True)
    )

    # Sanity checks.
    sanity_check_inputs(tb_killed=tb_killed, tb_stock=tb_stock, tb_qcl=tb_qcl)

    tb_killed["animal"] = map_series(
        tb_killed["item_code"],
        mapping=MEAT_TOTAL_ITEM_CODES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb_killed = tb_killed[INDEX_COLUMNS + ["value"]].rename(columns={"value": "n_animals_killed"}, errors="raise")

    tb_killed_per_capita["animal"] = map_series(
        tb_killed_per_capita["item_code"],
        mapping=MEAT_TOTAL_ITEM_CODES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb_killed_per_capita = tb_killed_per_capita[INDEX_COLUMNS + ["value"]].rename(
        columns={"value": "n_animals_killed"}, errors="raise"
    )

    tb_stock["animal"] = map_series(
        tb_stock["item_code"], mapping=STOCK_ITEM_CODES, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )
    tb_stock = tb_stock[INDEX_COLUMNS + ["value"]].rename(columns={"value": "n_animals_alive"}, errors="raise")

    tb_stock_per_capita["animal"] = map_series(
        tb_stock_per_capita["item_code"],
        mapping=STOCK_ITEM_CODES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb_stock_per_capita = tb_stock_per_capita[INDEX_COLUMNS + ["value"]].rename(
        columns={"value": "n_animals_alive"}, errors="raise"
    )

    # Combine tables.
    tb_killed_all = pr.concat([tb_killed, tb_killed_per_capita, tb_fish], ignore_index=True)
    tb_stock_all = pr.concat([tb_stock, tb_stock_per_capita], ignore_index=True)
    tb = pr.multi_merge([tb_killed_all, tb_stock_all], on=INDEX_COLUMNS, how="outer")

    # Format table conveniently.
    tb = tb.format(keys=INDEX_COLUMNS, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        yaml_params={
            "MEAT_TOTAL_LABEL": MEAT_TOTAL_LABEL,
            "WILD_FISH_LABEL": WILD_FISH_LABEL,
            "FARMED_FISH_LABEL": FARMED_FISH_LABEL,
        },
    )
    ds_garden.save()
