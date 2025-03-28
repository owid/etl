"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Item codes that should add up to "Meat, Total".
# NOTE: These should coincide with those defined in the garden faostat_qcl step.
MEAT_TOTAL_ITEM_CODES = {
    "00001058": "chickens",  # 'Meat of chickens, fresh or chilled',
    "00001069": "ducks",  # 'Meat of ducks, fresh or chilled',
    "00001035": "pigs",  # 'Meat of pig with the bone, fresh or chilled',
    "00001073": "geese",  # 'Meat of geese, fresh or chilled',
    "00000977": "sheep",  # 'Meat of sheep, fresh or chilled',
    "00001141": "rabbits",  # 'Meat of rabbits and hares, fresh or chilled',
    "00001080": "turkeys",  # 'Meat of turkeys, fresh or chilled',
    "00001017": "goats",  # 'Meat of goat, fresh or chilled',
    "00000867": "cattle",  # 'Meat of cattle with the bone, fresh or chilled',
    "00001151": "other_rodents",  # 'Meat of other domestic rodents, fresh or chilled',
    "00001089": "pigeons",  # 'Meat of pigeons and other birds n.e.c., fresh, chilled or frozen',
    "00000947": "buffaloes",  # 'Meat of buffalo, fresh or chilled',
    "00001097": "horses",  # 'Horse meat, fresh or chilled',
    "00001127": "camels",  # 'Meat of camels, fresh or chilled',
    "00001108": "donkeys",  # 'Meat of asses, fresh or chilled',
    "00001158": "other_camelids",  # 'Meat of other domestic camelids, fresh or chilled',
    "00001111": "mule",  # 'Meat of mules, fresh or chilled',
    "00001166": "other_non_mammals",  # 'Other meat n.e.c. (excluding mammals), fresh, chilled or frozen',
    "00001163": "game",  # 'Game meat, fresh, chilled or frozen',
    # "00001176": "snails",  # 'Snails, fresh, chilled, frozen, dried, salted or in brine, except sea snails',
    # Items that were in the list of "Meat, Total", but were not in the data:
    # "00001083",  # 'Other birds',
}

# List of item codes that should add up to the total stocks of animals.
STOCK_ITEM_CODES = {
    "00000866": "cattle",  # Cattle
    "00000946": "buffalo",  # Buffalo
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
    "00001150": "other_rodents",  # Other rodents
    "00001157": "other_camelids",  # Other camelids
}

# List of element codes for "Producing or slaughtered animals" (they have different items assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]

# Element code for the stocks of animals.
STOCK_ANIMALS_ELEMENT_CODES = ["005111", "005112", "005114"]

# Item code for "Meat, total" (used only for sanity checks).
MEAT_TOTAL_ITEM_CODE = "00001765"


def sanity_check_inputs(tb_killed, tb_stock, tb_qcl):
    assert set(tb_killed["unit"]) == {"animals"}, "Units may have changed."

    # Check that the sum of the different animals killed for food adds up to the total.
    # NOTE: This should be true by construction, as the "Meat, total" was created in the faostat_qcl garden step.
    # If this is not fulfilled, it may be because the list of items in that step differs from the one defined here.
    tb_killed_global_sum = (
        tb_killed[(tb_killed["country"] == "World")].groupby("year", as_index=False).agg({"value": "sum"})
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
    assert set(tb_stock["unit"]) == {"Number", "animals"}, "Units may have changed."

    # TODO: It would be good to check that the sum of all values for Stocks (for all animals) agrees (within a certain percentage) with the Stocks for "Meat, Total". However, it doesn't. I just checked directly on https://www.fao.org/faostat/en/#data/QCL that Stocks for Meat, Total for World is 23449 in 2023 (in 1000 An). This means 23 million animals. This number is at least a factor of 1000 too small! I will try to figure out what this number means.


def run() -> None:
    #
    # Load inputs.
    #
    # Load faostat qcl dataset, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl")

    #
    # Process data.
    #
    # Create a table for the number of killed animals of each kind.
    tb_killed = tb_qcl[
        tb_qcl["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)
        & tb_qcl["item_code"].isin(MEAT_TOTAL_ITEM_CODES.keys())
    ].reset_index(drop=True)

    # Create a table for the number of animals in stock.
    tb_stock = tb_qcl[
        tb_qcl["element_code"].isin(STOCK_ANIMALS_ELEMENT_CODES) & tb_qcl["item_code"].isin(STOCK_ITEM_CODES)
    ].reset_index(drop=True)

    # Sanity checks.
    sanity_check_inputs(tb_killed=tb_killed, tb_stock=tb_stock, tb_qcl=tb_qcl)

    # Make one column per item for the table of slaughtered animals.
    tb_killed = (
        tb_killed[["country", "year", "item_code", "value"]]
        .pivot(index=["country", "year"], columns="item_code", values="value")
        .reset_index()
        .rename(
            columns={column: f"{animal}_killed" for column, animal in MEAT_TOTAL_ITEM_CODES.items()}, errors="raise"
        )
    )

    # Make one column per item for the table of live animals.
    tb_stock = (
        tb_stock[["country", "year", "item_code", "value"]]
        .pivot(index=["country", "year"], columns="item_code", values="value")
        .reset_index()
        .rename(columns={column: f"{animal}_alive" for column, animal in STOCK_ITEM_CODES.items()}, errors="raise")
    )

    # Combine both tables.
    tb = tb_killed.merge(tb_stock, on=["country", "year"], how="outer")

    # Format table conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
