"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Item codes that should add up to "Meat, Total", as well as meat total itself.
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
}

# List of element codes for "Producing or slaughtered animals" (they have different items assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]

# List of main land animals used to produce meat (the rest will be grouped as other).
MAIN_ANIMALS_KILLED = [
    "chickens",
    "ducks",
    "pigs",
    "geese",
    "sheep",
    "rabbits",
    "turkeys",
    "goats",
    "cattle",
]

# Label for all other animals.
OTHER_ANIMALS_KILLED_LABEL = "other animals"


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
    # Create a table for the number of killed/alive animals of each kind.
    tb = tb_qcl[
        tb_qcl["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)
        & tb_qcl["item_code"].isin(MEAT_TOTAL_ITEM_CODES.keys())
    ].reset_index(drop=True)

    # Rename animals appropriately.
    tb["animal"] = map_series(
        tb["item_code"],
        mapping=MEAT_TOTAL_ITEM_CODES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )

    # Select and rename columns.
    tb = tb[["country", "year", "animal", "value"]].rename(columns={"value": "n_animals_killed"}, errors="raise")

    # Group less frequently slaughtered animals into an "other" category.
    tb_other = (
        tb[~tb["animal"].isin(MAIN_ANIMALS_KILLED)]
        .groupby(["country", "year"], as_index=False)
        .agg({"n_animals_killed": "sum"})
        .assign(**{"animal": OTHER_ANIMALS_KILLED_LABEL})
    )
    tb = pr.concat([tb[tb["animal"].isin(MAIN_ANIMALS_KILLED)], tb_other], ignore_index=True)

    # Improve table format.
    tb = tb.format(["country", "year", "animal"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
