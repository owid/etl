"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import VariablePresentationMeta
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Item code for "Meat, total" (used only for sanity checks).
MEAT_TOTAL_ITEM_CODE = "00001765"
MEAT_TOTAL_LABEL = "all land animals"
# Item code for total poultry meat.
MEAT_POULTRY_ITEM_CODE = "00001808"

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
    "00001166": "other non-mammals",  # 'Other meat n.e.c. (excluding mammals), fresh, chilled or frozen',
    "00001163": "game animals",  # 'Game meat, fresh, chilled or frozen',
    # "00001176": "snails",  # 'Snails, fresh, chilled, frozen, dried, salted or in brine, except sea snails',
    # Items that were in the list of "Meat, Total", but were not in the data:
    # "00001083",  # 'Other birds',
    MEAT_POULTRY_ITEM_CODE: "poultry",  # Meat, Poultry
}

# Label for wild-caught fish.
WILD_FISH_LABEL = "wild-caught fish"

# Label for farmed fish.
FARMED_FISH_LABEL = "farmed fish"

# Label for farmed crustaceans.
FARMED_CRUSTACEANS_LABEL = "farmed crustaceans"

# Labels for the three estimate dimensions (relevant for fish and crustacean data).
ESTIMATE_MIDPOINT_LABEL = "midpoint"
ESTIMATE_LOW_LABEL = "lower limit"
ESTIMATE_HIGH_LABEL = "upper limit"

# Label for the estimate dimension for land animals data (where this dimension is irrelevant).
EMPTY_DIMENSION_LABEL = ""

# List of item codes that should add up to the total stocks of animals.
STOCK_ITEM_CODES = {
    "00000866": "cattle",  # Cattle
    "00000946": "buffaloes",  # Buffalo
    # '00001746': 'cattle_and_buffaloes',  # Cattle and Buffaloes
    "00001057": "chickens",  # Chickens
    "00001068": "ducks",  # Ducks
    "00001079": "turkeys",  # Turkeys
    "00000976": "sheep",  # Sheep
    "00001016": "goats",  # Goats
    # '00001749': 'sheep_and_goats',  # Sheep and goats
    "00001034": "pigs",  # Swine / pigs
    "00001096": "horses",  # Horses
    "00001107": "donkeys",  # Asses
    "00001110": "mules",  # Mules and hinnies
    "00001140": "rabbits",  # Rabbits
    # "00001181": "bees",  # Bees
    "00001126": "camels",  # Camels
    "00001072": "geese",  # Geese
    "00001150": "other rodents",  # Other rodents
    "00001157": "other camelids",  # Other camelids
    "00002029": "poultry",  # Poultry Birds
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
INDEX_COLUMNS = ["country", "year", "metric", "animal", "per_capita", "estimate"]


def sanity_check_animals_killed(tb_killed, tb_qcl):
    assert set(tb_killed["unit"]) == {"animals"}, "Units may have changed."

    # Check that the sum of the different animals killed for food adds up to the total.
    # NOTE: This should be true by construction, as the "Meat, total" was created in the faostat_qcl garden step.
    # If this is not fulfilled, it may be because the list of items in that step differs from the one defined here.
    tb_killed_global_sum = (
        tb_killed[
            (~tb_killed["item_code"].isin([MEAT_TOTAL_ITEM_CODE, MEAT_POULTRY_ITEM_CODE]))
            & (tb_killed["country"] == "World")
        ]
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


def sanity_check_animals_alive(tb_stock):
    # Sanity check for stock.
    assert set(tb_stock["unit"]) == {"animals"}, "Units may have changed."

    # TODO: It would be good to check that the sum of all values for Stocks (for all animals) agrees (within a certain percentage) with the Stocks for "Meat, Total". However, it doesn't. I just checked directly on https://www.fao.org/faostat/en/#data/QCL that Stocks for Meat, Total for World is 23449 in 2023 (in 1000 An). This means 23 million animals. This number is at least a factor of 1000 too small! I will try to figure out what this number means.


def prepare_fish_and_crustaceans_data(tb_wild_fish, tb_farmed_fish, tb_farmed_crustaceans):
    with pr.ignore_warnings():
        tables = []
        for estimate in [ESTIMATE_MIDPOINT_LABEL, ESTIMATE_LOW_LABEL, ESTIMATE_HIGH_LABEL]:
            suffix_estimate = {
                ESTIMATE_MIDPOINT_LABEL: "",
                ESTIMATE_LOW_LABEL: "_low",
                ESTIMATE_HIGH_LABEL: "_high",
            }[estimate]
            for per_capita in [True, False]:
                suffix_pc = "_per_capita" if per_capita else ""
                tables.extend(
                    [
                        tb_wild_fish[["country", "year", f"n_wild_fish{suffix_estimate}{suffix_pc}"]].assign(
                            **{
                                "metric": "animals_killed",
                                "per_capita": per_capita,
                                "animal": WILD_FISH_LABEL,
                                "estimate": estimate,
                            }
                        ),
                        tb_farmed_fish[["country", "year", f"n_farmed_fish{suffix_estimate}{suffix_pc}"]].assign(
                            **{
                                "metric": "animals_killed",
                                "per_capita": per_capita,
                                "animal": FARMED_FISH_LABEL,
                                "estimate": estimate,
                            }
                        ),
                        tb_farmed_crustaceans[
                            ["country", "year", f"n_farmed_crustaceans{suffix_estimate}{suffix_pc}"]
                        ].assign(
                            **{
                                "metric": "animals_killed",
                                "per_capita": per_capita,
                                "animal": FARMED_CRUSTACEANS_LABEL,
                                "estimate": estimate,
                            }
                        ),
                    ]
                )
        tb_fish = pr.multi_merge(tables, on=INDEX_COLUMNS, how="outer")

    return tb_fish


def prepare_land_animals_data(tb_qcl, killed_or_alive):
    if killed_or_alive == "killed":
        element_codes = SLAUGHTERED_ANIMALS_ELEMENT_CODES
        element_codes_per_capita = SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT_CODES
        item_codes = MEAT_TOTAL_ITEM_CODES
    elif killed_or_alive == "alive":
        element_codes = STOCK_ANIMALS_ELEMENT_CODES
        element_codes_per_capita = STOCK_ANIMALS_PER_CAPITA_ELEMENT_CODES
        item_codes = STOCK_ITEM_CODES

    # Create a table for the number of killed/alive animals of each kind.
    tb = (
        tb_qcl[tb_qcl["element_code"].isin(element_codes) & tb_qcl["item_code"].isin(item_codes.keys())]
        .assign(**{"metric": f"animals_{killed_or_alive}", "per_capita": False, "estimate": EMPTY_DIMENSION_LABEL})
        .reset_index(drop=True)
    )

    # Create a table for the number of animals of each kind per person.
    tb_per_capita = (
        tb_qcl[tb_qcl["element_code"].isin(element_codes_per_capita) & tb_qcl["item_code"].isin(item_codes.keys())]
        .assign(**{"metric": f"animals_{killed_or_alive}", "per_capita": True, "estimate": EMPTY_DIMENSION_LABEL})
        .reset_index(drop=True)
    )

    # Sanity checks.
    if killed_or_alive == "killed":
        sanity_check_animals_killed(tb_killed=tb, tb_qcl=tb_qcl)
    else:
        sanity_check_animals_alive(tb_stock=tb)

    # Rename animals appropriately.
    tb["animal"] = map_series(
        tb["item_code"],
        mapping=item_codes,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb_per_capita["animal"] = map_series(
        tb_per_capita["item_code"],
        mapping=item_codes,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )

    # Select and rename columns.
    tb = tb[INDEX_COLUMNS + ["value"]].rename(columns={"value": f"n_animals_{killed_or_alive}"}, errors="raise")
    tb_per_capita = tb_per_capita[INDEX_COLUMNS + ["value"]].rename(
        columns={"value": f"n_animals_{killed_or_alive}_per_capita"}, errors="raise"
    )

    return tb, tb_per_capita


def improve_metadata(tb, tb_qcl_flat):
    # Firstly, get the original description from producer (from the original wide-format table).
    descriptions_from_producer_killed = {
        animal: tb_qcl_flat[column].metadata.description_from_producer
        for column in tb_qcl_flat.columns
        for item_code, animal in MEAT_TOTAL_ITEM_CODES.items()
        for element_code in SLAUGHTERED_ANIMALS_ELEMENT_CODES
        if item_code in column and element_code in column
    }
    descriptions_from_producer_alive = {
        animal: tb_qcl_flat[column].metadata.description_from_producer
        for column in tb_qcl_flat.columns
        for item_code, animal in STOCK_ITEM_CODES.items()
        for element_code in STOCK_ANIMALS_ELEMENT_CODES
        if item_code in column and element_code in column
    }

    for column in tb.columns:
        tb[column].metadata.unit = """animals<% if per_capita == True %> per person<% endif %>"""
        tb[column].metadata.short_unit = ""
        if "_alive" in column:
            title = """Live << animal >><% if per_capita == True %> per person<% endif %>"""
            tb[
                column
            ].metadata.description_short = """Livestock counts represent the total number of live animals at a given time in any year. This is not to be confused with the total number of livestock animals slaughtered in any given year."""
            description_from_producer = ""
            for animal, description in descriptions_from_producer_alive.items():
                if animal == list(descriptions_from_producer_alive)[0]:
                    description_from_producer += f"""<% if animal == "{animal}" %>{description}"""
                else:
                    description_from_producer += f"""<% elif animal == "{animal}" %>{description}"""
            description_from_producer += "<% endif %>"
        else:
            title = f"""<% if animal == "{MEAT_TOTAL_LABEL}" %>Land animals slaughtered for meat<% elif animal == "{WILD_FISH_LABEL}" %>Fishes caught from the wild<% elif animal == "{FARMED_FISH_LABEL}" %>Farmed fishes killed for food<% elif animal == "{FARMED_CRUSTACEANS_LABEL}" %>Farmed crustaceans killed for food<% else %><< animal.capitalize() >> slaughtered for meat<% endif %><% if per_capita == True %> per person<% endif %><% if estimate == "{ESTIMATE_HIGH_LABEL}" %> (upper limit)<% elif estimate == "{ESTIMATE_LOW_LABEL}" %> (lower limit)<% endif %>"""
            tb[column].metadata.description_short = """Based on the country of production, not consumption."""
            tb[column].metadata.description_key = [
                """Additional deaths that happen during meat and dairy production prior to the slaughter, for example due to disease or accidents, are not included.""",
                """<% if animal == "chickens" %>Male baby chickens slaughtered in the egg industry are not included.<% endif %>""",
            ]
            description_from_producer = ""
            for animal, description in descriptions_from_producer_killed.items():
                if animal == list(descriptions_from_producer_killed)[0]:
                    description_from_producer += f"""<% if animal == "{animal}" %>{description}"""
                else:
                    description_from_producer += f"""<% elif animal == "{animal}" %>{description}"""
            description_from_producer += "<% endif %>"
        tb[column].metadata.title = title
        tb[column].metadata.display = {"name": """<< animal.capitalize() >>"""}
        tb[column].metadata.presentation = VariablePresentationMeta(title_public=title)
        tb[column].metadata.description_from_producer = description_from_producer


def run() -> None:
    #
    # Load inputs.
    #
    # Load faostat qcl dataset, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl")

    # Load the wide-format table, to get the already prepared descriptions from producer.
    # NOTE: In hindsight, I think it would have been much easier to work with flat tables from the start, to keep all the original metadata.
    tb_qcl_flat = ds_qcl.read("faostat_qcl_flat")

    # Load number of wild-caught fish.
    ds_wild_fish = paths.load_dataset("number_of_wild_fish_killed_for_food")
    tb_wild_fish = ds_wild_fish.read("number_of_wild_fish_killed_for_food")

    # Load number of farmed fish.
    ds_farmed_fish = paths.load_dataset("number_of_farmed_fish")
    tb_farmed_fish = ds_farmed_fish.read("number_of_farmed_fish")

    # Load number of farmed crustaceans.
    ds_farmed_crustaceans = paths.load_dataset("number_of_farmed_crustaceans")
    tb_farmed_crustaceans = ds_farmed_crustaceans.read("number_of_farmed_crustaceans")

    #
    # Process data.
    #
    # Prepare fish and crustaceans data.
    tb_fish = prepare_fish_and_crustaceans_data(
        tb_wild_fish=tb_wild_fish, tb_farmed_fish=tb_farmed_fish, tb_farmed_crustaceans=tb_farmed_crustaceans
    )

    # Prepare land animals slaughtered data.
    tb_killed, tb_killed_per_capita = prepare_land_animals_data(tb_qcl=tb_qcl, killed_or_alive="killed")

    # Prepare land animals alive data.
    tb_stock, tb_stock_per_capita = prepare_land_animals_data(tb_qcl=tb_qcl, killed_or_alive="alive")

    # Combine tables.
    tb = pr.multi_merge(
        [tb_fish, tb_killed, tb_killed_per_capita, tb_stock, tb_stock_per_capita], on=INDEX_COLUMNS, how="outer"
    )

    # Format table conveniently.
    tb = tb.format(keys=INDEX_COLUMNS, short_name=paths.short_name)

    # Improve metadata programmatically (using some metadata from the old wide-format table).
    improve_metadata(tb=tb, tb_qcl_flat=tb_qcl_flat)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
