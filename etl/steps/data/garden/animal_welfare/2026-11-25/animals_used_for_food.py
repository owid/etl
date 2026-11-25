"""Combine the numbers of animals killed for food, and of live animals, into one table.

The animal welfare multidims build their views from this table, so every indicator shares the same dimensions: metric
(animals killed where they are killed, animals killed for each country's consumption, or live animals), animal, per
capita, and estimate (only fish and crustaceans are estimated as a range).

Each indicator comes from a single source, so that it keeps its own origins:
- land_animals_killed and live_animals: FAOSTAT (QCL).
- land_animals_killed_for_consumption: our estimate in animals_killed_for_domestic_consumption.
- chickens_killed_for_eggs: our estimate in chickens_killed_for_eggs.
- wild_caught_fish, farmed_fish and farmed_crustaceans: Fishcount.

Values per person are in indicators of their own (ending in "_per_capita"), since only they also come from population
data.
"""

from owid.catalog import Table
from owid.catalog import processing as pr
from owid.catalog.utils import underscore
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Dimensions of all indicators (besides country and year).
DIMENSIONS = ["metric", "animal", "per_capita", "estimate"]

# Value of the estimate dimension for animals that are not estimated as a range.
ESTIMATE_NOT_APPLICABLE = "not_applicable"

# FAOSTAT QCL items of animals slaughtered for meat. "Meat, Total" (all land animals) is the sum of all other items.
# NOTE: Poultry is not included, since it is mostly chickens.
SLAUGHTERED_ITEM_CODES = {
    "00001765": "all_land_animals",  # 'Meat, Total'.
    "00001058": "chickens",  # 'Meat of chickens, fresh or chilled'.
    "00001069": "ducks",  # 'Meat of ducks, fresh or chilled'.
    "00001035": "pigs",  # 'Meat of pig with the bone, fresh or chilled'.
    "00001073": "geese",  # 'Meat of geese, fresh or chilled'.
    "00000977": "sheep",  # 'Meat of sheep, fresh or chilled'.
    "00001141": "rabbits",  # 'Meat of rabbits and hares, fresh or chilled'.
    "00001080": "turkeys",  # 'Meat of turkeys, fresh or chilled'.
    "00001017": "goats",  # 'Meat of goat, fresh or chilled'.
    "00000867": "cattle",  # 'Meat of cattle with the bone, fresh or chilled'.
    "00001151": "other_rodents",  # 'Meat of other domestic rodents, fresh or chilled'.
    "00001089": "pigeons",  # 'Meat of pigeons and other birds n.e.c., fresh, chilled or frozen'.
    "00000947": "buffaloes",  # 'Meat of buffalo, fresh or chilled'.
    "00001097": "horses",  # 'Horse meat, fresh or chilled'.
    "00001127": "camels",  # 'Meat of camels, fresh or chilled'.
    "00001108": "donkeys",  # 'Meat of asses, fresh or chilled'.
    "00001158": "other_camelids",  # 'Meat of other domestic camelids, fresh or chilled'.
    "00001111": "mules",  # 'Meat of mules, fresh or chilled'.
    "00001163": "game_animals",  # 'Game meat, fresh, chilled or frozen'.
}
# FAOSTAT QCL elements of animals slaughtered ("Producing or slaughtered animals", which have different items), in
# total and per person.
SLAUGHTERED_ELEMENT_CODES = {"005320": "total", "005321": "total", "5320pc": "per_capita", "5321pc": "per_capita"}

# FAOSTAT QCL items of live animals.
LIVE_ITEM_CODES = {
    "00000866": "cattle",  # 'Cattle'.
    "00000946": "buffaloes",  # 'Buffalo'.
    "00001057": "chickens",  # 'Chickens'.
    "00001068": "ducks",  # 'Ducks'.
    "00001079": "turkeys",  # 'Turkeys'.
    "00000976": "sheep",  # 'Sheep'.
    "00001016": "goats",  # 'Goats'.
    "00001034": "pigs",  # 'Swine / pigs'.
    "00001096": "horses",  # 'Horses'.
    "00001107": "donkeys",  # 'Asses'.
    "00001110": "mules",  # 'Mules and hinnies'.
    "00001140": "rabbits",  # 'Rabbits'.
    "00001126": "camels",  # 'Camels'.
    "00001072": "geese",  # 'Geese'.
    "00001150": "other_rodents",  # 'Other rodents'.
    "00001157": "other_camelids",  # 'Other camelids'.
}
# FAOSTAT QCL elements of live animals ("Stocks", in different units), in total and per person.
LIVE_ELEMENT_CODES = {
    "005111": "total",
    "005112": "total",
    "005114": "total",
    "5111pc": "per_capita",
    "5112pc": "per_capita",
    "5114pc": "per_capita",
}
# FAOSTAT QCL items and elements of live animals that produce eggs or milk.
LIVE_PRODUCING_ANIMALS = {
    "laying_hens": ("00001062", {"005313": "total", "5313pc": "per_capita"}),  # 'Eggs from hens', 'Laying'.
    "milk_cattle": ("00000882", {"005318": "total", "5318pc": "per_capita"}),  # 'Raw milk of cattle', 'Milk Animals'.
}

# Land animals shown on their own when stacked. All other land animals are added up as "other_land_animals".
MAIN_LAND_ANIMALS = ["chickens", "ducks", "pigs", "geese", "sheep", "rabbits", "turkeys", "goats", "cattle"]
OTHER_LAND_ANIMALS = "other_land_animals"
ALL_LAND_ANIMALS = "all_land_animals"

# Fishcount estimates: for each animal, its dataset and the prefix of its columns.
FISH_AND_CRUSTACEANS = {
    "wild_caught_fish": ("number_of_wild_fish_killed_for_food", "n_wild_fish"),
    "farmed_fish": ("number_of_farmed_fish", "n_farmed_fish"),
    "farmed_crustaceans": ("number_of_farmed_crustaceans", "n_farmed_crustaceans"),
}
# Suffix of the columns of each estimate in the Fishcount datasets.
ESTIMATE_SUFFIXES = {"midpoint": "", "lower_limit": "_low", "upper_limit": "_high"}


def split_per_capita(tb: Table, column: str) -> Table:
    # Keep values per person in an indicator of their own, since only they also come from population data.
    tb_total = tb[tb["per_capita"] == "total"]
    tb_per_capita = tb[tb["per_capita"] == "per_capita"].rename(
        columns={column: f"{column}_per_capita"}, errors="raise"
    )

    return pr.concat([tb_total, tb_per_capita], ignore_index=True)


def prepare_qcl(tb_qcl: Table, item_codes: dict[str, str], element_codes: dict[str, str], metric: str, column: str):
    tb = tb_qcl[tb_qcl["item_code"].isin(item_codes) & tb_qcl["element_code"].isin(element_codes)].reset_index(
        drop=True
    )
    tb["animal"] = map_series(tb["item_code"].astype(str), item_codes, warn_on_missing_mappings=True)
    tb["per_capita"] = map_series(tb["element_code"].astype(str), element_codes, warn_on_missing_mappings=True)
    tb = tb.assign(**{"metric": metric, "estimate": ESTIMATE_NOT_APPLICABLE})
    tb = tb[["country", "year"] + DIMENSIONS + ["value"]].rename(columns={"value": column}, errors="raise")
    tb["country"] = tb["country"].astype(str)

    return split_per_capita(tb, column=column)


def add_other_land_animals(tb: Table, column: str) -> Table:
    # Add up all land animals other than the main ones (and other than their total).
    columns = [column, f"{column}_per_capita"]
    others = tb[~tb["animal"].isin(MAIN_LAND_ANIMALS + [ALL_LAND_ANIMALS])]
    tb_other = (
        others.groupby(["country", "year", "metric", "per_capita", "estimate"], observed=True, as_index=False)
        .agg({name: lambda values: values.sum(min_count=1) for name in columns})
        .assign(**{"animal": OTHER_LAND_ANIMALS})
    )

    return pr.concat([tb, tb_other], ignore_index=True)


def prepare_consumption(tb_consumption: Table) -> Table:
    column = "land_animals_killed_for_consumption"
    tables = []
    for per_capita, source_column, new_column in [
        ("total", "killed_for_domestic_supply", column),
        ("per_capita", "killed_for_domestic_supply_per_capita", f"{column}_per_capita"),
    ]:
        tb = tb_consumption[["country", "year", "animal", source_column]].rename(
            columns={source_column: new_column}, errors="raise"
        )
        tables.append(tb.assign(**{"per_capita": per_capita}).dropna(subset=[new_column]))
    tb = pr.concat(tables, ignore_index=True)
    tb["animal"] = tb["animal"].astype(str).map(underscore)
    # Poultry is not included, since it is mostly chickens.
    tb = tb[tb["animal"] != "poultry"].reset_index(drop=True)
    tb = tb.assign(**{"metric": "animals_killed_for_consumption", "estimate": ESTIMATE_NOT_APPLICABLE})

    return add_other_land_animals(tb[["country", "year"] + DIMENSIONS + [column, f"{column}_per_capita"]], column)


def prepare_chickens_killed_for_eggs(tb_eggs: Table) -> Table:
    column = "chickens_killed_for_eggs"
    tables = []
    for per_capita, source_column, new_column in [
        ("total", "chickens_killed", column),
        ("per_capita", "chickens_killed_per_capita", f"{column}_per_capita"),
    ]:
        tb = tb_eggs[["country", "year", source_column]].rename(columns={source_column: new_column}, errors="raise")
        tables.append(tb.assign(**{"per_capita": per_capita}).dropna(subset=[new_column]))
    tb = pr.concat(tables, ignore_index=True)
    tb = tb.assign(**{"metric": "animals_killed", "animal": column, "estimate": ESTIMATE_NOT_APPLICABLE})

    return tb[["country", "year"] + DIMENSIONS + [column, f"{column}_per_capita"]].reset_index(drop=True)


def prepare_fish_and_crustaceans(tb: Table, animal: str, prefix: str) -> Table:
    tables = []
    for estimate, suffix in ESTIMATE_SUFFIXES.items():
        for per_capita, suffix_per_capita in [("total", ""), ("per_capita", "_per_capita")]:
            source_column = f"{prefix}{suffix}{suffix_per_capita}"
            new_column = f"{animal}{suffix_per_capita}"
            tb_estimate = tb[["country", "year", source_column]].rename(
                columns={source_column: new_column}, errors="raise"
            )
            tb_estimate = tb_estimate.assign(**{"per_capita": per_capita, "estimate": estimate})
            tables.append(tb_estimate.dropna(subset=[new_column]))
    tb = pr.concat(tables, ignore_index=True)
    tb = tb.assign(**{"metric": "animals_killed", "animal": animal})

    return tb[["country", "year"] + DIMENSIONS + [animal, f"{animal}_per_capita"]].reset_index(drop=True)


def add_descriptions_from_producer(tb: Table, tb_qcl_flat: Table) -> None:
    # Take FAOSTAT's description of each item (from the wide table, where each column is an item and element).
    for column, item_codes, element_codes in [
        ("land_animals_killed", SLAUGHTERED_ITEM_CODES, SLAUGHTERED_ELEMENT_CODES),
        ("live_animals", LIVE_ITEM_CODES, LIVE_ELEMENT_CODES),
    ]:
        descriptions = {}
        for flat_column in tb_qcl_flat.columns:
            for item_code, animal in item_codes.items():
                if any(
                    f"__{item_code}__" in flat_column and f"__{element}__" in flat_column for element in element_codes
                ):
                    descriptions[animal] = tb_qcl_flat[flat_column].metadata.description_from_producer
        if column == "live_animals":
            for animal, (item_code, elements) in LIVE_PRODUCING_ANIMALS.items():
                for flat_column in tb_qcl_flat.columns:
                    if any(
                        f"__{item_code}__" in flat_column and f"__{element}__" in flat_column for element in elements
                    ):
                        descriptions[animal] = tb_qcl_flat[flat_column].metadata.description_from_producer
        template = "".join(
            f'<% {"if" if i == 0 else "elif"} animal == "{animal}" %>{description}'
            for i, (animal, description) in enumerate(descriptions.items())
            if description
        )
        for variable in [column, f"{column}_per_capita"]:
            tb[variable].metadata.description_from_producer = f"{template}<% endif %>" if template else None


def sanity_check_outputs(tb: Table) -> None:
    error = "Negative numbers of animals."
    assert (tb.drop(columns=["country", "year"] + DIMENSIONS).fillna(0) >= 0).all().all(), error

    # The main land animals and the other land animals must add up to all land animals, so that they can be stacked.
    for metric, column in [
        ("animals_killed", "land_animals_killed"),
        ("animals_killed_for_consumption", "land_animals_killed_for_consumption"),
    ]:
        world = tb[(tb["country"] == "World") & (tb["metric"] == metric) & (tb["per_capita"] == "total")]
        stacked = (
            world[world["animal"].isin(MAIN_LAND_ANIMALS + [OTHER_LAND_ANIMALS])]
            .groupby("year", as_index=False)
            .agg({column: "sum"})
        )
        total = world[world["animal"] == ALL_LAND_ANIMALS][["year", column]]
        compared = pr.merge(stacked, total, on="year", suffixes=("_stacked", "_total")).dropna()
        assert len(compared) > 0, f"No world data to compare for {column}."
        difference = (compared[f"{column}_stacked"] - compared[f"{column}_total"]).abs()
        error = f"The main and other land animals do not add up to all land animals ({column})."
        assert (difference <= 1e-5 * compared[f"{column}_total"]).all(), error

    # The lower and upper estimates of fish and crustaceans must enclose the midpoint.
    for animal in FISH_AND_CRUSTACEANS:
        estimates = (
            tb[tb["animal"] == animal]
            .pivot(index=["country", "year", "per_capita"], columns="estimate", values=animal)
            .dropna()
        )
        assert len(estimates) > 0, f"No estimates for {animal}."
        error = f"Midpoint of {animal} outside of its range."
        assert (estimates["lower_limit"] <= estimates["midpoint"] * (1 + 1e-6)).all(), error
        assert (estimates["midpoint"] <= estimates["upper_limit"] * (1 + 1e-6)).all(), error


def run() -> None:
    #
    # Load inputs.
    #
    # NOTE: FAOSTAT tables are large; keep their categoricals, or the staging server runs out of memory.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl", safe_types=False)
    # The wide table has FAOSTAT's description of each item.
    tb_qcl_flat = ds_qcl.read("faostat_qcl_flat", load_data=False)
    tb_consumption = paths.load_dataset("animals_killed_for_domestic_consumption").read(
        "animals_killed_for_domestic_consumption"
    )
    tb_eggs = paths.load_dataset("chickens_killed_for_eggs").read("chickens_killed_for_eggs")
    tables_fish = {
        animal: paths.load_dataset(dataset).read(dataset) for animal, (dataset, _) in FISH_AND_CRUSTACEANS.items()
    }

    #
    # Process data.
    #
    # Land animals slaughtered in each country, including the other land animals added up.
    tb_killed = prepare_qcl(
        tb_qcl,
        item_codes=SLAUGHTERED_ITEM_CODES,
        element_codes=SLAUGHTERED_ELEMENT_CODES,
        metric="animals_killed",
        column="land_animals_killed",
    )
    tb_killed = add_other_land_animals(tb_killed, column="land_animals_killed")

    # Live animals, including laying hens and cattle used for milk.
    tables_live = [
        prepare_qcl(
            tb_qcl,
            item_codes=LIVE_ITEM_CODES,
            element_codes=LIVE_ELEMENT_CODES,
            metric="animals_alive",
            column="live_animals",
        )
    ]
    for animal, (item_code, element_codes) in LIVE_PRODUCING_ANIMALS.items():
        tables_live.append(
            prepare_qcl(
                tb_qcl,
                item_codes={item_code: animal},
                element_codes=element_codes,
                metric="animals_alive",
                column="live_animals",
            )
        )

    # Combine all animals.
    tb = pr.concat(
        [
            tb_killed,
            *tables_live,
            prepare_consumption(tb_consumption),
            prepare_chickens_killed_for_eggs(tb_eggs),
            *[
                prepare_fish_and_crustaceans(tables_fish[animal], animal=animal, prefix=prefix)
                for animal, (_, prefix) in FISH_AND_CRUSTACEANS.items()
            ],
        ],
        ignore_index=True,
    )
    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(keys=["country", "year"] + DIMENSIONS, short_name=paths.short_name)
    add_descriptions_from_producer(tb, tb_qcl_flat=tb_qcl_flat)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
