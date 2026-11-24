"""Estimate the number of land animals killed to supply each country's own consumption of meat, for each animal.

Our other indicators (animal_welfare/*/animals_used_for_food) count animals where they are killed. That attributes
exported meat to the exporting country, so big exporters look worse than their diets warrant, and big importers look
better. Here we instead attribute animals to the country that consumes the meat, for each animal:

    animals killed for consumption = animals killed in the country * (meat supplied / meat produced)

All quantities come from FAOSTAT:
- The animals killed come from QCL. Meat of an animal that the country does not produce itself is converted at the
  world's number of animals killed per tonne produced.
- The meat supplied and produced come from the Food Balances (FBS), which report groups of animals (e.g. poultry), and
  include processed meat (e.g. ham and sausages) converted back to carcass weight. Each group is split into its
  animals (e.g. chickens and turkeys) in proportion to their supply in the Supply Utilization Accounts (SCL), which
  start in 2010.
- FBS report the remaining animals (horses, rabbits, camels...) as "Meat, Other", together with processed meat of
  unknown species. So, for those animals, the meat supplied and produced come from SCL directly.
- FBS and SCL often report a different meat production than QCL, so the meat supplied and produced must come from
  the same source: the ratio between them is what carries over to QCL's animals killed.

This builds on van der Laan, S., Breeman, G., & Scherer, L. (2024). Animal Lives Affected by Meat Consumption Trends in
the G20 Countries. Animals, 14(11), 1662. https://doi.org/10.3390/ani14111662, extended from the G20 to all
countries, and from groups of animals to each animal.
"""

from typing import Any

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# Regions to create aggregates for.
REGIONS = list(geo.REGIONS) + ["World"]

# FBS item code of each group of animals whose supply is split among its animals.
FBS_GROUPS = {
    "cattle and buffaloes": "00002731",  # Meat, beef and buffalo.
    "sheep and goats": "00002732",  # Meat, sheep and goat.
    "pigs": "00002733",  # Meat, pig.
    "poultry": "00002734",  # Meat, poultry.
}

# For each animal: its FBS group (None if its supply comes from SCL directly), its QCL item code (for the animals
# killed and the meat produced), and its SCL items (for its supply). The animals and their QCL items are those of
# animals_used_for_food, except game animals, which FAOSTAT does not count.
ANIMALS: dict[str, dict[str, Any]] = {
    "cattle": {
        "fbs_group": "cattle and buffaloes",
        "qcl_item_code": "00000867",
        "scl_items": ["Meat of cattle with the bone, fresh or chilled", "Meat of cattle boneless, fresh or chilled"],
    },
    "buffaloes": {
        "fbs_group": "cattle and buffaloes",
        "qcl_item_code": "00000947",
        "scl_items": ["Meat of buffalo, fresh or chilled"],
    },
    "sheep": {
        "fbs_group": "sheep and goats",
        "qcl_item_code": "00000977",
        "scl_items": ["Meat of sheep, fresh or chilled"],
    },
    "goats": {
        "fbs_group": "sheep and goats",
        "qcl_item_code": "00001017",
        "scl_items": ["Meat of goat, fresh or chilled"],
    },
    "pigs": {
        "fbs_group": "pigs",
        "qcl_item_code": "00001035",
        "scl_items": ["Meat of pig with the bone, fresh or chilled", "Meat of pig boneless, fresh or chilled"],
    },
    "chickens": {
        "fbs_group": "poultry",
        "qcl_item_code": "00001058",
        "scl_items": ["Meat of chickens, fresh or chilled"],
    },
    "ducks": {"fbs_group": "poultry", "qcl_item_code": "00001069", "scl_items": ["Meat of ducks, fresh or chilled"]},
    "geese": {"fbs_group": "poultry", "qcl_item_code": "00001073", "scl_items": ["Meat of geese, fresh or chilled"]},
    "turkeys": {
        "fbs_group": "poultry",
        "qcl_item_code": "00001080",
        "scl_items": ["Meat of turkeys, fresh or chilled"],
    },
    "pigeons": {
        "fbs_group": "poultry",
        "qcl_item_code": "00001089",
        "scl_items": ["Meat of pigeons and other birds n.e.c., fresh, chilled or frozen"],
    },
    "horses": {"fbs_group": None, "qcl_item_code": "00001097", "scl_items": ["Horse meat, fresh or chilled"]},
    "donkeys": {"fbs_group": None, "qcl_item_code": "00001108", "scl_items": ["Meat of asses, fresh or chilled"]},
    "mules": {"fbs_group": None, "qcl_item_code": "00001111", "scl_items": ["Meat of mules, fresh or chilled"]},
    "camels": {"fbs_group": None, "qcl_item_code": "00001127", "scl_items": ["Meat of camels, fresh or chilled"]},
    # NOTE: SCL has no item for other camelids (llamas and alpacas); their supply is taken as their production.
    "other camelids": {"fbs_group": None, "qcl_item_code": "00001158", "scl_items": []},
    "rabbits": {
        "fbs_group": None,
        "qcl_item_code": "00001141",
        "scl_items": ["Meat of rabbits and hares, fresh or chilled"],
    },
    "other rodents": {
        "fbs_group": None,
        "qcl_item_code": "00001151",
        "scl_items": ["Meat of other domestic rodents, fresh or chilled"],
    },
}

# Groups reported as sums of their animals (pigs is both an animal and an FBS group).
GROUPS = {
    "cattle and buffaloes": ["cattle", "buffaloes"],
    "sheep and goats": ["sheep", "goats"],
    "poultry": ["chickens", "ducks", "geese", "turkeys", "pigeons"],
    "all land animals": list(ANIMALS),
}

# FBS elements giving the meat supplied to a country, in tonnes, and the name of the resulting number of animals.
# "Domestic supply" is production + imports - exports - stock variation, and includes waste and non-food uses.
# "Food" is the part of that supply that reaches people.
FBS_ELEMENTS = {"005301": "killed_for_domestic_supply", "005142": "killed_for_food"}

# FBS element code for meat production, in tonnes.
FBS_PRODUCTION = "005511"

# SCL element codes, in tonnes.
SCL_PRODUCTION, SCL_IMPORTS, SCL_EXPORTS, SCL_STOCK_VARIATION, SCL_FOOD = (
    "005510",
    "005610",
    "005910",
    "005071",
    "005141",
)

# QCL element codes for "Producing or slaughtered animals" (mammals and poultry have different codes assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]

# QCL element code for meat production, in tonnes.
PRODUCTION_ELEMENT_CODE = "005510"

# Countries with FBS data but no SCL data, which therefore have no estimates.
# NOTE: Remove countries from this list once SCL includes them.
COUNTRIES_WITHOUT_SCL = [
    "Benin",
    "Burundi",
    "Central African Republic",
    "Chad",
    "Dominica",
    "Japan",
    "Mali",
    "Somalia",
    "South Sudan",
    "Sudan",
    "Sudan (former)",
    "Togo",
]

# Countries in FAOSTAT that have no population data.
COUNTRIES_WITHOUT_POPULATION = ["Sudan (former)"]


def prepare_animals_per_tonne(tb_qcl: Table) -> Table:
    """Number of animals killed, meat produced, and animals killed per tonne of meat, for each animal."""
    qcl_item_code_to_animal = {info["qcl_item_code"]: animal for animal, info in ANIMALS.items()}
    tb = tb_qcl[tb_qcl["item_code"].isin(qcl_item_code_to_animal)].reset_index(drop=True)
    tb["animal"] = tb["item_code"].map(qcl_item_code_to_animal)
    keys = ["country", "year", "animal"]
    tb = pr.merge(
        tb[tb["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)][keys + ["value"]].rename(
            columns={"value": "killed_in_country"}, errors="raise"
        ),
        tb[tb["element_code"] == PRODUCTION_ELEMENT_CODE][keys + ["value"]].rename(
            columns={"value": "production"}, errors="raise"
        ),
        on=keys,
        how="outer",
    )
    tb["animals_per_tonne"] = tb["killed_in_country"] / tb["production"].replace(0, float("nan"))

    return tb


def scale_to_qcl_production(tb: Table, production_column: str, qcl_production_column: str) -> Table:
    """Scale the supply (domestic supply and food) by QCL production / the production reported with that supply.

    FBS and SCL sometimes report a different meat production than QCL (e.g. Brazil's beef). Since the animals per
    tonne come from QCL, this keeps the share of production supplied to the country (from FBS or SCL) instead of the
    tonnes, so that animals killed for consumption = animals killed in the country * supplied / produced.
    """
    produced = (tb[production_column] > 0) & (tb[qcl_production_column] > 0)
    for column in FBS_ELEMENTS.values():
        tb.loc[produced, column] = (
            tb.loc[produced, column] * tb.loc[produced, qcl_production_column] / tb.loc[produced, production_column]
        )

    return tb


def warn_on_production_mismatch(tb_fbs: Table) -> None:
    # Soft signal: list where FBS and QCL disagree on a group's meat production in the latest year, since there the
    # estimates rest on FBS's share of production supplied to the country, and the two cannot both be right.
    latest = tb_fbs[
        (tb_fbs["year"] == tb_fbs["year"].max())
        & ~tb_fbs["country"].isin(paths.regions.regions_all)
        & ~tb_fbs["country"].str.contains("(FAO)", regex=False)
    ]
    ratio = latest["fbs_production"] / latest["qcl_production"]
    mismatched = latest[(latest["qcl_production"] > 1e4) & ~ratio.between(0.8, 1.25)]
    if len(mismatched) > 0:
        listed = ", ".join(f"{row.country} ({row.fbs_group})" for row in mismatched.itertuples())
        log.warning(f"FBS and QCL meat production differ by over 25% in {latest['year'].max()}: {listed}")


def prepare_scl_supply(tb_scl: Table, tb_rates: Table) -> Table:
    """Tonnes of meat of each animal supplied to each country (domestic supply and food), according to SCL."""
    scl_item_to_animal = {item: animal for animal, info in ANIMALS.items() for item in info["scl_items"]}
    tb = tb_scl[tb_scl["item"].isin(scl_item_to_animal)].reset_index(drop=True)
    tb["animal"] = tb["item"].map(scl_item_to_animal)
    keys = ["country", "year", "animal"]
    # Domestic supply is production + imports - exports - stock variation (as in FBS).
    signs = {SCL_PRODUCTION: 1, SCL_IMPORTS: 1, SCL_EXPORTS: -1, SCL_STOCK_VARIATION: -1}
    tb_supply = tb[tb["element_code"].isin(signs)].reset_index(drop=True)
    tb_supply["value"] = tb_supply["value"] * tb_supply["element_code"].map(signs)
    tb_supply = tb_supply.groupby(keys, observed=True, as_index=False).agg({"value": "sum"})
    tb_food = tb[tb["element_code"] == SCL_FOOD].groupby(keys, observed=True, as_index=False).agg({"value": "sum"})
    tb_production = (
        tb[tb["element_code"] == SCL_PRODUCTION].groupby(keys, observed=True, as_index=False).agg({"value": "sum"})
    )
    tb = pr.merge(
        tb_supply.rename(columns={"value": "killed_for_domestic_supply"}, errors="raise"),
        tb_food.rename(columns={"value": "killed_for_food"}, errors="raise"),
        on=keys,
        how="outer",
    )
    tb = pr.merge(tb, tb_production.rename(columns={"value": "scl_production"}, errors="raise"), on=keys, how="left")
    tb = pr.merge(
        tb, tb_rates[keys + ["production"]].rename(columns={"production": "qcl_production"}), on=keys, how="left"
    )
    tb = scale_to_qcl_production(tb=tb, production_column="scl_production", qcl_production_column="qcl_production")
    tb = tb.drop(columns=["scl_production", "qcl_production"], errors="raise")

    # Where SCL covers a country-year but has no data on an animal (e.g. rabbits in North Korea, or other camelids,
    # which have no SCL item), the supply of that animal is its production (QCL), with no trade.
    tb_production = pr.merge(
        tb_rates[["country", "year", "animal", "production"]].dropna(),
        tb_scl[["country", "year"]].drop_duplicates(),
        on=["country", "year"],
        how="inner",
    )
    tb_production = pr.merge(tb_production, tb[keys], on=keys, how="left", indicator=True)
    tb_production = (
        tb_production[tb_production["_merge"] == "left_only"].drop(columns=["_merge"]).reset_index(drop=True)
    )
    tb_production["killed_for_domestic_supply"] = tb_production["production"]
    tb_production["killed_for_food"] = tb_production["production"]
    tb = pr.concat([tb, tb_production.drop(columns=["production"])], ignore_index=True)

    # A negative supply means a country exported more than it produced and imported (e.g. by re-exporting meat from
    # stocks), so it supplied none of that meat to itself.
    for column in FBS_ELEMENTS.values():
        tb[column] = tb[column].clip(lower=0)

    return tb


def prepare_meat_supply(tb_fbsc: Table, tb_scl_supply: Table, tb_rates: Table) -> Table:
    """Tonnes of meat of each animal supplied to each country (domestic supply and food), from SCL's first year."""
    # Supply and production of each FBS group.
    fbs_item_code_to_group = {item_code: group for group, item_code in FBS_GROUPS.items()}
    elements = {**FBS_ELEMENTS, FBS_PRODUCTION: "fbs_production"}
    tb_fbs = tb_fbsc[
        tb_fbsc["item_code"].isin(fbs_item_code_to_group)
        & tb_fbsc["element_code"].isin(elements)
        & (tb_fbsc["year"] >= tb_scl_supply["year"].min())
    ].reset_index(drop=True)
    tb_fbs["fbs_group"] = tb_fbs["item_code"].map(fbs_item_code_to_group)
    tb_fbs["element"] = tb_fbs["element_code"].map(elements)
    tb_fbs = tb_fbs.pivot(
        index=["country", "year", "fbs_group"], columns="element", values="value", join_column_levels_with="_"
    )
    # A few country-years report a negative supply, because FAOSTAT's exports exceed production plus imports minus
    # stock variation. A negative supply cannot be turned into a number of animals.
    for column in FBS_ELEMENTS.values():
        tb_fbs.loc[tb_fbs[column] < 0, column] = float("nan")

    # Scale each group's supply by QCL production / FBS production (see scale_to_qcl_production).
    animal_to_group = {animal: info["fbs_group"] for animal, info in ANIMALS.items() if info["fbs_group"]}
    tb_qcl_production = tb_rates[tb_rates["animal"].isin(animal_to_group)].reset_index(drop=True)
    tb_qcl_production["fbs_group"] = tb_qcl_production["animal"].map(animal_to_group)
    tb_qcl_production = tb_qcl_production.groupby(["country", "year", "fbs_group"], observed=True, as_index=False).agg(
        {"production": lambda values: values.sum(min_count=1)}
    )
    tb_fbs = pr.merge(
        tb_fbs,
        tb_qcl_production.rename(columns={"production": "qcl_production"}),
        on=["country", "year", "fbs_group"],
        how="left",
    )
    warn_on_production_mismatch(tb_fbs=tb_fbs)
    tb_fbs = scale_to_qcl_production(
        tb=tb_fbs, production_column="fbs_production", qcl_production_column="qcl_production"
    )
    tb_fbs = tb_fbs.drop(columns=["fbs_production", "qcl_production"], errors="raise")

    # Share of each animal in the domestic supply of its group, according to SCL.
    tb_shares = tb_scl_supply[tb_scl_supply["animal"].isin(animal_to_group)][
        ["country", "year", "animal", "killed_for_domestic_supply"]
    ].reset_index(drop=True)
    tb_shares["fbs_group"] = tb_shares["animal"].map(animal_to_group)
    group_supply = tb_shares.groupby(["country", "year", "fbs_group"], observed=True)[
        "killed_for_domestic_supply"
    ].transform("sum")
    tb_shares["share"] = tb_shares["killed_for_domestic_supply"] / group_supply.replace(0, float("nan"))

    # Split each group's FBS supply into its animals.
    tb = pr.merge(
        tb_shares[["country", "year", "animal", "fbs_group", "share"]],
        tb_fbs,
        on=["country", "year", "fbs_group"],
        how="inner",
    )
    for column in FBS_ELEMENTS.values():
        tb[column] = tb[column] * tb["share"]
    tb = tb.drop(columns=["fbs_group", "share"], errors="raise")

    # Animals outside the FBS groups take their supply from SCL directly.
    tb_others = tb_scl_supply[~tb_scl_supply["animal"].isin(animal_to_group)].reset_index(drop=True)
    tb = pr.concat([tb, tb_others], ignore_index=True)

    return tb


def sanity_check_inputs(tb_fbsc: Table, tb_scl: Table, tb_qcl: Table) -> None:
    tb_meat = tb_fbsc[tb_fbsc["item_code"].isin(FBS_GROUPS.values()) & tb_fbsc["element_code"].isin(FBS_ELEMENTS)]
    assert set(tb_meat["unit"]) == {"tonnes"}, "Unexpected units in FAOSTAT FBS meat supply."
    scl_items = {item for info in ANIMALS.values() for item in info["scl_items"]}
    error = f"SCL items not found: {scl_items - set(tb_scl['item'])}"
    assert scl_items <= set(tb_scl["item"]), error
    error = "Unexpected units in SCL."
    assert set(
        tb_scl[tb_scl["item"].isin(scl_items) & tb_scl["element_code"].isin([SCL_PRODUCTION, SCL_FOOD])]["unit"]
    ) == {"Tonnes"}, error
    qcl_item_codes = {info["qcl_item_code"] for info in ANIMALS.values()}
    error = f"QCL items not found: {qcl_item_codes - set(tb_qcl['item_code'])}"
    assert qcl_item_codes <= set(tb_qcl["item_code"]), error

    # Countries with FBS meat supply but no SCL data have no estimates, so they must be known.
    first_year = tb_scl["year"].min()
    fbs_countries = set(
        tb_fbsc[
            (tb_fbsc["year"] >= first_year)
            & tb_fbsc["item_code"].isin(FBS_GROUPS.values())
            & (tb_fbsc["element_code"] == "005301")
        ]["country"]
    )
    fbs_countries = {c for c in fbs_countries if c not in paths.regions.regions_all and "(FAO)" not in c}
    missing = fbs_countries - set(tb_scl["country"])
    error = f"Countries with FBS but no SCL data have changed: {sorted(missing ^ set(COUNTRIES_WITHOUT_SCL))}"
    assert missing == set(COUNTRIES_WITHOUT_SCL), error


def sanity_check_outputs(tb: Table, tb_rates: Table) -> None:
    # A world number of animals per tonne outside these ranges (in kg of meat per animal) would mean the method is
    # broken.
    expected_carcass_weight_ranges = {
        "cattle": (100, 400),
        "sheep": (8, 35),
        "goats": (8, 35),
        "pigs": (50, 130),
        "chickens": (0.8, 3),
    }
    world = tb_rates[tb_rates["country"] == "World"]
    assert len(world) > 0, "FAOSTAT QCL no longer includes a World aggregate."
    for animal, (minimum, maximum) in expected_carcass_weight_ranges.items():
        weights = 1000 / world[world["animal"] == animal]["animals_per_tonne"].dropna()
        error = f"World carcass weight for {animal} is outside {minimum}-{maximum} kg."
        assert weights.between(minimum, maximum).all(), error

    error = "Negative numbers of animals killed."
    assert (tb[list(FBS_ELEMENTS.values())].fillna(0) >= 0).all().all(), error

    # Main correctness check: for each animal, the animals killed for the consumption of all countries should roughly
    # match the animals killed in those same countries. They differ by stock changes, FBS residuals, and processed
    # meat of unknown species. Only countries with estimates in a given year are compared (e.g. FBS stop covering North
    # Korea in 2018, while QCL keeps counting its animals).
    covered = tb.groupby(["country", "year"], observed=True)["killed_for_domestic_supply"].transform("count") > 0
    compared = (
        tb[covered]
        .groupby(["year", "animal"], observed=True, as_index=False)
        .agg(
            {
                "killed_for_domestic_supply": lambda values: values.sum(min_count=1),
                "killed_in_country": lambda values: values.sum(min_count=1),
            }
        )
        .dropna()
    )
    # Only animals with at least 10 million killed a year (others are too few for a stable comparison).
    compared = compared[compared["killed_in_country"] > 1e7].reset_index(drop=True)
    compared["ratio"] = compared["killed_for_domestic_supply"] / compared["killed_in_country"]
    outliers = compared[~compared["ratio"].between(0.8, 1.2)]
    # NOTE: This should be an assertion. It is a warning only until this step uses the FAOSTAT release that fixes
    # India's beef and buffalo meat production in FBS (which leaves only buffaloes in 2012-2013 and pigeons in 2010
    # as outliers).
    if not outliers.empty:
        log.warning(
            f"Animals killed for consumption differ from those killed in the same countries by over 20%:\n{outliers}"
        )


def run() -> None:
    #
    # Load inputs.
    #
    # NOTE: FAOSTAT tables are large; keep their categoricals, or the staging server runs out of memory.
    tb_fbsc = paths.load_dataset("faostat_fbsc").read("faostat_fbsc", safe_types=False)
    tb_qcl = paths.load_dataset("faostat_qcl").read("faostat_qcl", safe_types=False)
    tb_scl = paths.load_dataset("faostat_scl").read("faostat_scl", safe_types=False)

    #
    # Process data.
    #
    sanity_check_inputs(tb_fbsc=tb_fbsc, tb_scl=tb_scl, tb_qcl=tb_qcl)

    # Animals killed per tonne of meat, and meat of each animal supplied to each country.
    tb_rates = prepare_animals_per_tonne(tb_qcl=tb_qcl)
    tb_scl_supply = prepare_scl_supply(tb_scl=tb_scl, tb_rates=tb_rates)
    tb = prepare_meat_supply(tb_fbsc=tb_fbsc, tb_scl_supply=tb_scl_supply, tb_rates=tb_rates)

    # Meat of an animal that a country does not produce itself is converted at the world's number of animals per tonne.
    world_rates = tb_rates[tb_rates["country"] == "World"][["year", "animal", "animals_per_tonne"]].rename(
        columns={"animals_per_tonne": "world_animals_per_tonne"}, errors="raise"
    )
    tb = pr.merge(
        tb, tb_rates[["country", "year", "animal", "animals_per_tonne"]], on=["country", "year", "animal"], how="left"
    )
    tb = pr.merge(tb, world_rates, on=["year", "animal"], how="left")
    uses_world_rate = tb["animals_per_tonne"].isna()
    tb["animals_per_tonne"] = tb["animals_per_tonne"].fillna(tb["world_animals_per_tonne"])
    for column in FBS_ELEMENTS.values():
        tb[column] = tb[column] * tb["animals_per_tonne"]
    tb = tb.drop(columns=["animals_per_tonne", "world_animals_per_tonne"], errors="raise")

    # Canary: if a future FAOSTAT release stopped publishing slaughter counts, most animals would quietly be
    # converted at the world rate and nothing else in this step would notice.
    share_world_rate = (
        tb.loc[uses_world_rate, "killed_for_domestic_supply"].sum() / tb["killed_for_domestic_supply"].sum()
    )
    error = f"{share_world_rate:.1%} of animals are converted at the world rate; FAOSTAT coverage may have changed."
    assert share_world_rate <= 0.05, error

    # Add the animals killed in each country (for all years, not only those with estimates).
    tb = pr.merge(
        tb_rates[["country", "year", "animal", "killed_in_country", "production"]],
        tb,
        on=["country", "year", "animal"],
        how="outer",
    )

    # FAOSTAT already includes aggregates (OWID regions, and FAO's own). Keep only countries, and build our own
    # aggregates as sums of member countries, so that a region's total is the sum of what its members consume.
    tb_countries = tb[
        ~tb["country"].isin(paths.regions.regions_all) & ~tb["country"].str.contains("(FAO)", regex=False)
    ].reset_index(drop=True)
    sanity_check_outputs(tb=tb_countries, tb_rates=tb_rates)
    tb = tb_countries

    # Add groups of animals, as sums of their animals.
    # NOTE: A country's number of animals killed in a group is left empty if any of its animals producing meat has
    # no slaughter count (Estonia reports 23,000 tonnes of chicken meat but no chicken slaughter count), since a
    # partial sum understates the group. Estimates sum the animals that are informed.
    tb["incomplete"] = tb["killed_in_country"].isna() & (tb["production"].fillna(0) > 0)
    tb = tb.drop(columns=["production"], errors="raise")
    metrics = ["killed_in_country"] + list(FBS_ELEMENTS.values())
    tb_groups = []
    for group, animals in GROUPS.items():
        tb_group = (
            tb[tb["animal"].isin(animals)]
            .groupby(["country", "year"], observed=True, as_index=False)
            .agg({**{metric: lambda values: values.sum(min_count=1) for metric in metrics}, "incomplete": "any"})
        )
        tb_group.loc[tb_group["incomplete"], "killed_in_country"] = float("nan")
        tb_group["animal"] = group
        tb_groups.append(tb_group)
    tb = pr.concat([tb, *tb_groups], ignore_index=True).drop(columns=["incomplete"], errors="raise")
    tb = tb.dropna(how="all", subset=metrics).reset_index(drop=True)

    # NOTE: Require at least one informed country per year, otherwise a year that FBS has not published yet would
    # sum to a hard zero and the curves would plunge in the final year.
    tb = paths.regions.add_aggregates(
        tb=tb, regions=REGIONS, index_columns=["country", "year", "animal"], min_num_values_per_year=1
    )
    tb = paths.regions.add_per_capita(
        tb=tb,
        regions=REGIONS,
        index_columns=["country", "year", "animal"],
        expected_countries_without_population=COUNTRIES_WITHOUT_POPULATION,
    )
    tb = tb.format(keys=["country", "year", "animal"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    paths.create_dataset(tables=[tb]).save()
