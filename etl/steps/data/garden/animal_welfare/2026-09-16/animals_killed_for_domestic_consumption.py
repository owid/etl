"""Estimate the number of land animals killed to supply each country's own consumption of meat.

Our other indicators (animal_welfare/*/animals_used_for_food) count animals where they are slaughtered. That
attributes exported meat to the exporting country, so big exporters look worse than their diets warrant, and big
importers look better. Here we instead attribute animals to the country that consumes the meat:

    animals killed for consumption = animals slaughtered * (meat supplied / meat produced)

Both quantities come from FAOSTAT: the meat supplied from the Food Balance Sheets, the animals slaughtered and the
meat produced from QCL. Countries that slaughter no animals of their own have no ratio, so for them the meat
supplied is converted at the world average number of animals per tonne.

This follows the approach of van der Laan, S., Breeman, G., & Scherer, L. (2024). Animal Lives Affected by Meat
Consumption Trends in the G20 Countries. Animals, 14(11), 1662. https://doi.org/10.3390/ani14111662, extended from
the G20 to all countries. That paper converts all quantities from carcass weight to boneless retail weight first;
the conversion cancels out, so we skip it.
"""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Regions to create aggregates for.
REGIONS = list(geo.REGIONS) + ["World"]

# Animal groups: for each group, the FBS item code for the meat consumed, and the QCL item codes for the species
# whose slaughter and production give the group's number of animals per tonne.
ANIMAL_GROUPS = {
    "cattle_and_buffaloes": {
        "fbs_item_code": "00002731",  # Meat, beef and buffalo.
        "qcl_item_codes": [
            "00000867",  # Meat of cattle with the bone, fresh or chilled.
            "00000947",  # Meat, buffalo.
        ],
    },
    "sheep_and_goats": {
        "fbs_item_code": "00002732",  # Meat, sheep and goat.
        "qcl_item_codes": [
            "00000977",  # Meat, lamb and mutton.
            "00001017",  # Meat, goat.
        ],
    },
    "pigs": {
        "fbs_item_code": "00002733",  # Meat, pig.
        "qcl_item_codes": [
            "00001035",  # Meat, pig.
        ],
    },
    "poultry": {
        "fbs_item_code": "00002734",  # Meat, poultry.
        "qcl_item_codes": [
            "00001058",  # Meat, chicken.
            "00001069",  # Meat, duck.
            "00001073",  # Meat, goose and guinea fowl.
            "00001080",  # Meat, turkey.
            "00001089",  # Meat of pigeons and other birds n.e.c., fresh, chilled or frozen.
        ],
    },
}

# Name of the column with the sum of all animal groups.
TOTAL_GROUP = "land_animals"

# FBS elements giving the meat supplied to a country, in tonnes.
# "Domestic supply" is production + imports - exports - stock variation, and includes waste and non-food uses.
# "Food" is the part of that supply that reaches people.
MEAT_ELEMENTS = {"005301": "killed_domestic_supply", "005142": "killed_food"}

# QCL element codes for "Producing or slaughtered animals" (mammals and poultry have different codes assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]

# QCL element code for meat production, in tonnes.
PRODUCTION_ELEMENT_CODE = "005510"

# Countries in FAOSTAT that have no population data.
COUNTRIES_WITHOUT_POPULATION = ["Sudan (former)"]


def prepare_animals_per_tonne(tb_qcl: Table) -> Table:
    """Number of animals slaughtered, and animals slaughtered per tonne of meat, for each group, country and year."""
    qcl_item_code_to_group = {
        item_code: group for group, info in ANIMAL_GROUPS.items() for item_code in info["qcl_item_codes"]
    }
    tb = tb_qcl[tb_qcl["item_code"].isin(qcl_item_code_to_group)].reset_index(drop=True)
    tb["animal_group"] = tb["item_code"].map(qcl_item_code_to_group)
    keys = ["country", "year", "animal_group", "item_code"]

    tb = (
        tb[tb["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)][keys + ["value"]]
        .rename(columns={"value": "slaughtered"}, errors="raise")
        .merge(
            tb[tb["element_code"] == PRODUCTION_ELEMENT_CODE][keys + ["value"]].rename(
                columns={"value": "production"}, errors="raise"
            ),
            on=keys,
            how="outer",
        )
    )

    # FAOSTAT often reports one of the two quantities without the other (Estonia reports 23,000 tonnes of chicken
    # meat but no chicken slaughter count). The rate is computed only from species reporting both, so that no
    # species enters the denominator that is missing from the numerator. The slaughter count is left empty unless
    # every species producing meat reports one, since a partial sum understates the group (and Estonia's poultry
    # would otherwise read as zero).
    tb["paired_slaughtered"] = tb["slaughtered"].where(tb["production"].notna())
    tb["paired_production"] = tb["production"].where(tb["slaughtered"].notna())
    tb["complete"] = tb["slaughtered"].notna() | (tb["production"].fillna(0) == 0)

    tb = tb.groupby(["country", "year", "animal_group"], observed=True, as_index=False).agg(
        slaughtered=("slaughtered", lambda values: values.sum(min_count=1)),
        complete=("complete", "all"),
        paired_slaughtered=("paired_slaughtered", "sum"),
        paired_production=("paired_production", "sum"),
    )
    tb.loc[~tb["complete"], "slaughtered"] = float("nan")
    tb["animals_per_tonne"] = tb["paired_slaughtered"] / tb["paired_production"].replace(0, float("nan"))
    tb = tb.drop(columns=["complete", "paired_slaughtered", "paired_production"], errors="raise")

    return tb


def prepare_meat_supply(tb_fbsc: Table) -> Table:
    """Tonnes of meat supplied to each country, per group and year."""
    fbs_item_code_to_group = {info["fbs_item_code"]: group for group, info in ANIMAL_GROUPS.items()}
    tb = tb_fbsc[
        tb_fbsc["item_code"].isin(fbs_item_code_to_group) & tb_fbsc["element_code"].isin(MEAT_ELEMENTS)
    ].reset_index(drop=True)
    assert set(tb["unit"]) == {"tonnes"}, "Unexpected units in FAOSTAT FBS meat supply."
    tb["animal_group"] = tb["item_code"].map(fbs_item_code_to_group)
    tb["element"] = tb["element_code"].map(MEAT_ELEMENTS)
    tb = tb.pivot(
        index=["country", "year", "animal_group"], columns="element", values="value", join_column_levels_with="_"
    )

    # A few country-years report a negative supply, because FAOSTAT's exports exceed production plus imports minus
    # stock variation. A negative supply cannot be turned into a number of animals.
    for element in MEAT_ELEMENTS.values():
        tb.loc[tb[element] < 0, element] = float("nan")

    return tb


def sanity_check(tb: Table, tb_rates: Table) -> None:
    # A world average outside these ranges (in kg per animal) would mean the method is broken.
    expected_carcass_weight_ranges = {
        "cattle_and_buffaloes": (100, 400),
        "sheep_and_goats": (8, 35),
        "pigs": (50, 130),
        "poultry": (0.8, 3),
    }
    world = tb_rates[tb_rates["country"] == "World"]
    assert len(world) > 0, "FAOSTAT QCL no longer includes a World aggregate."
    for animal_group, (minimum, maximum) in expected_carcass_weight_ranges.items():
        weights = 1000 / world[world["animal_group"] == animal_group]["animals_per_tonne"].dropna()
        error = f"World carcass weight for {animal_group} is outside {minimum}-{maximum} kg."
        assert weights.between(minimum, maximum).all(), error

    assert (tb[list(MEAT_ELEMENTS.values())].fillna(0) >= 0).all().all(), "Negative numbers of animals killed."

    # Main correctness check: at world level the animals implied by domestic supply should roughly match the
    # animals QCL says were slaughtered. They differ only by stock changes and FBS residuals.
    compared = (
        tb[tb["country"] == "World"][["year", "animal_group", "killed_domestic_supply"]]
        .merge(world[["year", "animal_group", "slaughtered"]], on=["year", "animal_group"], how="inner")
        .dropna()
    )
    discrepancy = abs(compared["killed_domestic_supply"] - compared["slaughtered"]) / compared["slaughtered"]
    assert discrepancy.max() <= 0.15, f"World totals differ from QCL slaughter by up to {discrepancy.max():.1%}."


def run() -> None:
    #
    # Load inputs.
    #
    tb_fbsc = paths.load_dataset("faostat_fbsc").read("faostat_fbsc")
    tb_qcl = paths.load_dataset("faostat_qcl").read("faostat_qcl")

    #
    # Process data.
    #
    tb_rates = prepare_animals_per_tonne(tb_qcl=tb_qcl)
    tb = prepare_meat_supply(tb_fbsc=tb_fbsc).merge(tb_rates, on=["country", "year", "animal_group"], how="outer")

    # Countries that slaughter no animals of their own have no rate; convert their meat at the world's.
    world_rates = tb_rates[tb_rates["country"] == "World"][["year", "animal_group", "animals_per_tonne"]].rename(
        columns={"animals_per_tonne": "world_animals_per_tonne"}, errors="raise"
    )
    tb = tb.merge(world_rates, on=["year", "animal_group"], how="left")
    tb["animals_per_tonne"] = tb["animals_per_tonne"].fillna(tb["world_animals_per_tonne"])

    for element in MEAT_ELEMENTS.values():
        tb[element] = tb[element] * tb["animals_per_tonne"]

    sanity_check(tb=tb, tb_rates=tb_rates)

    tb = tb.drop(columns=["animals_per_tonne", "world_animals_per_tonne"], errors="raise")

    # FAOSTAT already includes aggregates (OWID regions, and FAO's own). Drop them all, and build our own as sums
    # of member countries, so that a region's total is the sum of what its members consume.
    tb = tb[
        ~tb["country"].isin(paths.regions.regions_all) & ~tb["country"].str.contains("(FAO)", regex=False)
    ].reset_index(drop=True)

    # Move animal groups into columns, and add the total across groups.
    metrics = list(MEAT_ELEMENTS.values()) + ["slaughtered"]
    tb = tb.pivot(index=["country", "year"], columns="animal_group", values=metrics, join_column_levels_with="_")
    # NOTE: Sum the groups that are informed, rather than requiring all of them. Otherwise a country missing one
    # group would have no total at all, while still counting towards each region's per-group totals.
    for metric in metrics:
        columns = [f"{metric}_{animal_group}" for animal_group in ANIMAL_GROUPS]
        tb[f"{metric}_{TOTAL_GROUP}"] = tb[columns].sum(axis=1, min_count=1)
    tb = tb.rename(
        columns={
            f"{metric}_{animal_group}": f"{animal_group}_{metric}"
            for metric in metrics
            for animal_group in list(ANIMAL_GROUPS) + [TOTAL_GROUP]
        },
        errors="raise",
    )
    tb = tb.dropna(how="all", subset=[c for c in tb.columns if c not in ("country", "year")]).reset_index(drop=True)

    # NOTE: Require at least one informed country per year, otherwise a year that FBS has not published yet would
    # sum to a hard zero and the curves would plunge in the final year.
    tb = paths.regions.add_aggregates(tb=tb, regions=REGIONS, min_num_values_per_year=1)
    tb = paths.regions.add_per_capita(
        tb=tb, regions=REGIONS, expected_countries_without_population=COUNTRIES_WITHOUT_POPULATION
    )
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    paths.create_dataset(tables=[tb]).save()
