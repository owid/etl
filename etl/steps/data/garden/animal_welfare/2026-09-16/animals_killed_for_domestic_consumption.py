"""Estimate the number of land animals killed to supply each country's own consumption of meat.

Our other indicators (animal_welfare/*/animals_used_for_food) count animals where they are slaughtered.
That attributes exported meat to the exporting country, so big exporters look worse than their diets warrant,
and big importers look better. Here we instead attribute animals to the country that consumes the meat.

For each country, year and animal group:

    animals = meat supplied (tonnes) * 1000 / carcass weight per animal (kg)
    carcass weight per animal = QCL production (tonnes) * 1000 / QCL animals slaughtered

Meat supplied comes from FAOSTAT Food Balance Sheets, for two different elements (see MEAT_ELEMENTS), and the
carcass weight comes from FAOSTAT QCL. Both FBS meat and QCL meat production are in carcass weight equivalent,
so they divide directly.

This follows the approach of:
    van der Laan, S., Breeman, G., & Scherer, L. (2024). Animal Lives Affected by Meat Consumption Trends in the
    G20 Countries. Animals, 14(11), 1662. https://doi.org/10.3390/ani14111662
extended from the G20 to all countries. One deliberate difference: that paper converts all quantities from carcass
weight to boneless retail weight before dividing. That conversion cancels out (it is applied to both the meat
consumed and the meat produced), so we skip it and work in carcass weight throughout.
"""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Regions to create aggregates for.
REGIONS = list(geo.REGIONS) + ["World"]

# Animal groups: for each group, the FBS item code for the meat consumed, and the QCL item codes for the species
# whose slaughter and production give the group's carcass weight.
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

# Name of the metric counting animals slaughtered in a country, regardless of where their meat is consumed.
SLAUGHTERED_METRIC = "slaughtered"

# FBS elements giving the meat supplied to a country, in tonnes.
# "Domestic supply" is production + imports - exports - stock variation, and includes waste and non-food uses.
# "Food" is the part of that supply that reaches people.
MEAT_ELEMENTS = {
    "005301": "domestic_supply",
    "005142": "food",
}

# QCL element codes for "Producing or slaughtered animals" (mammals and poultry have different codes assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]

# QCL element code for meat production, in tonnes.
PRODUCTION_ELEMENT_CODE = "005510"

# Range of carcass weight (in kg per animal) accepted for an individual country.
# NOTE: This affects the output, it is not just a check. A country-year whose carcass weight falls outside its
# group's range is treated as unreliable, and the world average is used for it instead.
# The ranges are wide, because breeds and slaughter ages genuinely differ a lot between countries: cattle
# carcasses average ~450kg in Japan and averaged ~55kg in Bangladesh in the 1970s.
ACCEPTED_CARCASS_WEIGHT_RANGES = {
    "cattle_and_buffaloes": (30, 550),
    "sheep_and_goats": (3, 70),
    "pigs": (8, 220),
    "poultry": (0.3, 6),
}

# Countries in FAOSTAT that have no population data.
COUNTRIES_WITHOUT_POPULATION = ["Sudan (former)"]


def column_name(animal_group: str, metric: str) -> str:
    """Name of the output column for an animal group and a metric."""
    if metric == SLAUGHTERED_METRIC:
        return f"{animal_group}_slaughtered"

    return f"{animal_group}_killed_{metric}"


def prepare_carcass_weights(tb_qcl: Table) -> Table:
    """Compute the average carcass weight (kg per animal) of each group, country and year."""
    qcl_item_code_to_group = {
        item_code: group for group, info in ANIMAL_GROUPS.items() for item_code in info["qcl_item_codes"]
    }

    tb = tb_qcl[
        tb_qcl["item_code"].isin(qcl_item_code_to_group)
        & tb_qcl["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES + [PRODUCTION_ELEMENT_CODE])
    ].reset_index(drop=True)
    tb["animal_group"] = tb["item_code"].map(qcl_item_code_to_group)

    # Sum the species of each group, separately for the number of animals slaughtered and the meat produced.
    # A group's slaughter count is only usable if every species that reports meat production also reports how
    # many animals were slaughtered. FAOSTAT often publishes one without the other (Estonia reports 23,000 tonnes
    # of chicken meat but no chicken slaughter count), and summing what is there would understate the group, or
    # even claim a country slaughters no animals at all.
    species_with_production = set(
        map(
            tuple,
            tb[(tb["element_code"] == PRODUCTION_ELEMENT_CODE) & (tb["value"] > 0)][
                ["country", "year", "animal_group", "item_code"]
            ].values,
        )
    )
    species_with_slaughter = set(
        map(
            tuple,
            tb[tb["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES) & tb["value"].notna()][
                ["country", "year", "animal_group", "item_code"]
            ].values,
        )
    )
    groups_missing_a_species = {key[:3] for key in species_with_production - species_with_slaughter}

    tb_slaughtered = (
        tb[tb["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)]
        .groupby(["country", "year", "animal_group"], observed=True, as_index=False)
        .agg({"value": lambda values: values.sum(min_count=1)})
        .rename(columns={"value": "slaughtered_animals"}, errors="raise")
    )
    incomplete = [
        key in groups_missing_a_species
        for key in map(tuple, tb_slaughtered[["country", "year", "animal_group"]].values)
    ]
    tb_slaughtered.loc[incomplete, "slaughtered_animals"] = float("nan")

    tb_production = (
        tb[tb["element_code"] == PRODUCTION_ELEMENT_CODE]
        .groupby(["country", "year", "animal_group"], observed=True, as_index=False)
        .agg({"value": lambda values: values.sum(min_count=1)})
        .rename(columns={"value": "production_tonnes"}, errors="raise")
    )
    tb_weights = tb_slaughtered.merge(tb_production, on=["country", "year", "animal_group"], how="outer")

    # A handful of country-years report meat production with zero animals slaughtered; they have no usable weight.
    tb_weights["carcass_weight"] = (
        tb_weights["production_tonnes"] * 1000 / tb_weights["slaughtered_animals"].replace(0, float("nan"))
    )

    return tb_weights


def sanity_check_world_carcass_weights(tb_weights: Table) -> None:
    # Range of carcass weight (in kg per animal) expected for the world average of each group. These are much
    # tighter than the ranges accepted for individual countries, because a world average outside them would mean
    # the method is broken.
    expected_ranges = {
        "cattle_and_buffaloes": (100, 400),
        "sheep_and_goats": (8, 35),
        "pigs": (50, 130),
        "poultry": (0.8, 3),
    }

    tb_world = tb_weights[tb_weights["country"] == "World"]
    error = "World carcass weights are missing; FAOSTAT QCL may no longer include a World aggregate."
    assert len(tb_world) > 0, error
    for animal_group, (minimum, maximum) in expected_ranges.items():
        weights = tb_world[tb_world["animal_group"] == animal_group]["carcass_weight"].dropna()
        error = (
            f"World carcass weight for {animal_group} is outside the expected range "
            f"({minimum}-{maximum} kg): found {weights.min():.1f}-{weights.max():.1f} kg."
        )
        assert weights.between(minimum, maximum).all(), error


def prepare_meat_supply(tb_fbsc: Table) -> Table:
    """Extract the tonnes of meat supplied to each country, per group and year."""
    fbs_item_code_to_group = {info["fbs_item_code"]: group for group, info in ANIMAL_GROUPS.items()}

    tb = tb_fbsc[
        tb_fbsc["item_code"].isin(fbs_item_code_to_group) & tb_fbsc["element_code"].isin(MEAT_ELEMENTS)
    ].reset_index(drop=True)
    tb["animal_group"] = tb["item_code"].map(fbs_item_code_to_group)
    tb["element"] = tb["element_code"].map(MEAT_ELEMENTS)

    error = "Unexpected units in FAOSTAT FBS meat supply."
    assert set(tb["unit"]) == {"tonnes"}, error

    tb = tb.pivot(
        index=["country", "year", "animal_group"], columns="element", values="value", join_column_levels_with="_"
    )

    # A few country-years report a negative domestic supply, because FAOSTAT's exports exceed production plus
    # imports minus stock variation (most often pig meat in countries that barely consume it). A negative supply
    # cannot be turned into a number of animals, so discard those values instead of assuming they are zero.
    for element in MEAT_ELEMENTS.values():
        negative = tb[element] < 0
        if negative.any():
            paths.log.warning(
                f"Discarding {negative.sum()} country-year-groups with a negative '{element}' of meat "
                f"(most often: {', '.join(tb[negative]['country'].value_counts().head(3).index)})."
            )
            tb.loc[negative, element] = float("nan")

    return tb


def estimate_animals_killed(tb_supply: Table, tb_weights: Table) -> Table:
    """Convert tonnes of meat supplied into numbers of animals, using carcass weights."""
    # World carcass weights, used wherever a country's own weight is missing or implausible.
    tb_world_weights = tb_weights[tb_weights["country"] == "World"][["year", "animal_group", "carcass_weight"]].rename(
        columns={"carcass_weight": "world_carcass_weight"}, errors="raise"
    )

    tb = tb_supply.merge(
        tb_weights[["country", "year", "animal_group", "carcass_weight"]],
        on=["country", "year", "animal_group"],
        how="left",
    ).merge(tb_world_weights, on=["year", "animal_group"], how="left")

    # Discard country carcass weights that fall outside the plausible range for their group.
    for animal_group, (minimum, maximum) in ACCEPTED_CARCASS_WEIGHT_RANGES.items():
        implausible = (tb["animal_group"] == animal_group) & ~tb["carcass_weight"].between(minimum, maximum)
        tb.loc[implausible, "carcass_weight"] = float("nan")

    # Fall back on the world carcass weight where the country has none.
    tb["uses_world_carcass_weight"] = tb["carcass_weight"].isna()
    tb["carcass_weight"] = tb["carcass_weight"].fillna(tb["world_carcass_weight"])

    # Maximum fraction of country-year-groups that may take the world carcass weight instead of their own.
    # Only used for the sanity check below: most countries that import their meat have no slaughter data at all.
    maximum_fraction_of_fallbacks = 0.15

    fraction_of_fallbacks = tb["uses_world_carcass_weight"].mean()
    error = (
        f"{fraction_of_fallbacks:.1%} of carcass weights fall back on the world average, more than the expected "
        f"{maximum_fraction_of_fallbacks:.0%}. FAOSTAT QCL coverage may have changed."
    )
    assert fraction_of_fallbacks <= maximum_fraction_of_fallbacks, error

    for element in MEAT_ELEMENTS.values():
        tb[element] = tb[element] * 1000 / tb["carcass_weight"]

    tb = tb.drop(columns=["carcass_weight", "world_carcass_weight", "uses_world_carcass_weight"], errors="raise")

    return tb


def sanity_check_animals_killed(tb: Table, tb_weights: Table) -> None:
    # Maximum relative difference accepted between the world number of animals implied by domestic supply and the
    # world number of animals slaughtered reported by QCL.
    maximum_world_discrepancy = 0.15

    error = "Negative numbers of animals killed."
    assert (tb[list(MEAT_ELEMENTS.values())].fillna(0) >= 0).all().all(), error

    # The main correctness check of the method: at the world level, the animals implied by domestic supply should
    # roughly match the animals that QCL says were slaughtered. They differ only by stock changes, FBS residuals,
    # and the countries that report to one dataset but not the other.
    tb_world = tb[tb["country"] == "World"][["year", "animal_group", "domestic_supply"]]
    tb_slaughtered = tb_weights[tb_weights["country"] == "World"][["year", "animal_group", "slaughtered_animals"]]
    compared = tb_world.merge(tb_slaughtered, on=["year", "animal_group"], how="inner").dropna()
    discrepancy = abs(compared["domestic_supply"] - compared["slaughtered_animals"]) / compared["slaughtered_animals"]
    worst = compared.loc[discrepancy.idxmax()]
    error = (
        f"World animals implied by domestic supply differ from QCL animals slaughtered by up to "
        f"{discrepancy.max():.1%} (worst: {worst['animal_group']} in {worst['year']}), more than the expected "
        f"{maximum_world_discrepancy:.0%}."
    )
    assert discrepancy.max() <= maximum_world_discrepancy, error


def warn_on_food_exceeding_domestic_supply(tb: Table) -> None:
    """Food is a part of domestic supply, so it should never exceed it.

    A number of country-years break this in the original FAOSTAT data. Warn instead of failing, since it is a flaw
    of the source, not of this step.
    """
    inconsistent = tb[tb["food"] > tb["domestic_supply"]]
    if len(inconsistent) > 0:
        paths.log.warning(
            f"{len(inconsistent)} country-year-groups where FAOSTAT reports more meat as food than as domestic "
            f"supply (most often: {', '.join(inconsistent['country'].value_counts().head(3).index)})."
        )


def run() -> None:
    #
    # Load inputs.
    #
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl")

    #
    # Process data.
    #
    # Compute the average carcass weight of each group, country and year.
    tb_weights = prepare_carcass_weights(tb_qcl=tb_qcl)
    sanity_check_world_carcass_weights(tb_weights=tb_weights)

    # Extract the tonnes of meat supplied to each country.
    tb_supply = prepare_meat_supply(tb_fbsc=tb_fbsc)

    # Convert tonnes of meat into numbers of animals.
    tb = estimate_animals_killed(tb_supply=tb_supply, tb_weights=tb_weights)

    sanity_check_animals_killed(tb=tb, tb_weights=tb_weights)

    # Add the number of animals actually slaughtered in each country, to compare against the numbers killed for
    # its own consumption. Merge on the outside, so that countries reporting slaughter but no meat supply (or the
    # other way around) keep the metric they do have.
    tb = tb.merge(
        tb_weights[["country", "year", "animal_group", "slaughtered_animals"]].rename(
            columns={"slaughtered_animals": SLAUGHTERED_METRIC}, errors="raise"
        ),
        on=["country", "year", "animal_group"],
        how="outer",
    )
    metrics = list(MEAT_ELEMENTS.values()) + [SLAUGHTERED_METRIC]

    # FAOSTAT already includes aggregates (OWID regions, and FAO's own). Drop them all, and build our own
    # aggregates as sums of member countries, so that a region's total is the sum of what its members consume.
    tb = tb[
        ~tb["country"].isin(paths.regions.regions_all) & ~tb["country"].str.contains("(FAO)", regex=False)
    ].reset_index(drop=True)

    warn_on_food_exceeding_domestic_supply(tb=tb)

    # Move animal groups into columns, and add the total across groups.
    tb = tb.pivot(
        index=["country", "year"],
        columns="animal_group",
        values=metrics,
        join_column_levels_with="_",
    )
    # NOTE: Sum the groups that are informed, rather than requiring all of them. Otherwise a country that is
    # missing one group (e.g. Turkey, whose pig meat supply is discarded for being negative) would have no total
    # at all, and would then be missing from region totals while still counting towards each region's per-group
    # totals.
    for metric in metrics:
        columns = [f"{metric}_{animal_group}" for animal_group in ANIMAL_GROUPS]
        tb[f"{metric}_{TOTAL_GROUP}"] = tb[columns].sum(axis=1, min_count=1)

    tb = tb.rename(
        columns={
            f"{metric}_{animal_group}": column_name(animal_group=animal_group, metric=metric)
            for metric in metrics
            for animal_group in list(ANIMAL_GROUPS) + [TOTAL_GROUP]
        },
        errors="raise",
    )

    # The outer merge above can leave country-years with no data at all in either dataset.
    tb = tb.dropna(
        subset=[column_name(animal_group=group, metric=metric) for group in ANIMAL_GROUPS for metric in metrics],
        how="all",
    ).reset_index(drop=True)

    # Add region aggregates and per capita indicators.
    # NOTE: Require at least one informed country per year. The consumption metrics come from FAO's Food Balance
    # Sheets, which stop a year earlier than the production data, so without this every region would sum a year of
    # missing values into a hard zero, and the curves would plunge to zero in the final year.
    tb = paths.regions.add_aggregates(tb=tb, regions=REGIONS, min_num_values_per_year=1)
    tb = paths.regions.add_per_capita(
        tb=tb, regions=REGIONS, expected_countries_without_population=COUNTRIES_WITHOUT_POPULATION
    )

    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
