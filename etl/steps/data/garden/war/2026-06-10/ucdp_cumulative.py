"""Cumulative deaths in armed conflicts since 1989, by conflict type, for countries, UCDP regions and the world.

Sums the yearly deaths in ongoing conflicts from the UCDP garden dataset (best estimate, by the location where the
deaths occurred) over 1989 to the latest year, and divides them by the population in 1989 to get cumulative death
rates per 100,000 people. Each entity gets a single row, stamped with the latest year.

This replaces the fast-track dataset `fasttrack/latest/cumulative_conflict_deaths_ucdp`, which was built outside ETL
with the same method:
- Countries use OWID's population estimate for 1989.
- UCDP regions and the world use the population of the Gleditsch & Ward states in each region in 1989, which is the
  population the UCDP garden step uses for its yearly death rates.
- Entities with no population estimate for 1989 (e.g. East and West Germany, Abkhazia) are dropped.
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# First year of UCDP's Georeferenced Event Dataset, from which the deaths are summed.
FIRST_YEAR = 1989

# Conflict types in the UCDP garden dataset, and the short names they get here. Extrasystemic conflicts are left out:
# there have been none since 1989.
CONFLICT_TYPES = {
    "all": "all",
    "intrastate": "intrastate",
    "one-sided violence": "onesided",
    "non-state conflict": "nonstate",
    "interstate": "interstate",
}
DEATHS_COLUMN = "number_deaths_ongoing_conflicts"

# Region names in the UCDP garden dataset carry this suffix; the regions table of the Gleditsch dataset does not.
REGION_SUFFIX = " (UCDP)"


def run() -> None:
    #
    # Load inputs.
    #
    tb_ucdp = paths.load_dataset("ucdp").read("ucdp")
    tb_regions = paths.load_dataset("gleditsch").read("gleditsch_regions")

    #
    # Process data.
    #
    last_year = int(tb_ucdp["year"].max())
    sanity_check_inputs(tb_ucdp, last_year)

    tb = cumulative_deaths(tb_ucdp, last_year)
    tb = add_population_in_first_year(tb, tb_regions)
    tb = add_death_rates(tb)
    tb["year"] = last_year

    sanity_check_outputs(tb)
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb], yaml_params={"first_year": FIRST_YEAR, "last_year": last_year}
    )
    ds_garden.save()


def cumulative_deaths(tb: Table, last_year: int) -> Table:
    """Sum yearly deaths in ongoing conflicts over FIRST_YEAR..last_year, one column per conflict type."""
    tb = tb.loc[
        (tb["year"] >= FIRST_YEAR) & (tb["year"] <= last_year) & tb["conflict_type"].isin(CONFLICT_TYPES),
        ["country", "conflict_type", DEATHS_COLUMN],
    ]

    tables = []
    for conflict_type, short in CONFLICT_TYPES.items():
        tb_type = (
            tb.loc[tb["conflict_type"] == conflict_type]
            .groupby("country", observed=True, as_index=False)[DEATHS_COLUMN]
            .sum()
            .rename(columns={DEATHS_COLUMN: f"{short}_deaths"})
        )
        tables.append(tb_type)

    tb_cumulative = tables[0]
    for tb_type in tables[1:]:
        tb_cumulative = pr.merge(tb_cumulative, tb_type, on="country", how="outer")
    tb_cumulative["country"] = tb_cumulative["country"].astype(str)
    return tb_cumulative


def add_population_in_first_year(tb: Table, tb_regions: Table) -> Table:
    """Add each entity's population in FIRST_YEAR, and drop entities that have none."""
    is_region = tb["country"].str.endswith(REGION_SUFFIX) | (tb["country"] == "World")

    # Countries: OWID's population estimate.
    tb_countries = tb.loc[~is_region].reset_index(drop=True)
    tb_countries["year"] = FIRST_YEAR
    tb_countries = paths.regions.add_population(tb=tb_countries, warn_on_missing_countries=False)

    # UCDP regions and the world: the population of the Gleditsch & Ward states in each region.
    tb_regions = tb_regions.loc[tb_regions["year"] == FIRST_YEAR, ["region", "population"]].copy()
    tb_regions["region"] = tb_regions["region"].astype(str)
    tb_regions.loc[tb_regions["region"] != "World", "region"] += REGION_SUFFIX
    tb_regions = tb_regions.rename(columns={"region": "country"})
    tb_regions_with_deaths = pr.merge(tb.loc[is_region], tb_regions, on="country", how="left", validate="one_to_one")
    assert tb_regions_with_deaths["population"].notna().all(), "A UCDP region has no population in the Gleditsch data."

    tb = pr.concat([tb_countries.drop(columns=["year"]), tb_regions_with_deaths], ignore_index=True)

    # Drop entities without a population estimate, as the fast-track version did.
    missing = sorted(tb.loc[tb["population"].isna(), "country"])
    if missing:
        paths.log.warning(f"Dropping entities without a population estimate for {FIRST_YEAR}: {missing}")
    tb = tb.dropna(subset=["population"]).reset_index(drop=True)
    return tb


def add_death_rates(tb: Table) -> Table:
    """Cumulative deaths per 100,000 people, relative to the population in FIRST_YEAR."""
    for short in CONFLICT_TYPES.values():
        tb[f"{short}_rate"] = tb[f"{short}_deaths"] / tb["population"] * 100_000
    return tb.drop(columns=["population"])


def sanity_check_inputs(tb: Table, last_year: int) -> None:
    world = tb.loc[(tb["country"] == "World") & (tb["conflict_type"] == "all")]
    first_year_with_data = world.loc[world[DEATHS_COLUMN].notna(), "year"].min()
    assert first_year_with_data == FIRST_YEAR, f"UCDP deaths data now starts in {first_year_with_data}."
    assert set(world["year"]) >= set(range(FIRST_YEAR, last_year + 1)), "Years are missing from the world totals."
    assert set(CONFLICT_TYPES) <= set(tb["conflict_type"]), "A conflict type is missing from the UCDP data."
    assert (tb[DEATHS_COLUMN].dropna() >= 0).all(), "Negative death counts in the UCDP data."


def sanity_check_outputs(tb: Table) -> None:
    deaths = [f"{short}_deaths" for short in CONFLICT_TYPES.values() if short != "all"]
    # The conflict types add up to all conflicts, for every entity.
    mismatch = tb.loc[(tb[deaths].sum(axis=1) - tb["all_deaths"]).abs() > 0.5, "country"]
    assert mismatch.empty, f"Conflict types do not add up to all conflicts for: {sorted(mismatch)}"

    # The UCDP regions add up to the world.
    world = tb.loc[tb["country"] == "World", "all_deaths"].item()
    regions = tb.loc[tb["country"].str.endswith(REGION_SUFFIX), "all_deaths"]
    assert len(regions) == 5, f"Expected 5 UCDP regions, found {len(regions)}."
    assert abs(regions.sum() - world) <= 0.5, f"Regions add up to {regions.sum()}, the world total is {world}."

    assert not tb["country"].duplicated().any(), "Duplicate entities in the output."
    assert tb.notna().all().all(), "Missing values in the output."
