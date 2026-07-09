"""Combine historical (pre-1961) HYDE mammal counts with FAOSTAT livestock stocks (1961 onwards).

This produces a long-run "Livestock counts" dataset: the number of live animals by species, for
individual countries and for regions (World, OWID continents, and FAO's own continental regions),
from 1890 to the latest FAOSTAT year.

Two sources are spliced:

- FAOSTAT QCL provides annual stocks from 1961 onwards, for every country, every FAO/OWID region,
  and every livestock species.
- The HYDE historical livestock table (Klein Goldewijk, 2005) provides decadal figures (1890-1950
  used here) for eight mammal species only. We use it to extend backwards those entities that HYDE
  can fill: World, the continents, and the three single countries HYDE reports (Canada, United
  States, Japan).

HYDE only tracks mammals, so all other species (birds, camelids, rabbits, etc.) begin in 1961.

The HYDE-to-region aggregation was validated against FAOSTAT over the overlap years (1961-1998):
aggregating HYDE's sub-regions to continents reproduces FAOSTAT's regional data to within ~1% for
every continent and species, except Oceania pigs (HYDE's Papua New Guinea estimate is ~2x FAOSTAT's).
"""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Last HYDE year to keep, and first FAOSTAT year. HYDE also reports 1960-1998, but those years were
# themselves scaled to a 1998 vintage of FAOSTAT, so we drop them in favour of the current FAOSTAT
# data and keep only the genuinely historical decadal points (1890-1950).
LAST_HYDE_YEAR = 1950
FIRST_FAOSTAT_YEAR = 1961

# The eight mammal species HYDE tracks (meadow column name == output column name).
HYDE_MAMMALS = ["asses", "buffalo", "cattle", "goats", "horses", "mules", "pigs", "sheep"]

# FAOSTAT QCL "Stocks" items to keep, mapped from the item-name prefix of the flattened column
# ("{item}__{item_code}__stocks__{element_code}__animals") to the output column name. The composite
# items "cattle_and_buffaloes" and "sheep_and_goats", and "bees" (hives, not animals), are excluded.
FAOSTAT_SPECIES = {
    "asses": "asses",
    "buffalo": "buffalo",
    "camels": "camels",
    "cattle": "cattle",
    "chickens": "chickens",
    "ducks": "ducks",
    "geese": "geese",
    "goats": "goats",
    "horses": "horses",
    "mules_and_hinnies": "mules",
    "other_birds": "other_birds",
    "other_camelids": "other_camelids",
    "other_rodents": "other_rodents",
    "poultry": "poultry",
    "rabbits": "rabbits",
    "sheep": "sheep",
    "swine__pigs": "pigs",
    "turkeys": "turkeys",
}

# HYDE historical entities and the sub-regions that make them up. The same continents appear under
# both OWID names and FAO "(FAO)" names because HYDE's regions match both (validated); note the two
# schemes differ only for the Americas (OWID's "North America" includes Central America and the
# Caribbean, while FAO's "Northern America" does not). HYDE's "CIS" (former USSR) goes to Europe,
# consistent with FAOSTAT reporting the USSR as a single European entity until its 1991 dissolution.
_AFRICA = ["N.Africa", "W.Africa", "E.Africa", "S.Africa"]
_ASIA = ["M.East", "S.Asia", "E.Asia", "SE.Asia", "Japan"]
_EUROPE = ["W.Europe", "E.Europe", "CIS"]
HYDE_ENTITY_TO_SUBREGIONS = {
    # OWID continents.
    "Africa": _AFRICA,
    "Asia": _ASIA,
    "Europe": _EUROPE,
    "North America": ["Canada", "USA", "C.America", "Greenland"],
    "South America": ["S.America"],
    "Oceania": ["Oceania"],
    # FAO continental regions.
    "Africa (FAO)": _AFRICA,
    "Asia (FAO)": _ASIA,
    "Europe (FAO)": _EUROPE,
    "Northern America (FAO)": ["Canada", "USA", "Greenland"],
    "South America (FAO)": ["S.America"],
    "Oceania (FAO)": ["Oceania"],
    # Single countries HYDE reports (mapped to OWID country names).
    "Canada": ["Canada"],
    "United States": ["USA"],
    "Japan": ["Japan"],
}

# FAO continental regions to keep from FAOSTAT (individual countries and OWID continents are kept
# automatically; see select_faostat_entities). Non-geographic FAO aggregates (income groups, FAO
# sub-regions, historical composites like "Belgium-Luxembourg (FAO)") are dropped.
FAO_REGIONS_TO_KEEP = {
    "Africa (FAO)",
    "Asia (FAO)",
    "Europe (FAO)",
    "Oceania (FAO)",
    "Northern America (FAO)",
    "Central America (FAO)",
    "Caribbean (FAO)",
    "South America (FAO)",
}

# Non-geographic aggregates that carry no "(FAO)" suffix and must be excluded explicitly.
OWID_AGGREGATES_TO_DROP = {
    "European Union (27)",
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
}


def select_faostat_entities(countries: set) -> list:
    """Keep individual countries, OWID continents and World (non-"(FAO)", non-aggregate), plus the
    chosen FAO continental regions."""
    keep = []
    for country in countries:
        is_fao = "(FAO)" in country
        if (not is_fao and country not in OWID_AGGREGATES_TO_DROP) or (country in FAO_REGIONS_TO_KEEP):
            keep.append(country)
    return keep


def prepare_faostat(tb: Table) -> Table:
    """Select livestock stocks (number of animals) for all species and the chosen entities."""
    tb = tb.reset_index()

    # Find and rename the flattened stock column for each species.
    rename = {}
    for item_prefix, species in FAOSTAT_SPECIES.items():
        matches = [
            column
            for column in tb.columns
            if column.startswith(item_prefix + "__") and "__stocks__" in column and column.endswith("__animals")
        ]
        assert len(matches) == 1, f"Expected exactly one FAOSTAT stock column for '{item_prefix}', found {matches}."
        rename[matches[0]] = species

    tb = tb[["country", "year"] + list(rename)].rename(columns=rename, errors="raise")

    # Keep the chosen entities and years from 1961 onwards.
    entities = select_faostat_entities(set(tb["country"]))
    tb = tb[(tb["country"].isin(entities)) & (tb["year"] >= FIRST_FAOSTAT_YEAR)].reset_index(drop=True)

    return tb


def prepare_hyde(tb: Table) -> Table:
    """Aggregate HYDE sub-regions into the historical entities, in number of animals."""
    tb = tb.reset_index()

    # Convert from thousands of animals to number of animals.
    for mammal in HYDE_MAMMALS:
        tb[mammal] = tb[mammal] * 1000

    frames = []
    for entity, subregions in HYDE_ENTITY_TO_SUBREGIONS.items():
        part = tb[tb["country"].isin(subregions)].groupby("year", observed=True, as_index=False)[HYDE_MAMMALS].sum()
        part["country"] = entity
        frames.append(part)
    # World is reported directly in the source.
    world = tb[tb["country"] == "World"][["year"] + HYDE_MAMMALS].copy()
    world["country"] = "World"
    frames.append(world)

    tb = pr.concat(frames, ignore_index=True)

    # Keep only the historical decadal points.
    tb = tb[tb["year"] <= LAST_HYDE_YEAR].reset_index(drop=True)

    return tb


def sanity_check_inputs(tb_hyde_raw: Table, tb_faostat: Table) -> None:
    tb = tb_hyde_raw.reset_index()

    # Every HYDE sub-region used in the mapping must exist in the meadow table.
    hyde_regions = set(tb["country"])
    used = {sub for subs in HYDE_ENTITY_TO_SUBREGIONS.values() for sub in subs}
    assert used <= hyde_regions, f"HYDE sub-regions missing from meadow: {used - hyde_regions}."

    # HYDE sub-regions must sum to the reported World totals (Greenland is all zeros).
    subregions = [r for r in hyde_regions if r != "World"]
    for mammal in HYDE_MAMMALS:
        subregion_sum = tb[tb["country"].isin(subregions)].groupby("year", observed=True)[mammal].sum()
        world = tb[tb["country"] == "World"].set_index("year")[mammal]
        assert ((subregion_sum - world).abs() <= 1e-3 * world.abs()).all(), (
            f"HYDE sub-regions do not sum to World for '{mammal}'."
        )

    # FAOSTAT must provide every historical entity we intend to extend backwards.
    missing = set(HYDE_ENTITY_TO_SUBREGIONS) | {"World"}
    missing -= set(tb_faostat["country"])
    assert not missing, f"FAOSTAT is missing entities that HYDE extends: {missing}."


def sanity_check_outputs(tb: Table) -> None:
    species = [column for column in tb.columns if column not in ["country", "year"]]

    # No fully-empty column, non-negative counts, and unique (country, year).
    assert not tb[species].isna().all().any(), "Output has a fully-NaN species column."
    assert (tb[species].fillna(0) >= 0).all().all(), "Negative livestock count found."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."

    # Only the eight HYDE mammals may have pre-1961 data; every other species must start in 1961.
    non_mammals = [s for s in species if s not in HYDE_MAMMALS]
    pre = tb[tb["year"] < FIRST_FAOSTAT_YEAR]
    assert pre[non_mammals].isna().all().all(), "A non-mammal species unexpectedly has pre-1961 data."
    assert not pre[HYDE_MAMMALS].isna().all().any(), "A HYDE mammal is missing all pre-1961 data."

    # Soft check: report the 1950->1961 cattle splice step for the historical entities. Some step is
    # expected (an 11-year gap of real growth); a very large jump would flag a splice problem.
    historical_entities = list(HYDE_ENTITY_TO_SUBREGIONS) + ["World"]
    for entity in historical_entities:
        series = tb[tb["country"] == entity].set_index("year")["cattle"].dropna()
        if LAST_HYDE_YEAR in series.index and FIRST_FAOSTAT_YEAR in series.index:
            before, after = float(series[LAST_HYDE_YEAR]), float(series[FIRST_FAOSTAT_YEAR])
            if before > 0 and abs(100 * (after - before) / before) > 40:
                change = 100 * (after - before) / before
                log.warning(
                    f"Cattle jumps {change:+.0f}% across the {LAST_HYDE_YEAR}-{FIRST_FAOSTAT_YEAR} gap for "
                    f"{entity} (decadal HYDE to annual FAOSTAT; usually real growth over the 11-year gap, "
                    f"but review if unexpectedly large)."
                )


def run() -> None:
    #
    # Load inputs.
    #
    ds_hyde = paths.load_dataset("historical_livestock_mammals")
    tb_hyde_raw = ds_hyde["historical_livestock_mammals"]

    ds_faostat = paths.load_dataset("faostat_qcl")
    tb_faostat_raw = ds_faostat["faostat_qcl_flat"]

    #
    # Process data.
    #
    tb_faostat = prepare_faostat(tb_faostat_raw)
    tb_hyde = prepare_hyde(tb_hyde_raw)

    sanity_check_inputs(tb_hyde_raw, tb_faostat)

    # Splice: HYDE (1890-1950) then FAOSTAT (1961 onwards). The years do not overlap.
    tb = pr.concat([tb_hyde, tb_faostat], ignore_index=True)
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)

    # Harmonize units across sources: all counts are numbers of animals. This also clears FAOSTAT's
    # inherited "An"/"1000 An" short units (its bird series were already scaled to head upstream).
    for column in tb.columns:
        if column not in ["country", "year"]:
            tb[column].metadata.unit = "animals"
            tb[column].metadata.short_unit = ""

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
