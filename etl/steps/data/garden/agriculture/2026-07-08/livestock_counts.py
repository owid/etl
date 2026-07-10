"""Combine historical (pre-1961) HYDE mammal counts with FAOSTAT livestock stocks (1961 onwards).

This produces a long-run "Livestock counts" dataset: the number of live animals by species, for
individual countries and for regions (World and OWID continents), from 1890 to the latest FAOSTAT
year.

Two sources are spliced:

- FAOSTAT QCL provides annual stocks from 1961 onwards, for every country, every OWID region, and
  every livestock species.
- The HYDE historical livestock table (Klein Goldewijk, 2005) provides decadal figures (1890-1950
  used here) for eight mammal species only. We use it to extend backwards those entities that HYDE
  can fill: World, the continents, and the three single countries HYDE reports (Canada, United
  States, Japan).

HYDE only tracks mammals, so all other species (birds, camelids, rabbits, etc.) begin in 1961.

"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

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

# HYDE historical entities and the sub-regions that make them up. We only build OWID continents (not
# FAO's own regions): HYDE's regions were validated to match both, but extending FAO regions with
# HYDE data would make them no longer FAO's own aggregates, and they coincide with the OWID ones
# anyway. HYDE's "CIS" (former USSR) goes to Europe, consistent with FAOSTAT reporting the USSR as a
# single European entity until its 1991 dissolution.
HYDE_ENTITY_TO_SUBREGIONS = {
    # OWID continents.
    "Africa": ["N.Africa", "W.Africa", "E.Africa", "S.Africa"],
    "Asia": ["M.East", "S.Asia", "E.Asia", "SE.Asia", "Japan"],
    "Europe": ["W.Europe", "E.Europe", "CIS"],
    "North America": ["Canada", "USA", "C.America", "Greenland"],
    "South America": ["S.America"],
    "Oceania": ["Oceania"],
    # Single countries HYDE reports (mapped to OWID country names).
    "Canada": ["Canada"],
    "United States": ["USA"],
    "Japan": ["Japan"],
}

# Regional aggregates to exclude from FAOSTAT: all of FAO's own regions (they mix badly with HYDE and
# duplicate the OWID continents) and the non-geographic OWID aggregates. Everything else (individual
# countries, OWID continents, and World) is kept.
OWID_AGGREGATES_TO_DROP = {
    "European Union (27)",
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
}


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
    entities = [c for c in set(tb["country"]) if "(FAO)" not in c and c not in OWID_AGGREGATES_TO_DROP]
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

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
