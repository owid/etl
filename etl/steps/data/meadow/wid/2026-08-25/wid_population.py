"""Load the WID population snapshot and create a meadow dataset.

The snapshot is the raw long-format response of WID's data API (one row per country, year and
variable, where variable is `npopul992i` for adults aged 20+ and `npopul999i` for the total
population). It is reshaped here to one column per variable, with descriptive names.

WID area codes are ISO alpha-2 plus WID's own additions (historical entities, subnational splits
and regional aggregates). Codes are mapped to country names with the OWID regions dataset, plus
the manual lists below for the non-ISO codes.
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# There is a country labeled NA (Namibia) which pandas reads as null without these parameters.
NA_VALUES = [""]

# WID variable codes in the snapshot, renamed to descriptive column names.
VARIABLE_NAMES = {
    "npopul992i": "adult_population",
    "npopul999i": "total_population",
}

# WID codes not in the ISO alpha-2 standard, mapped by hand: historical entities plus the nine
# canonical WID world regions and World (PPP variants).
# NOTE: kept in sync with meadow/wid/<version>/world_inequality_database.py — the population
# fetch also returns historical entities (Czechoslovakia, USSR, ...) the income fetch does not.
CODES_MISSING = {
    "CS": "Czechoslovakia",
    "DD": "East Germany",
    "KS": "Kosovo",
    "SU": "USSR",
    "XI": "Channel Islands",
    "YU": "Yugoslavia",
    "ZZ": "Zanzibar",
    "QE-PPP": "Europe (WID)",
    "QF-PPP": "Oceania (WID)",
    "XR-PPP": "Russia and Central Asia (WID)",
    "QL-PPP": "East Asia (WID)",
    "QP-PPP": "North America (WID)",
    "XF-PPP": "Sub-Saharan Africa (WID)",
    "XL-PPP": "Latin America (WID)",
    "XN-PPP": "Middle East and North Africa (WID)",
    "XS-PPP": "South and South-East Asia (WID)",
    "WO-PPP": "World",
}

# Dropped without warning, following the main WID meadow step: market-exchange-rate duplicates
# (population is identical to the PPP variants), "Other ..." residual aggregates, non-canonical
# subregions, and China's rural/urban split.
CODES_EXCLUDED = {
    "CN-RU",
    "CN-UR",
    "OA-MER",
    "OA-PPP",
    "OB-MER",
    "OB-PPP",
    "OC-MER",
    "OC-PPP",
    "OD-MER",
    "OD-PPP",
    "OE-MER",
    "OE-PPP",
    "OH-MER",
    "OH-PPP",
    "OI-MER",
    "OI-PPP",
    "OJ-MER",
    "OJ-PPP",
    "OK-MER",
    "OK-PPP",
    "OL-MER",
    "OL-PPP",
    "QE-MER",
    "QF-MER",
    "QL-MER",
    "QM-MER",
    "QM-PPP",
    "QP-MER",
    "WO-MER",
    "XB-MER",
    "XB-PPP",
    "XF-MER",
    "XL-MER",
    "XN-MER",
    "XR-MER",
    "XS-MER",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("wid_population.csv")
    tb = snap.read(keep_default_na=False, na_values=NA_VALUES)

    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions.read("regions")

    #
    # Process data.
    #
    tb = reshape_to_one_column_per_variable(tb=tb)

    tb = harmonize_countries(tb=tb, tb_regions=tb_regions)

    tb = tb.format(["country", "year"], short_name="wid_population")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def reshape_to_one_column_per_variable(tb: Table) -> Table:
    """Reshape the raw long API response to one column per WID variable, with descriptive names."""
    unexpected = sorted(set(tb["variable"]) - set(VARIABLE_NAMES))
    assert not unexpected, f"Unexpected WID variables in the population snapshot: {unexpected}"
    assert (tb["percentile"] == "p0p100").all(), "Population rows must all cover the full distribution (p0p100)."
    assert not tb.duplicated(subset=["country", "year", "variable"]).any(), "Duplicate (country, year, variable) rows."

    tb = tb.pivot(index=["country", "year"], columns="variable", values="value", join_column_levels_with="_")
    tb = tb.rename(columns=VARIABLE_NAMES)

    return tb


def harmonize_countries(tb: Table, tb_regions: Table) -> Table:
    """Map WID area codes to country names via OWID regions ISO alpha-2 codes plus the manual lists."""
    tb = pr.merge(tb, tb_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left")

    tb["name"] = tb["name"].astype(object)
    tb["country"] = tb["country"].astype(str)

    for code, name in CODES_MISSING.items():
        tb.loc[tb["country"] == code, "name"] = name

    # Any code that is neither ISO, manually mapped, nor deliberately excluded means WID added a
    # new area — it must be classified by hand, not silently dropped.
    missing = tb["name"].isna()
    unmapped = sorted(set(tb.loc[missing, "country"]) - CODES_EXCLUDED)
    assert not unmapped, f"Unmapped WID area codes (add them to CODES_MISSING or CODES_EXCLUDED): {unmapped}"

    tb = tb.loc[~missing].reset_index(drop=True)

    tb = tb.drop(columns=["country", "iso_alpha2"]).rename(columns={"name": "country"})

    return tb[["country", "year", "adult_population", "total_population"]]
