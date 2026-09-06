"""Combine every historical estimate of child work incidence into one comparable long table.

The estimates come from a dozen unrelated publications, each measuring a slightly different
thing: a different age band, one sex or both, a whole country or a single mill town, all
work or only factory work. Two tables come out of this step:

``child_work_incidence``
    Every observation, one row per (country, sex, age group, year), with a ``comparable``
    flag marking the rows that meet the target definition — a national estimate, from a
    census or administrative source, covering roughly ages 10-15, and counting any kind of
    work rather than one sector.

``child_work_incidence_chart``
    One row per (country, year), holding the single series per place that belongs on the
    long-run chart. ``share_all`` carries every estimate and ``share_comparable`` only the
    rows that meet the target definition, so the two versions of the chart are one indicator
    swap apart rather than two hand-curated datasets. Each column cites only the
    publications its own values come from.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Meadow tables, one per publication.
MEADOW_TABLES = [
    "canada",
    "canada_census",
    "cunningham_viazzo",
    "finland",
    "france",
    "roubaix",
    "germany",
    "netherlands",
    "tilburg",
    "spain_barcelona",
    "spain_manlleu",
]

# Rows meeting the target definition: national scope, census or administrative source,
# roughly ages 10-15, any kind of work. Keyed by (country, age_group), with an optional
# minimum year where the source's own authors advise dropping the earliest observation.
COMPARABLE_SERIES: dict[tuple[str, str], int | None] = {
    ("Canada", "10-14"): None,
    ("Colombia", "12-14"): None,
    ("England and Wales", "10-14"): None,
    ("France", "10-14"): None,
    ("Italy", "10-14"): None,
    ("Netherlands", "12-15"): None,
    # Carter and Sutch recommend dropping 1870, whose published rate their correction cannot repair.
    ("United States", "10-15"): 1880,
}

# The one series per place that belongs on the chart: which sex it is drawn for. Where a
# source reports both sexes as well as each separately, the combined figure is preferred;
# where it reports only boys, that is what the chart shows.
HEADLINE_SEX: dict[str, str] = {
    "Canada": "boys",
    "Colombia": "boys",
    "England and Wales": "boys",
    "Finland (Tampere)": "boys",
    "France": "both sexes",
    "France (Roubaix)": "boys",
    "Germany": "both sexes",
    "Italy": "both sexes",
    "Japan": "boys",
    "Japan (Yamanashi)": "boys",
    "Netherlands": "both sexes",
    "Netherlands (Tilburg)": "both sexes",
    "Spain (Barcelona)": "boys",
    "Spain (Manlleu)": "boys",
    "Spain (Sabadell)": "boys",
    "United States": "boys",
}

# Age bands in order of preference, most to least like the 10-14 target band, used when a
# source reports more than one band for the same place and year. Any band not listed sorts last.
AGE_PREFERENCE = ["10-14", "10-15", "12-14", "12-15", "10-13", "8-14", "8-15", "5-14", "13", "14"]

# England and Wales arrives as a single `age_group` column that folds in the sex.
ENGLAND_WALES_GROUPS = {
    "Boys 5-9": ("boys", "5-9"),
    "Girls 5-9": ("girls", "5-9"),
    "Boys 10-14": ("boys", "10-14"),
    "Girls 10-14": ("girls", "10-14"),
}

ITALY_COLUMNS = {
    "incidence_both_sexes": "both sexes",
    "incidence_boys": "boys",
    "incidence_girls": "girls",
}

# Only the corrected series is carried over; the as-published rates stay in the source
# dataset (data://garden/child_labor/2026-08-03/child_work_incidence_us).
US_COLUMNS = {
    "corrected_incidence_boys": "boys",
    "corrected_incidence_girls": "girls",
}

LONG_COLUMNS = ["country", "sex", "age_group", "year", "share", "coverage", "source"]


def _wide_to_long(tb: Table, sex_map: dict[str, str], country: str, age_group: str, source: str) -> Table:
    """Melt a wide (country, year) table with one column per sex into the long schema."""
    tb = tb[["country", "year"] + list(sex_map)].melt(id_vars=["country", "year"], var_name="sex", value_name="share")
    tb["sex"] = tb["sex"].map(sex_map)
    tb["age_group"] = age_group
    tb["coverage"] = "All work"
    tb["source"] = source
    assert set(tb["country"].unique()) == {country}, f"Expected {country} as the only entity."
    return tb[LONG_COLUMNS]


def _england_wales_to_long(tb: Table) -> Table:
    """Split the folded `Boys 10-14`-style age group into separate sex and age columns."""
    tb = tb[tb["age_group"].isin(ENGLAND_WALES_GROUPS)].copy()
    assert not tb.empty, "No England and Wales rows found."
    tb["sex"] = tb["age_group"].map(lambda g: ENGLAND_WALES_GROUPS[g][0])
    tb["age_group"] = tb["age_group"].map(lambda g: ENGLAND_WALES_GROUPS[g][1])
    tb["coverage"] = "All work"
    tb["source"] = "cunningham_viazzo_england_wales"
    return tb[LONG_COLUMNS]


def _flag_comparable(tb: Table) -> Table:
    """Mark the rows that meet the target definition."""
    tb["comparable"] = False
    for (country, age_group), min_year in COMPARABLE_SERIES.items():
        mask = (tb["country"] == country) & (tb["age_group"] == age_group)
        if min_year is not None:
            mask &= tb["year"] >= min_year
        tb.loc[mask, "comparable"] = True
    # The flag and the source label describe the same observations, so they carry the same origins.
    for col in ("comparable", "source"):
        tb[col] = tb[col].copy_metadata(tb["share"])
    return tb


def _headline(tb: Table, sex_of: dict[str, str]) -> Table:
    """Keep one row per (country, year): the preferred age band for the given sex."""
    tb = tb[tb["sex"] == tb["country"].map(sex_of)].copy()
    rank = {age: i for i, age in enumerate(AGE_PREFERENCE)}
    tb["_rank"] = tb["age_group"].map(lambda a: rank.get(a, len(rank)))
    tb = tb.sort_values(["country", "year", "_rank"]).drop_duplicates(subset=["country", "year"], keep="first")
    return tb.drop(columns="_rank")


def _cite_contributing_sources(tb: Table, column: str, sources: Table, origins_by_source: dict) -> None:
    """Attach to `column` only the origins of the publications that actually feed its values."""
    used = sources[tb[column].notna()].unique()
    origins = []
    for source in used:
        for origin in origins_by_source[source]:
            if origin not in origins:
                origins.append(origin)
    assert origins, f"No origins found for {column}."
    tb[column].metadata.origins = origins


def sanity_check_inputs(tb_italy: Table, tb_us: Table, tb_england_wales: Table, places: set[str]) -> None:
    assert set(ITALY_COLUMNS) <= set(tb_italy.columns), "Italy dataset no longer has the expected sex columns."
    assert set(US_COLUMNS) <= set(tb_us.columns), "US dataset no longer has the expected corrected columns."
    assert set(ENGLAND_WALES_GROUPS) <= set(tb_england_wales["age_group"]), (
        "England and Wales table no longer has the expected age groups."
    )
    # Each place in the snapshots must be one we know how to put on the chart.
    unknown = places - set(HEADLINE_SEX)
    assert not unknown, f"Places with no headline sex defined: {sorted(unknown)}"


def sanity_check_outputs(tb_long: Table, tb_chart: Table) -> None:
    assert tb_long["share"].between(0, 100).all(), "Share outside 0-100%."
    assert not tb_long.duplicated(subset=["country", "sex", "age_group", "year"]).any(), "Duplicate long rows."
    assert not tb_chart.duplicated(subset=["country", "year"]).any(), "Duplicate chart rows."
    assert tb_long["year"].between(1849, 1973).all(), "Year outside the range covered by these sources."
    assert tb_long["comparable"].sum() > 0, "No row met the comparable-estimate definition."

    # Every place that has a headline sex must reach the chart table, and vice versa.
    missing = set(HEADLINE_SEX) - set(tb_chart["country"].unique())
    assert not missing, f"Places with a headline sex but no chart rows: {sorted(missing)}"
    extra = set(tb_chart["country"].unique()) - set(HEADLINE_SEX)
    assert not extra, f"Chart rows for places with no headline sex: {sorted(extra)}"

    # The comparable series is the restricted chart, so it must hold exactly the places that
    # meet the target definition and nothing else.
    comparable_places = set(tb_chart.loc[tb_chart["share_comparable"].notna(), "country"].unique())
    assert comparable_places == {country for country, _ in COMPARABLE_SERIES}, (
        f"Unexpected comparable places: {sorted(comparable_places)}"
    )

    # Comparable values are a subset of all values, never a different number.
    both = tb_chart["share_comparable"].notna()
    assert (tb_chart.loc[both, "share_comparable"] == tb_chart.loc[both, "share_all"]).all(), (
        "Comparable and all-estimates values disagree."
    )

    # The restricted chart must not cite publications none of its values come from.
    restricted_producers = {o.producer for o in tb_chart["share_comparable"].metadata.origins}
    assert "Boentert" not in restricted_producers, "Restricted series cites a source it does not use."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("child_work_incidence")
    ds_italy = paths.load_dataset("child_work_incidence_italy")
    ds_us = paths.load_dataset("child_work_incidence_us")
    ds_long_run = paths.load_dataset("child_labor_long_run")

    tb_italy = ds_italy.read("child_work_incidence_italy")
    tb_us = ds_us.read("child_work_incidence_us")
    tb_england_wales = ds_long_run.read("england_wales")

    # One long table per publication, each tagged with the source it came from.
    parts = {}
    for name in MEADOW_TABLES:
        tb_part = ds_meadow.read(name)
        tb_part["source"] = name
        parts[name] = tb_part[LONG_COLUMNS]
    parts["cunningham_viazzo_england_wales"] = _england_wales_to_long(tb_england_wales)
    parts["toniolo_vecchi"] = _wide_to_long(tb_italy, ITALY_COLUMNS, "Italy", "10-14", "toniolo_vecchi")
    parts["carter_sutch"] = _wide_to_long(tb_us, US_COLUMNS, "United States", "10-15", "carter_sutch")

    #
    # Process data.
    #
    places = {place for part in parts.values() for place in part["country"].unique()}
    sanity_check_inputs(tb_italy, tb_us, tb_england_wales, places)

    # Remember each publication's origins before the tables are stacked and they merge.
    origins_by_source = {name: list(part["share"].metadata.origins) for name, part in parts.items()}

    tb = pr.concat(list(parts.values()), ignore_index=True)
    tb = tb.dropna(subset="share")
    tb = _flag_comparable(tb)

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Chart table: one series per place, with the two chart versions side by side.
    tb_headline = _headline(tb, HEADLINE_SEX)
    tb_girls = _headline(tb, dict.fromkeys(HEADLINE_SEX, "girls"))

    tb_chart = tb_headline[["country", "year", "share"]].rename(columns={"share": "share_all"})
    tb_chart["share_comparable"] = tb_headline["share"].where(tb_headline["comparable"])
    tb_chart["_source"] = tb_headline["source"]

    tb_girls_out = tb_girls[["country", "year", "share"]].rename(columns={"share": "share_girls_all"})
    tb_girls_out["share_girls_comparable"] = tb_girls["share"].where(tb_girls["comparable"])
    tb_girls_out["_source_girls"] = tb_girls["source"]
    tb_chart = tb_chart.merge(tb_girls_out, on=["country", "year"], how="outer")

    for column, source_col in [
        ("share_all", "_source"),
        ("share_comparable", "_source"),
        ("share_girls_all", "_source_girls"),
        ("share_girls_comparable", "_source_girls"),
    ]:
        _cite_contributing_sources(tb_chart, column, tb_chart[source_col], origins_by_source)
    tb_chart = tb_chart.drop(columns=["_source", "_source_girls"])

    sanity_check_outputs(tb, tb_chart)

    tb = tb.format(["country", "sex", "age_group", "year"], short_name=paths.short_name)
    tb_chart = tb_chart.format(["country", "year"], short_name=f"{paths.short_name}_chart")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_chart], default_metadata=ds_meadow.metadata)
    ds_garden.save()
