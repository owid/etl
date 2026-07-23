"""Combine census (1850-2000, decennial) and American Community Survey (2005-, annual) data on
the foreign-born population of the United States.

Outputs three tables:
- us_foreign_born_population: total foreign-born population and its share of the total
  population, 1850-2024.
- by_country_of_birth: foreign-born population by country of birth — at each census from 1850
  to 1930 and from 1960 to 2000, then annually from 2005 from the American Community Survey.
- by_region_of_birth: the same, aggregated to continents using OWID region definitions
  (rebuilt from the sources' rows, regrouping Latin America into North and South America).
"""

import re

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Source rows of Table 4 that we publish as countries, mapped to the entity we show. Their
# sub-rows are skipped: "Great Britain" extends the United Kingdom before 1930 (when the UK
# row is not reported and Northern Ireland is included in Ireland); "Turkey in Europe" and
# "Turkey in Asia" are summed into Turkey; "Portugal (total)" includes the Azores and Madeira.
CENSUS_ENTITIES = {
    c: c
    for c in [
        "Ireland",
        "Denmark",
        "Finland",
        "Iceland",
        "Norway",
        "Sweden",
        "Belgium",
        "Luxembourg",
        "Netherlands",
        "Austria",
        "France",
        "Germany",
        "Switzerland",
        "Greece",
        "Italy",
        "Spain",
        "Albania",
        "Bulgaria",
        "Czechoslovakia",
        "Estonia",
        "Hungary",
        "Latvia",
        "Lithuania",
        "Poland",
        "Romania",
        "Soviet Union (former)",
        "Yugoslavia",
        "Armenia",
        "China",
        "India",
        "Japan",
        "Palestine",
        "Syria",
        "Cuba",
        "Mexico",
        "Canada",
        "Australia",
        "Sandwich Islands (Hawaii)",
    ]
}
CENSUS_ENTITIES.update(
    {
        "United Kingdom": "United Kingdom",
        "Great Britain": "United Kingdom",
        "Turkey in Europe": "Turkey",
        "Turkey in Asia": "Turkey",
        "Portugal (total)": "Portugal",
    }
)

# Continent of each branch of the source's hierarchy, for naming "(not specified)" entities.
CENSUS_BRANCH_REGIONS = {
    "Europe": "Europe",
    "Asia": "Asia",
    "Africa": "Africa",
    "Oceania": "Oceania",
    "Caribbean": "North America",
    "Central America": "North America",
    "South America": "South America",
    "Northern America": "North America",
}

# ACS rows we take whole, skipping their sub-rows.
ACS_PARENT_COUNTRIES = ["United Kingdom", "United Kingdom (inc. Crown Dependencies)", "Portugal"]

# ACS leaf rows that are not countries.
ACS_NON_COUNTRY = (
    r"Other |n\.e\.c|excluding England and Scotland|^West Indies$|^Middle Africa$|^England$|^Scotland$|^Azores Islands$"
)


def adjust_soviet_asian_part(tb: Table) -> Table:
    """Align the census table with our convention that the Soviet Union counts in Europe.

    The source's "Soviet Union (former)" row includes its Asian part (footnote 3), but where
    the source reports the European part separately (the "In Europe" sub-row, in 2000), its
    own aggregates count the Asian part under Asia. We keep the full Soviet Union in Europe,
    so we move that amount from Asia's rows to Europe's."""
    piv = tb.pivot(index="country", columns="year", values="foreign_born_population")
    tb = tb.copy()
    for year in piv.columns:
        in_europe = piv.at["In Europe", year]
        if pd.isna(in_europe):
            continue
        delta = piv.at["Soviet Union (former)", year] - in_europe
        for country, sign in [
            ("Other Asia", -1),
            ("Asia", -1),
            ("Eastern Europe", 1),
            ("Southern and Eastern Europe", 1),
            ("Europe", 1),
        ]:
            mask = (tb["country"] == country) & (tb["year"] == year)
            tb.loc[mask, "foreign_born_population"] += sign * delta
    return tb


def cover_census_tree(tb_origin: Table) -> Table:
    """Walk the census table's hierarchy so that every person is counted exactly once.

    Rows in CENSUS_ENTITIES are taken whole (their sub-rows are skipped). Everyone else goes
    to a "<region> (not specified)" entity — including the remainders of rows whose sub-rows
    do not add up to them — or to "Not specified" when the source gives no region.
    """
    nodes = tb_origin[["country", "line", "depth"]].drop_duplicates().sort_values("line").to_dict("records")
    # The source's indentation is inconsistent for two rows: "Southern and Eastern Europe" and
    # "Europe n.e.c" are siblings of "Northern and Western Europe" (their values sum to the
    # Europe row), but carry one more leading dot.
    for node in nodes:
        if node["country"] in ("Southern and Eastern Europe", "Europe n.e.c"):
            node["depth"] = 2
    values = tb_origin.pivot(index="country", columns="year", values="foreign_born_population")

    # Parent of each node: the closest previous node with a smaller depth.
    for i, node in enumerate(nodes):
        node["children"] = []
        node["parent"] = None
        for prev in reversed(nodes[:i]):
            if prev["depth"] < node["depth"]:
                node["parent"] = prev
                prev["children"].append(node)
                break

    def residual_entity(node) -> str:
        while node is not None:
            if node["country"] in CENSUS_BRANCH_REGIONS:
                return f"{CENSUS_BRANCH_REGIONS[node['country']]} (not specified)"
            node = node["parent"]
        return "Not specified"

    rows = []

    def cover(node, year) -> float:
        value = values.at[node["country"], year]
        value = None if pd.isna(value) else float(value)
        if node["country"] in CENSUS_ENTITIES and value is not None:
            rows.append({"country": CENSUS_ENTITIES[node["country"]], "year": year, "foreign_born_population": value})
            return value
        if node["children"]:
            covered = sum(cover(child, year) for child in node["children"])
            if value is None:
                return covered
            remainder = value - covered
            assert remainder > -1, f"{node['country']} {year}: sub-rows add up to more than the row."
            if remainder > 0:
                rows.append({"country": residual_entity(node), "year": year, "foreign_born_population": remainder})
            return value
        if value is not None:
            rows.append({"country": residual_entity(node), "year": year, "foreign_born_population": value})
            return value
        return 0.0

    roots = [n for n in nodes if n["parent"] is None and n["country"] != "Total"]
    for year in values.columns:
        covered = sum(cover(root, year) for root in roots)
        assert abs(covered - values.at["Total", year]) < 1, f"{year}: entities do not add up to the total."

    tb = Table(pd.DataFrame(rows)).groupby(["country", "year"], as_index=False)["foreign_born_population"].sum()
    tb = Table(tb)
    tb["foreign_born_population"] = tb["foreign_born_population"].copy_metadata(tb_origin["foreign_born_population"])
    return tb


def cover_acs_tree(tb_origin: Table) -> Table:
    """Same exact-cover walk for the ACS table, whose hierarchy lives in the "path" column."""
    rows = []
    for year, sub in tb_origin.groupby("year"):
        values = {p: (None if pd.isna(v) else float(v)) for p, v in zip(sub["path"], sub["foreign_born_population"])}
        children = {p: [] for p in values}
        for p in values:
            parent = " / ".join(p.split(" / ")[:-1])
            if parent in children:
                children[parent].append(p)

        def region_of(path: str) -> str:
            parts = path.split(" / ")
            if parts[1] in ("Europe", "Asia", "Africa", "Oceania"):
                return parts[1]
            if "South America" in parts:
                return "South America"
            return "North America"

        def cover(path: str) -> float:
            value = values[path]
            name = path.split(" / ")[-1]
            if name in ACS_PARENT_COUNTRIES and value is not None:
                rows.append({"country": name, "year": year, "foreign_born_population": value})
                return value
            if children[path]:
                covered = sum(cover(child) for child in children[path])
                if value is None:
                    return covered
                remainder = value - covered
                assert remainder > -1, f"{path} {year}: sub-rows add up to more than the row."
                if remainder > 0:
                    rows.append(
                        {
                            "country": f"{region_of(path)} (not specified)",
                            "year": year,
                            "foreign_born_population": remainder,
                        }
                    )
                return value
            if value is None:
                return 0.0
            if re.search(ACS_NON_COUNTRY, name):
                rows.append(
                    {"country": f"{region_of(path)} (not specified)", "year": year, "foreign_born_population": value}
                )
            else:
                rows.append({"country": name, "year": year, "foreign_born_population": value})
            return value

        covered = sum(cover(p) for p in children["Total"])
        assert abs(covered - values["Total"]) < 1, f"{year}: entities do not add up to the total."

    tb = Table(pd.DataFrame(rows)).groupby(["country", "year"], as_index=False)["foreign_born_population"].sum()
    tb = Table(tb)
    tb["foreign_born_population"] = tb["foreign_born_population"].copy_metadata(tb_origin["foreign_born_population"])
    return tb


def sanity_check(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years after combining census and ACS."
    assert (tb["foreign_born_population"] < tb["total_population"]).all(), "Foreign-born exceeds total population."
    share = tb.set_index("year")["share_foreign_born"]
    # Values printed in the census working paper.
    assert abs(share.loc[1890] - 14.8) < 0.05, "1890 share does not match the working paper."
    assert abs(share.loc[1970] - 4.7) < 0.05, "1970 share does not match the working paper."
    assert ((share > 4) & (share < 17)).all(), "Share outside the plausible historical range."


def make_annual(tb_census: Table, tb_acs: Table) -> Table:
    # The two sources do not overlap: census years run to 2000, ACS years start in 2005.
    tb = pr.concat([tb_census, tb_acs], ignore_index=True).sort_values("year")
    tb["share_foreign_born"] = tb["foreign_born_population"] / tb["total_population"] * 100
    tb["country"] = "United States"

    sanity_check(tb)

    tb = tb.drop(columns=["total_population"])
    return tb


# Continents following OWID region definitions, built from the source's own aggregate rows.
# The source groups the Americas as "Latin America" + "Northern America"; OWID counts the
# Caribbean and Central America (incl. Mexico) as part of North America.
REGION_ROWS = {
    "Europe": ["Europe"],
    "Asia": ["Asia"],
    "Africa": ["Africa"],
    "Oceania": ["Oceania"],
    "North America": ["Caribbean", "Central America", "Northern America"],
    "South America": ["South America"],
    "Not specified": ["Region or country not reported"],
}


def make_by_region(tb_origin: Table) -> Table:
    """Rebuild region aggregates from the source's own aggregate rows, using OWID definitions."""
    tb = tb_origin[tb_origin["country"].isin([r for rows in REGION_ROWS.values() for r in rows])].copy()
    to_region = {row: region for region, rows in REGION_ROWS.items() for row in rows}
    tb["country"] = tb["country"].map(to_region)
    tb = tb.groupby(["country", "year"], as_index=False, observed=True)["foreign_born_population"].sum(min_count=1)

    # OWID counts all of Turkey in Asia, but the source files its "Turkey in Europe" row
    # (reported 1850-1930) inside Europe — move it over.
    turkey_in_europe = (
        tb_origin[tb_origin["country"] == "Turkey in Europe"].set_index("year")["foreign_born_population"].dropna()
    )
    for region, sign in [("Europe", -1), ("Asia", 1)]:
        mask = (tb["country"] == region) & tb["year"].isin(turkey_in_europe.index)
        tb.loc[mask, "foreign_born_population"] += sign * tb.loc[mask, "year"].map(turkey_in_europe)
    tb = tb.dropna(subset=["foreign_born_population"])

    # The regions and "Not specified" must add up exactly to the source's total.
    total = tb_origin[tb_origin["country"] == "Total"].set_index("year")["foreign_born_population"]
    ours = tb.groupby("year")["foreign_born_population"].sum()
    assert (ours - total).abs().max() < 1, "Region sums do not add up to the source's total."

    return tb


# ACS leaf rows that are not countries: residual buckets, region-level leftovers, and parts of
# countries we report at a higher level (the United Kingdom and Portugal aggregate rows).
ACS_NON_COUNTRY = (
    r"Other |n\.e\.c|excluding England and Scotland|^West Indies$|^Middle Africa$|^England$|^Scotland$|^Azores Islands$"
)
# Aggregate rows we report as countries, dropping their sub-rows.
ACS_PARENT_COUNTRIES = ["United Kingdom", "United Kingdom (inc. Crown Dependencies)", "Portugal"]

# ACS region rows, regrouped to OWID definitions like the census ones.
ACS_REGION_PATHS = {
    "Europe": ["Total / Europe"],
    "Asia": ["Total / Asia"],
    "Africa": ["Total / Africa"],
    "Oceania": ["Total / Oceania"],
    "North America": [
        "Total / Americas / Latin America / Caribbean",
        "Total / Americas / Latin America / Central America",
        "Total / Americas / Northern America",
    ],
    "South America": ["Total / Americas / Latin America / South America"],
}


def make_acs_by_country(tb: Table) -> tuple:
    """Select country rows from the ACS table: leaf rows of the hierarchy, except residual
    buckets, plus the United Kingdom and Portugal aggregate rows (whose sub-rows are dropped).
    Also returns the residual leaf paths, which feed the "(not specified)" entities."""
    frames = []
    residual_paths = []
    for _, sub in tb.groupby("year"):
        paths = set(sub["path"])
        parent_paths = [p for p in paths if p.split(" / ")[-1] in ACS_PARENT_COUNTRIES]
        is_leaf = sub["path"].apply(lambda p: not any(q.startswith(p + " / ") for q in paths))
        under_parent = sub["path"].apply(lambda p: any(p.startswith(q + " / ") for q in parent_paths))
        residual = sub["country"].astype(str).str.contains(ACS_NON_COUNTRY, regex=True)
        keep = (is_leaf & ~under_parent & ~residual) | sub["path"].isin(parent_paths)
        frames.append(sub[keep])
        residual_paths.extend(sub.loc[is_leaf & ~under_parent & residual, "path"])
    tb = pr.concat(frames)[["country", "year", "foreign_born_population"]]
    tb = tb.dropna(subset=["foreign_born_population"])
    return tb, residual_paths


# Census rows for people whose specific country is not listed, assigned to their region.
# "Not specified" is kept only for the truly unattributed.
CENSUS_RESIDUALS = {
    "Other Scandinavia": "Europe (not specified)",
    "Other Western Europe": "Europe (not specified)",
    "Other Southern Europe": "Europe (not specified)",
    "Other Eastern Europe": "Europe (not specified)",
    "Europe n.e.c": "Europe (not specified)",
    "Other Asia": "Asia (not specified)",
    "Africa excl. Atlantic Islands": "Africa (not specified)",
    "Atlantic Islands": "Africa (not specified)",
    "Other Oceania": "Oceania (not specified)",
    "Other Caribbean": "North America (not specified)",
    "Other Central America": "North America (not specified)",
    "Other Northern America": "North America (not specified)",
    "South America": "South America (not specified)",
    "Born at sea": "Not specified",
    "Not reported": "Not specified",
}


def acs_residual_entity(path: str) -> str:
    """Region-level entity for an ACS residual row, from its position in the hierarchy."""
    parts = path.split(" / ")
    if parts[1] in ("Europe", "Asia", "Africa", "Oceania"):
        return f"{parts[1]} (not specified)"
    if "South America" in parts:
        return "South America (not specified)"
    return "North America (not specified)"


def add_not_specified(tb_countries: Table, tb_census: Table, tb_acs: Table, acs_residual_paths: list) -> Table:
    """Add entities for people not attributed to a listed country. Where the source gives
    their region, they become e.g. "Asia (not specified)"; only people with no recorded
    origin become "Not specified"."""
    census = tb_census[tb_census["country"].isin(CENSUS_RESIDUALS)].copy()
    census["country"] = census["country"].map(CENSUS_RESIDUALS)
    census = census[["country", "year", "foreign_born_population"]]

    acs = tb_acs[tb_acs["path"].isin(acs_residual_paths)].copy()
    acs["country"] = acs["path"].apply(acs_residual_entity)
    acs = acs[["country", "year", "foreign_born_population"]]

    residuals = pr.concat([census, acs], ignore_index=True)
    residuals = residuals.groupby(["country", "year"], as_index=False, observed=True)["foreign_born_population"].sum(
        min_count=1
    )
    residuals = residuals.dropna(subset=["foreign_born_population"])

    tb = pr.concat([tb_countries, residuals], ignore_index=True)
    tb["foreign_born_population"] = tb["foreign_born_population"].copy_metadata(tb_countries["foreign_born_population"])

    # Everything must now add up to the source's totals.
    totals = pr.concat(
        [
            tb_census[tb_census["country"] == "Total"][["year", "foreign_born_population"]],
            tb_acs[tb_acs["path"] == "Total"][["year", "foreign_born_population"]],
        ],
        ignore_index=True,
    ).set_index("year")["foreign_born_population"]
    ours = tb.groupby("year")["foreign_born_population"].sum()
    assert (ours - totals).abs().max() < 1, "Countries and residuals do not add up to the source's total."
    return tb


def make_acs_by_region(tb: Table) -> Table:
    """Aggregate the ACS table to OWID regions, using the source's own region rows."""
    to_region = {p: region for region, ps in ACS_REGION_PATHS.items() for p in ps}
    out = tb[tb["path"].isin(to_region)].copy()
    out["country"] = out["path"].map(to_region)
    out = out.groupby(["country", "year"], as_index=False, observed=True)["foreign_born_population"].sum(min_count=1)

    total = tb[tb["path"] == "Total"].set_index("year")["foreign_born_population"]
    ours = out.groupby("year")["foreign_born_population"].sum()
    assert (ours - total).abs().max() < 1, "ACS region sums do not add up to the source's total."
    return out


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("us_foreign_born_population")
    tb_census = ds_meadow.read("census")
    tb_by_country = ds_meadow.read("census_by_country_of_birth")
    tb_acs = ds_meadow.read("american_community_survey")
    tb_acs_by_country = ds_meadow.read("acs_by_country_of_birth")

    #
    # Process data.
    #
    # By-country and by-region tables combine the census years (1850-2000) with the ACS years
    # (2005 onward); the two sources never overlap in years.
    tb_by_country = adjust_soviet_asian_part(tb_by_country)
    tb_countries = pr.concat([cover_census_tree(tb_by_country), cover_acs_tree(tb_acs_by_country)], ignore_index=True)
    tb_countries = paths.regions.harmonize_names(tb_countries, countries_file=paths.country_mapping_path)
    tb_regions = pr.concat([make_by_region(tb_by_country), make_acs_by_region(tb_acs_by_country)], ignore_index=True)

    tables = [
        make_annual(tb_census, tb_acs).format(["country", "year"], short_name="us_foreign_born_population"),
        tb_countries.format(["country", "year"], short_name="by_country_of_birth"),
        tb_regions.format(["country", "year"], short_name="by_region_of_birth"),
    ]

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)
    ds_garden.save()
