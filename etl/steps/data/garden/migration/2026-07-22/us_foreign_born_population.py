"""Combine census (1850-2000, decennial) and American Community Survey (2005-, annual) data on
the foreign-born population of the United States.

Outputs six tables:
- us_foreign_born_population: total foreign-born population and its share of the total
  population, 1850-2024.
- annual_change: annual change in the foreign-born population, in people and as a share of the
  total population. Observations before 2005 are 5-10 years apart, so the change is divided
  evenly across the years of each interval.
- annual_change_by_decade: the same, averaged by decade (shown with grapher's decade support).
- by_country_of_birth: foreign-born population by country of birth — at each census from 1850
  to 1930 and from 1960 to 2000, then annually from 2005 from the American Community Survey.
- by_region_of_birth: the same, aggregated to continents using OWID region definitions
  (rebuilt from the sources' rows, regrouping Latin America into North and South America).
- share_by_origin_group: foreign-born population as a share of the total US population, by
  group of origin (Africa, Asia, Oceania, Europe, Mexico, Canada, Rest of America).
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


def sanity_check_changes(tb_stock: Table, tb_stepped: Table, tb_decade: Table) -> None:
    stock = tb_stock.set_index("year")["foreign_born_population"]

    years = tb_stepped["year"]
    assert set(years) == set(range(int(stock.index.min()) + 1, int(stock.index.max()) + 1)), (
        "The stepped series must cover every year from the first observation + 1 to the last."
    )
    # Each interval contributes its full change, so the sum of the annual values must equal the
    # total change over the whole series.
    total = tb_stepped["annual_change"].sum()
    assert abs(total - (stock.iloc[-1] - stock.iloc[0])) < 1, "Annual changes do not add up to the total change."
    assert tb_stepped["annual_change"].abs().max() < 3_000_000, "Annual change outside the plausible range."
    assert tb_stepped["annual_change_share_of_population"].abs().max() < 1.5, "Share outside the plausible range."

    decades = tb_decade.set_index("year")["annual_change"]
    assert set(decades.index) == set(range(1850, 2021, 10)), "Unexpected set of decades."
    # Decades fully covered by two censuses must match the direct calculation between them.
    for decade in range(1850, 2000, 10):
        direct = (stock.loc[decade + 10] - stock.loc[decade]) / 10
        assert abs(decades.loc[decade] - direct) < 1, f"{decade}s value does not match the censuses."


def make_changes(tb_census: Table, tb_acs: Table) -> tuple[Table, Table]:
    """Annual change in the foreign-born population, from consecutive observations.

    Observations before 2005 are 5 or 10 years apart, so the change between two observations is
    divided evenly across the years in between (like the long-run Ireland net migration series).
    Returns two tables:
    - stepped: one value per year, 1851 to the latest year. The value for year Y is the average
      annual change over the interval that ends in or contains Y.
    - by decade: the average of the stepped values over each decade, where the 1850s cover the
      change from 1850 to 1860. The current decade is a partial average of the years so far.
    The share versions divide by the average of the total population reported at the two ends of
    each interval.
    """
    tb = pr.concat([tb_census, tb_acs], ignore_index=True).sort_values("year").reset_index(drop=True)

    rows = []
    for i in range(1, len(tb)):
        start, end = int(tb["year"].iloc[i - 1]), int(tb["year"].iloc[i])
        change = (tb["foreign_born_population"].iloc[i] - tb["foreign_born_population"].iloc[i - 1]) / (end - start)
        population = (tb["total_population"].iloc[i] + tb["total_population"].iloc[i - 1]) / 2
        for year in range(start + 1, end + 1):
            rows.append(
                {
                    "year": year,
                    "annual_change": change,
                    "annual_change_share_of_population": change / population * 100,
                }
            )
    tb_stepped = Table(pd.DataFrame(rows))
    for col in ["annual_change", "annual_change_share_of_population"]:
        tb_stepped[col] = tb_stepped[col].copy_metadata(tb["foreign_born_population"])

    # The change during year Y (from Y-1 to Y) belongs to the decade that starts at Y-1 rounded
    # down: the 1850s cover the changes from 1850 to 1860, i.e. the years 1851 to 1860.
    tb_decade = tb_stepped.copy()
    tb_decade["year"] = (tb_decade["year"] - 1) // 10 * 10
    tb_decade = tb_decade.groupby("year", as_index=False)[["annual_change", "annual_change_share_of_population"]].mean()
    tb_decade = Table(tb_decade)
    for col in ["annual_change", "annual_change_share_of_population"]:
        tb_decade[col] = tb_decade[col].copy_metadata(tb["foreign_born_population"])

    for t in (tb_stepped, tb_decade):
        t["country"] = "United States"

    sanity_check_changes(tb, tb_stepped, tb_decade)
    return tb_stepped, tb_decade


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


# Groups for the share-of-population breakdown: the Americas are split into Mexico, Canada,
# and the rest, since Mexico dominates recent decades.
ORIGIN_GROUP_ROWS_CENSUS = {
    "Europe": ["Europe"],
    "Asia": ["Asia"],
    "Africa": ["Africa"],
    "Oceania": ["Oceania"],
    "Mexico": ["Mexico"],
    "Canada": ["Canada"],
    "Rest of America": ["Caribbean", "Central America", "South America", "Northern America"],
    "Not specified": ["Region or country not reported"],
}
ORIGIN_GROUP_PATHS_ACS = {
    "Europe": ["Total / Europe"],
    "Asia": ["Total / Asia"],
    "Africa": ["Total / Africa"],
    "Oceania": ["Total / Oceania"],
    "Mexico": ["Total / Americas / Latin America / Central America / Mexico"],
    "Canada": ["Total / Americas / Northern America / Canada"],
    "Rest of America": ["Total / Americas"],
}


def make_share_by_origin_group(
    tb_census_by_country: Table, tb_acs_by_country: Table, tb_census: Table, tb_acs: Table
) -> Table:
    """Foreign-born population by group of origin, as a share of the total US population.

    "Rest of America" is the Americas minus Mexico and Canada. Like the by-region table,
    the European part of Turkey counts in Asia and the whole Soviet Union in Europe.
    """
    frames = []
    for source, groups, key in [
        (tb_census_by_country, ORIGIN_GROUP_ROWS_CENSUS, "country"),
        (tb_acs_by_country, ORIGIN_GROUP_PATHS_ACS, "path"),
    ]:
        tb = source[source[key].isin([r for rows in groups.values() for r in rows])].copy()
        to_group = {row: group for group, rows in groups.items() for row in rows}
        tb["country"] = tb[key].map(to_group)
        tb = tb.groupby(["country", "year"], as_index=False, observed=True)["foreign_born_population"].sum(min_count=1)
        tb = tb.dropna(subset=["foreign_born_population"])

        # "Rest of America" so far contains all of the Americas — take Mexico and Canada out.
        for _, row in tb[tb["country"].isin(["Mexico", "Canada"])].iterrows():
            mask = (tb["country"] == "Rest of America") & (tb["year"] == row["year"])
            tb.loc[mask, "foreign_born_population"] -= row["foreign_born_population"]

        total = source[source[key] == "Total"].set_index("year")["foreign_born_population"]
        ours = tb.groupby("year")["foreign_born_population"].sum()
        assert (ours - total).abs().max() < 1, "Origin groups do not add up to the source's total."
        frames.append(tb)

    tb = pr.concat(frames, ignore_index=True)

    # The census files the European part of Turkey (reported 1850-1930) under Europe; we count
    # all of Turkey in Asia, like in the by-region table.
    turkey_in_europe = (
        tb_census_by_country[tb_census_by_country["country"] == "Turkey in Europe"]
        .set_index("year")["foreign_born_population"]
        .dropna()
    )
    for group, sign in [("Europe", -1), ("Asia", 1)]:
        mask = (tb["country"] == group) & tb["year"].isin(turkey_in_europe.index)
        tb.loc[mask, "foreign_born_population"] += sign * tb.loc[mask, "year"].map(turkey_in_europe)

    # Divide by the total population of the same census or survey. Merging the population in and
    # dividing column by column keeps the origins of both the numerator (census and survey
    # by-country tables) and the denominator on the resulting indicator.
    tb_population = pr.concat(
        [tb_census[["year", "total_population"]], tb_acs[["year", "total_population"]]], ignore_index=True
    )
    tb = pr.merge(tb, tb_population, on="year", how="left")
    tb["share_of_population"] = tb["foreign_born_population"] / tb["total_population"] * 100
    tb = tb.drop(columns=["foreign_born_population", "total_population"])

    assert (tb.groupby("year")["share_of_population"].sum() < 17).all(), "Group shares exceed the plausible range."
    return tb


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

    tb_share_by_group = make_share_by_origin_group(tb_by_country, tb_acs_by_country, tb_census, tb_acs)

    tb_change, tb_change_by_decade = make_changes(tb_census, tb_acs)

    tables = [
        make_annual(tb_census, tb_acs).format(["country", "year"], short_name="us_foreign_born_population"),
        tb_change.format(["country", "year"], short_name="annual_change"),
        tb_change_by_decade.format(["country", "year"], short_name="annual_change_by_decade"),
        tb_countries.format(["country", "year"], short_name="by_country_of_birth"),
        tb_regions.format(["country", "year"], short_name="by_region_of_birth"),
        tb_share_by_group.format(["country", "year"], short_name="share_by_origin_group"),
    ]

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)
    ds_garden.save()
