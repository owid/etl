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

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Source rows of Table 4 that we publish as countries, as named in the source. Region rows,
# intermediate groupings (British Isles, Scandinavia, Low countries), residual rows
# ("Other ...", "... n.e.c.") and partial sub-rows (Azores, Canada-French, the Soviet Union's
# "In Europe" part) are used only for the region aggregates or skipped.
COUNTRIES = [
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
    "Portugal (total)",
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


def make_by_country(tb_origin: Table) -> Table:
    """Keep country rows, joining the source's split series for the United Kingdom and Turkey."""
    piv = tb_origin.pivot(index="country", columns="year", values="foreign_born_population")

    tb = tb_origin[tb_origin["country"].isin(COUNTRIES)].drop(columns=["depth"])

    # The United Kingdom is only reported as such from 1930. Before that, Northern Ireland is
    # included in the Ireland row, so Great Britain covers the same area as today's United
    # Kingdom minus Northern Ireland — we use it for the earlier years.
    uk = piv.loc["United Kingdom"].fillna(piv.loc["Great Britain"]).rename("foreign_born_population")
    uk = uk.reset_index().assign(country="United Kingdom")

    # Turkey is reported as "Turkey in Europe" (1850-1930) and "Turkey in Asia" (1910-2000);
    # we sum the two into one series.
    turkey = piv.loc[["Turkey in Europe", "Turkey in Asia"]].sum(min_count=1).rename("foreign_born_population")
    turkey = turkey.reset_index().assign(country="Turkey")

    tb = pr.concat([tb, Table(uk), Table(turkey)], ignore_index=True)
    tb = tb.dropna(subset=["foreign_born_population"])
    tb["foreign_born_population"] = tb["foreign_born_population"].copy_metadata(tb_origin["foreign_born_population"])
    return tb


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


def make_acs_by_country(tb: Table) -> Table:
    """Select country rows from the ACS table: leaf rows of the hierarchy, except residual
    buckets, plus the United Kingdom and Portugal aggregate rows (whose sub-rows are dropped)."""
    frames = []
    for _, sub in tb.groupby("year"):
        paths = set(sub["path"])
        parent_paths = [p for p in paths if p.split(" / ")[-1] in ACS_PARENT_COUNTRIES]
        is_leaf = sub["path"].apply(lambda p: not any(q.startswith(p + " / ") for q in paths))
        under_parent = sub["path"].apply(lambda p: any(p.startswith(q + " / ") for q in parent_paths))
        keep = (is_leaf & ~under_parent & ~sub["country"].astype(str).str.contains(ACS_NON_COUNTRY, regex=True)) | sub[
            "path"
        ].isin(parent_paths)
        frames.append(sub[keep])
    tb = pr.concat(frames)[["country", "year", "foreign_born_population"]]
    tb = tb.dropna(subset=["foreign_born_population"])
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
    tb_countries = pr.concat(
        [make_by_country(tb_by_country), make_acs_by_country(tb_acs_by_country)], ignore_index=True
    )
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
