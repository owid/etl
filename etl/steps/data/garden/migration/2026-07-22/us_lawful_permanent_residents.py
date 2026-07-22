"""Load the meadow dataset of the DHS Yearbook of Immigration Statistics and create a garden dataset.

Outputs three tables:
- us_lawful_permanent_residents: annual totals for the United States, 1820-2024, plus a rate
  per 1,000 US population.
- by_country_of_origin: decadal flows by country of last residence, 1820s-2010s.
- by_region_of_origin: decadal flows aggregated to continents using OWID region definitions
  (rebuilt from the country rows, not DHS's own region rows).
"""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# DHS's own aggregate rows in Table 2. We drop them from the country table and rebuild
# regions with OWID definitions, but use them in sanity checks.
DHS_REGIONS = ["Europe", "Asia", "America", "Africa", "Oceania"]
DHS_SUBREGIONS = ["Caribbean", "Central America", "South America"]

# Rows that combine two countries. DHS reports them combined in early decades; where the
# countries are reported separately, we keep the separate rows and drop the combined one.
COMPOSITES = {
    "Austria-Hungary": ["Austria", "Hungary"],
    "Norway-Sweden": ["Norway", "Sweden"],
}

# Continent of each country row, following OWID region definitions (e.g. Turkey in Asia,
# Caribbean and Central America in North America). Composite and dissolved countries are
# assigned to the continent that contains all of their territory. The last two entries are
# rows that cannot be assigned to a single continent; they are kept as their own series for now.
REGIONS = {
    "Europe": [
        "Austria-Hungary",
        "Belgium",
        "Bulgaria",
        "Czechoslovakia",
        "Denmark",
        "Finland",
        "France",
        "Germany",
        "Greece",
        "Ireland",
        "Italy",
        "Netherlands",
        "Norway-Sweden",
        "Poland",
        "Portugal",
        "Romania",
        "Russia",
        "Spain",
        "Switzerland",
        "United Kingdom",
        "Yugoslavia",
        "Other Europe",
    ],
    "Asia": [
        "China",
        "Hong Kong",
        "India",
        "Iran",
        "Israel",
        "Japan",
        "Jordan",
        "Korea",
        "Philippines",
        "Syria",
        "Taiwan",
        "Turkey",
        "Vietnam",
        "Other Asia",
    ],
    "North America": [
        "Canada and Newfoundland",
        "Mexico",
        "Cuba",
        "Dominican Republic",
        "Haiti",
        "Jamaica",
        "Other Caribbean",
        "Belize",
        "Costa Rica",
        "El Salvador",
        "Guatemala",
        "Honduras",
        "Nicaragua",
        "Panama",
        "Other Central America",
    ],
    "South America": [
        "Argentina",
        "Bolivia",
        "Brazil",
        "Chile",
        "Colombia",
        "Ecuador",
        "Guyana",
        "Paraguay",
        "Peru",
        "Suriname",
        "Uruguay",
        "Venezuela",
        "Other South America",
    ],
    "Africa": ["Egypt", "Ethiopia", "Liberia", "Morocco", "South Africa", "Other Africa"],
    "Oceania": ["Australia", "New Zealand", "Other Oceania"],
    "Other America": ["Other America"],
    "Not specified": ["Not Specified"],
}
COUNTRY_TO_REGION = {country: region for region, countries in REGIONS.items() for country in countries}

# Residual rows that feed the region aggregates but are not countries, so they are
# excluded from the by-country table.
RESIDUALS = [c for c in COUNTRY_TO_REGION if c.startswith("Other") or c == "Not Specified"]


def sanity_check_inputs(tb_annual: Table, tb_origin: Table) -> None:
    expected = set(COUNTRY_TO_REGION) | {c for ch in COMPOSITES.values() for c in ch}
    expected |= set(DHS_REGIONS) | set(DHS_SUBREGIONS) | {"Total"}
    assert set(tb_origin["country"]) == expected, (
        f"Country rows in Table 2 changed: unexpected {set(tb_origin['country']) - expected}, "
        f"missing {expected - set(tb_origin['country'])}"
    )
    assert not tb_origin.duplicated(subset=["country", "decade"]).any(), "Duplicate (country, decade) rows."
    assert (tb_origin["immigrants"].dropna() >= 0).all(), "Negative values in Table 2."
    assert tb_annual["immigrants"].notna().all(), "Missing values in annual totals."

    # The sum of the annual totals over each decade should be close to the decade totals of
    # Table 2. They come from different yearbook editions (2024, rounded to the nearest 10,
    # vs 2020, unrounded), so allow a small tolerance.
    totals = tb_origin[tb_origin["country"] == "Total"].set_index("decade")["immigrants"]
    for decade, total in totals.items():
        annual_sum = tb_annual[(tb_annual["year"] >= decade) & (tb_annual["year"] < decade + 10)]["immigrants"].sum()
        assert abs(annual_sum - total) / total < 0.01, (
            f"Decade {decade}: annual totals sum to {annual_sum:,.0f} but Table 2 total is {total:,.0f}."
        )


def make_by_country(tb_origin: Table) -> Table:
    """Keep country rows only, using separate countries where reported and combined rows otherwise."""
    tb = tb_origin[~tb_origin["country"].isin(DHS_REGIONS + DHS_SUBREGIONS + RESIDUALS + ["Total"])].copy()

    # Where a combined row overlaps with its separately-reported countries, keep the countries.
    for parent, children in COMPOSITES.items():
        child_decades = set(tb.loc[tb["country"].isin(children) & tb["immigrants"].notna(), "decade"])
        tb = tb[~((tb["country"] == parent) & tb["decade"].isin(child_decades))]

    # Drop Canada and Mexico before the 1910s: people arriving by land were not fully counted
    # until 1908, so earlier figures are large undercounts. They are kept in the region table,
    # whose totals match what the source published.
    tb = tb[~(tb["country"].isin(["Canada and Newfoundland", "Mexico"]) & (tb["decade"] < 1910))]

    tb = tb.dropna(subset=["immigrants"])
    tb = paths.regions.harmonize_names(tb, countries_file=paths.country_mapping_path)
    tb = tb.rename(columns={"decade": "year"})
    return tb


def make_by_region(tb_origin: Table) -> Table:
    """Rebuild region aggregates from the country rows, using OWID region definitions."""
    # Use top-level rows only: countries, combined rows (full series), and residuals. The
    # separately-reported halves of combined rows are excluded to avoid double counting.
    children = [c for ch in COMPOSITES.values() for c in ch]
    tb = tb_origin[tb_origin["country"].isin(COUNTRY_TO_REGION) & ~tb_origin["country"].isin(children)].copy()

    tb["country"] = tb["country"].map(COUNTRY_TO_REGION)
    tb = tb.groupby(["country", "decade"], as_index=False, observed=True)["immigrants"].sum(min_count=1)
    tb = tb.dropna(subset=["immigrants"])

    # Our region sums must reconcile exactly with DHS's own aggregate rows.
    dhs = tb_origin.set_index(["country", "decade"])["immigrants"]
    ours = tb.set_index(["country", "decade"])["immigrants"]
    for region, dhs_regions in [
        (["Europe"], ["Europe"]),
        (["Asia"], ["Asia"]),
        (["Africa"], ["Africa"]),
        (["Oceania"], ["Oceania"]),
        (["North America", "South America", "Other America"], ["America"]),
    ]:
        ours_sum = sum(ours.get(r, 0) for r in [(r, d) for r in region for d in range(1820, 2020, 10)])
        dhs_sum = sum(dhs.get((r, d), 0) or 0 for r in dhs_regions for d in range(1820, 2020, 10))
        assert abs(ours_sum - dhs_sum) < 1, f"Region {region} does not match DHS's {dhs_regions}."

    total = dhs.loc["Total"].sum()
    assert abs(tb["immigrants"].sum() - total) < 1, "Region sums (incl. placeholders) do not add up to the total."

    tb = tb.rename(columns={"decade": "year"})
    return tb


def make_annual(tb_annual: Table) -> Table:
    tb = tb_annual.copy()
    tb["country"] = "United States"

    tb = paths.regions.add_population(tb, interpolate_missing_population=True)
    tb["immigrants_per_1000"] = tb["immigrants"] / tb["population"] * 1000
    tb = tb.drop(columns=["population"])

    assert tb["immigrants_per_1000"].max() < 20, "Implausibly high immigration rate."
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("us_lawful_permanent_residents")
    tb_annual = ds_meadow.read("annual_totals")
    tb_origin = ds_meadow.read("by_country_of_origin")

    sanity_check_inputs(tb_annual, tb_origin)

    #
    # Process data.
    #
    tables = [
        make_annual(tb_annual).format(["country", "year"], short_name="us_lawful_permanent_residents"),
        make_by_country(tb_origin).format(["country", "year"], short_name="by_country_of_origin"),
        make_by_region(tb_origin).format(["country", "year"], short_name="by_region_of_origin"),
    ]

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)
    ds_garden.save()
