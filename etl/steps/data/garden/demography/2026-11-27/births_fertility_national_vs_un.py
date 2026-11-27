"""Compare births and fertility rates from national statistics with UN WPP 2024 estimates."""

from owid.catalog import Table, VariableMeta
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Years covered by the compilation.
YEAR_MIN, YEAR_MAX = 2010, 2025
# Last year of the UN WPP 2024 estimates; later years come from its medium projection.
# NOTE: This is specific to WPP 2024; WPP 2026 would have estimates to 2025.
YEAR_LAST_ESTIMATE = 2023
# Year given to the coverage table, so that its map matches the latest year of the data.
YEAR_COVERAGE = YEAR_MAX
STATUSES = [
    "Included",
    "Population under 1 million",
    "Birth registration under 90% or unknown",
    "Left out for another reason",
]


def run() -> None:
    # Load inputs.
    ds_meadow = paths.load_dataset("births_fertility_national_vs_un")
    ds_wpp = paths.load_dataset("un_wpp")
    tb = ds_meadow.read("births_fertility")
    tb_countries = ds_meadow.read("countries")

    # Sanity-check the national data.
    sanity_check_inputs(tb=tb, tb_countries=tb_countries)

    # Keep only the numbers; the sources and notes are in the spreadsheet and in the origin's citation.
    tb = tb[["country", "year", "births", "fertility_rate"]].rename(
        columns={"births": "births_national", "fertility_rate": "fertility_rate_national"}, errors="raise"
    )

    # Add the UN's births and fertility rates for the same countries and years.
    tb_un = get_un_series(ds_wpp=ds_wpp, countries=set(tb["country"]))
    assert set(tb["country"]) <= set(tb_un["country"]), set(tb["country"]) - set(tb_un["country"])
    tb = pr.merge(tb, tb_un, on=["country", "year"], how="left")

    # Add the differences: national minus UN, in absolute terms and as a percentage of the UN figure.
    for measure in ["births", "fertility_rate"]:
        tb[f"{measure}_difference"] = tb[f"{measure}_national"] - tb[f"{measure}_un"]
        tb[f"{measure}_relative_difference"] = 100 * tb[f"{measure}_difference"] / tb[f"{measure}_un"]

    # Create the coverage table: every country or area in UN WPP 2024, with its status.
    tb_coverage = tb_countries[["country", "status"]].copy()
    tb_coverage["year"] = YEAR_COVERAGE
    tb_coverage["status"] = tb_coverage["status"].astype("string")

    # Sanity-check the outputs.
    sanity_check_outputs(tb=tb, tb_coverage=tb_coverage, wpp_countries=set(tb_un["country"]))

    # Improve table formats.
    tb = tb.format(["country", "year"], short_name=paths.short_name)
    tb_coverage = tb_coverage.format(["country", "year"], short_name="coverage")

    # Save outputs.
    ds_garden = paths.create_dataset(tables=[tb, tb_coverage], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def get_un_series(ds_wpp, countries: set) -> Table:
    """UN WPP 2024 births and fertility rates, 2010–2025: estimates to 2023, medium projection after."""
    tables = []
    for table, column in [("births", "births"), ("fertility_rate", "fertility_rate")]:
        tb = ds_wpp.read(table)
        tb = tb[(tb["sex"] == "all") & (tb["age"] == "all") & tb["year"].between(YEAR_MIN, YEAR_MAX)]
        tb = tb[
            ((tb["variant"] == "estimates") & (tb["year"] <= YEAR_LAST_ESTIMATE))
            | ((tb["variant"] == "medium") & (tb["year"] > YEAR_LAST_ESTIMATE))
        ]
        tb = tb[["country", "year", column]].rename(columns={column: f"{column}_un"}, errors="raise")
        # Keep only the UN's origins and licenses: the rest of its metadata is templated on dimensions we drop, and
        # this step's .meta.yml gives the text.
        meta = tb[f"{column}_un"].metadata
        tb[f"{column}_un"].metadata = VariableMeta(origins=meta.origins, licenses=meta.licenses)
        tb["country"] = tb["country"].astype("string")
        tables.append(tb)
    tb_un = pr.merge(tables[0], tables[1], on=["country", "year"], how="outer")

    # Check there is exactly one value per country and year, for every year.
    assert not tb_un.duplicated(subset=["country", "year"]).any()
    n_years = tb_un[tb_un["country"].isin(countries)].groupby("country")["year"].nunique()
    assert (n_years == YEAR_MAX - YEAR_MIN + 1).all(), n_years[n_years != YEAR_MAX - YEAR_MIN + 1]
    return tb_un


def sanity_check_inputs(tb: Table, tb_countries: Table) -> None:
    # One row per included country and year, 2010–2025.
    assert tb["year"].min() == YEAR_MIN and tb["year"].max() == YEAR_MAX, (tb["year"].min(), tb["year"].max())
    assert not tb.duplicated(subset=["country", "year"]).any()
    assert (tb.groupby("country")["year"].nunique() == YEAR_MAX - YEAR_MIN + 1).all()
    # Plausible values: counts of births are positive, and fertility rates are within the range seen worldwide.
    assert (tb["births"].dropna() > 0).all()
    assert tb["fertility_rate"].dropna().between(0.5, 7).all(), tb["fertility_rate"].describe()
    # Every country has births for at least one year from 2020 onward (inclusion rule 4).
    recent = tb[(tb["year"] >= 2020) & tb["births"].notna()]
    assert set(recent["country"]) == set(tb["country"]), set(tb["country"]) - set(recent["country"])
    # The countries tab lists every country or area, and its "Included" ones are exactly those in the data.
    assert set(tb_countries["status"]) == set(STATUSES), set(tb_countries["status"])
    included = set(tb_countries.loc[tb_countries["status"] == "Included", "country"])
    assert included == set(tb["country"]), included ^ set(tb["country"])


def sanity_check_outputs(tb: Table, tb_coverage: Table, wpp_countries: set) -> None:
    # Every national value has a UN value to compare with.
    for measure in ["births", "fertility_rate"]:
        has_national = tb[f"{measure}_national"].notna()
        assert tb.loc[has_national, f"{measure}_un"].notna().all(), measure
        assert tb.loc[has_national, f"{measure}_relative_difference"].notna().all(), measure
    # National and UN figures are within 50% of each other. The largest gaps in the built data are about 45% for
    # births (Bolivia 2025, whose count is registrations of babies under 1) and 38% for fertility rates.
    for measure in ["births", "fertility_rate"]:
        gap = tb[f"{measure}_relative_difference"].dropna().abs()
        assert gap.max() < 50, tb.loc[gap.idxmax(), ["country", "year"]]
    # The coverage table has every country or area in UN WPP 2024, once.
    assert not tb_coverage["country"].duplicated().any()
    assert set(tb_coverage["country"]) <= wpp_countries, set(tb_coverage["country"]) - wpp_countries
    assert len(tb_coverage) == 237, len(tb_coverage)
