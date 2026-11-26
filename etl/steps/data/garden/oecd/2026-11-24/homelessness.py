"""Load a meadow dataset and create a garden dataset.

The source tables report one value per country, each for a different year, and state the years either in a column
(Table HC3.1.A1) or in the notes under each figure. This step assigns those years and combines everything into one
country-year table.
"""

import re

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Year of the data in Figure HC3.1.2 (share of women), from note 4 of the figure: 2026 unless listed below.
# NOTE: The note lists Sweden and the United States (2023), but the figure has no data for them in this release.
DEFAULT_YEAR_WOMEN = 2026
YEARS_WOMEN = {
    2025: ["Canada", "Finland", "Israel", "Japan"],
    2024: [
        "Austria",
        "Belgium (Brussels)(a)",
        "Belgium (Wallonia)(a)",
        "Denmark",
        "Germany",
        "Italy",
        "Korea",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Netherlands",
        "Slovenia",
        "Portugal",
    ],
    2023: ["Chile", "New Zealand", "Sweden", "United States"],
    2022: ["Norway", "South Africa", "Spain"],
    2021: ["Australia", "Estonia", "Iceland", "Slovak Republic", "Colombia"],
    2020: ["Mexico"],
}

# Definition (ETHOS Light categories) and type of count of each country in Figure HC3.1.2, read from the colors and
# cross-hatching of the bars in the PDF of the indicator. Short names keep the labels of the previous release:
# "1_2_3" is now "More than sheltered or unsheltered only (ETHOS Light 1-6)", "2_3" is "Sheltered only (ETHOS Light 2,3)"
# and "1" is "Unsheltered only (ETHOS Light 1)".
# NOTE: The Excel file doesn't include this classification, so it needs to be read from the PDF figure in each update.
CATEGORIES_WOMEN = {
    "point_in_time_1_2_3": [
        "New Zealand",
        "Australia",
        "Germany",
        "Iceland",
        "Slovak Republic",
        "Belgium (Wallonia)(a)",
        "Canada",
        "Italy",
        "Portugal",
        "Netherlands",
        "South Africa",
        "Norway",
        "Estonia",
        "Denmark",
        "Spain",
        "Korea",
        "Finland",
        "Belgium (Brussels)(a)",
        "Luxembourg",
        "Poland",
        "Colombia",
        "Costa Rica",
    ],
    "flow_1_2_3": ["Austria", "Chile", "Israel"],
    "point_in_time_2_3": ["Greece"],
    "flow_2_3": ["Lithuania", "Slovenia", "Latvia"],
    "point_in_time_1": ["Mexico", "Japan"],
}

# Year of each column of Table HC3.1.A2 (number of people experiencing homelessness), from notes 2-4 of the table:
# the year in the column name unless listed below.
YEARS_NUMBER = {
    "around_2018": {
        "default": 2018,
        2021: ["Croatia"],
        2020: ["Canada"],
        2019: ["Czechia", "Poland"],
        2017: ["Denmark", "Latvia", "Sweden", "Cyprus"],
        2016: ["Korea", "Netherlands"],
    },
    "around_2023": {
        "default": 2023,
        2024: ["Belgium (Flanders)(a)", "Canada", "Poland", "Croatia"],
        2022: [
            "Austria",
            "Belgium (Brussels)(a)",
            "Czechia",
            "Denmark",
            "Germany",
            "Greece",
            "Latvia",
            "Lithuania",
            "Luxembourg",
            "Norway",
            "Portugal",
            "Romania",
            "Spain",
            "Türkiye",
        ],
        2021: ["Australia", "Estonia", "Iceland", "Israel", "Italy", "Korea", "Slovak Republic", "Switzerland"],
        2020: ["Mexico"],
    },
    "latest": {
        "default": 2026,
        2025: [
            "Canada",
            "Croatia",
            "Czechia",
            "Finland",
            "Hungary",
            "Ireland",
            "Israel",
            "Japan",
            "Türkiye",
            "United States",
        ],
        2024: [
            "Austria",
            "Belgium (Brussels)(a)",
            "Belgium (Wallonia)(a)",
            "Denmark",
            "Germany",
            "Italy",
            "Korea",
            "Latvia",
            "Lithuania",
            "Luxembourg",
            "Netherlands",
            "Portugal",
            "Slovenia",
            "Cyprus",
            "Romania",
        ],
    },
}

# Year the strategies (indicator HC3.2, last updated 10 September 2026) are assigned to.
STRATEGIES_YEAR = 2026

# Countries whose total rate in Figure HC3.1.1 is not the sum of its components (see sanity_check_inputs).
RATE_TOTAL_EXCEPTIONS = {"Israel"}

# ETHOS Light categories in Table HC3.1.A1.
ETHOS_COLUMNS = [f"ethos_light_{i}" for i in range(1, 7)]

# Countries that report ETHOS Light categories outside their headline estimate, so their breakdown adds up to more.
# Table HC3.1.A1 shades in grey the categories included in the headline: Poland leaves ETHOS Light 6 unshaded, and
# its shaded categories add up exactly to the headline. (Mexico's unshaded categories are removed in corrections.)
# NOTE: Check the shading again at each update.
BREAKDOWN_OUTSIDE_HEADLINE = {"Poland"}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("homelessness")

    # Read tables from meadow dataset.
    tb_point_in_time = ds_meadow.read("point_in_time")
    tb_flow = ds_meadow.read("flow")
    tb_women = ds_meadow.read("share_of_women")
    tb_estimates = ds_meadow.read("estimates")
    tb_number = ds_meadow.read("number")
    tb_strategies = ds_meadow.read("strategies")

    sanity_check_inputs(tb_point_in_time, tb_flow, tb_women, tb_estimates)

    #
    # Process data.
    #
    # Parse the years and numbers of Table HC3.1.A1, which also gives the year of the rates.
    tb_estimates, years_estimates = clean_estimates(tb_estimates)

    # Rates per 10,000 people: assign the year of each country and convert to rates per 100,000 people.
    tb_rates = pr.merge(tb_point_in_time, tb_flow, on="country", how="outer")
    tb_rates = assign_years_from_estimates(tb_rates, years=years_estimates)
    for col in tb_rates.columns:
        if col not in ["country", "year"]:
            tb_rates[col] *= 10

    # Drop rates no country reports (in the 2026 release, flow counts without a breakdown by ETHOS Light category).
    empty = [col for col in tb_rates.columns if tb_rates[col].isna().all()]
    if empty:
        log.warning("Dropping rates with no data", columns=empty)
        tb_rates = tb_rates.drop(columns=empty)

    # Share of women: assign years and categories, convert to percentages, and add the share of men.
    tb_women = prepare_women(tb_women)

    # Number of people experiencing homelessness, from Table HC3.1.A2.
    tb_number = prepare_number(tb_number, tb_estimates=tb_estimates)

    # Share of the population and breakdown by ETHOS Light category, from Table HC3.1.A1.
    tb_estimates = tb_estimates[["country", "year", "share"] + [f"number_{col}" for col in ETHOS_COLUMNS]]

    # Strategies to address homelessness.
    tb_strategy = create_strategy_table(tb_strategies)

    # Harmonize country names. Belgium is reported only by region, and the regions are excluded.
    tables = []
    for tb in [tb_rates, tb_women, tb_number, tb_estimates, tb_strategy]:
        tables.append(
            paths.regions.harmonize_names(tb, warn_on_unused_countries=False, warn_on_unknown_excluded_countries=False)
        )

    # Combine all tables.
    tb = tables[0]
    for tb_other in tables[1:]:
        tb = pr.merge(tb, tb_other, on=["country", "year"], how="outer")

    # Patch known issues in the source (see the .corrections.yml file).
    tb = paths.apply_corrections(tb)

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb_point_in_time: Table, tb_flow: Table, tb_women: Table, tb_estimates: Table) -> None:
    # Every country in Figure HC3.1.2 has a category, and every category refers to a country in the figure.
    countries_women = set(tb_women["country"])
    countries_categories = {c for countries in CATEGORIES_WOMEN.values() for c in countries}
    assert countries_women == countries_categories, (
        f"Countries in Figure HC3.1.2 without a category: {sorted(countries_women - countries_categories)}; "
        f"categories for countries not in the figure: {sorted(countries_categories - countries_women)}"
    )

    # The rates of every country also appear in Table HC3.1.A1, which gives their year.
    countries_rates = set(tb_point_in_time["country"]) | set(tb_flow["country"])
    missing = {normalize_name(c) for c in countries_rates} - set(tb_estimates["country"])
    assert not missing, f"Countries with rates but missing in Table HC3.1.A1: {sorted(missing)}"

    # Total rates are the sum of the components.
    # NOTE: In the 2026 release, Israel's total flow rate (4.2) is its whole headline estimate per 10,000 people, while
    # its components only add up to 1.3 (ETHOS Light 2 and 3). Check whether this is still the case in the next update.
    for tb_rate, prefix in [(tb_point_in_time, "point_in_time"), (tb_flow, "flow")]:
        components = tb_rate[[f"{prefix}_1", f"{prefix}_2_3", f"{prefix}_1_2_3"]].astype(float).sum(axis=1)
        diff = (components - tb_rate[f"{prefix}_total"].astype(float)).abs()
        mismatched = set(tb_rate.loc[diff >= 1e-3, "country"])
        assert mismatched <= RATE_TOTAL_EXCEPTIONS, f"Total {prefix} rates don't add up: {sorted(mismatched)}"

    # Shares of women are fractions.
    assert tb_women["share_of_women"].between(0, 1).all(), "Share of women out of [0, 1]."


def normalize_name(name: str) -> str:
    """Map names of Table HC3.1.1 to the ones of Table HC3.1.A1 (e.g. 'Belgium (Brussels)(a)' -> 'Belgium: Brussels(a)', 'U.K. (England)(a)' -> 'United Kingdom: England(a,b)')."""
    name = name.replace("U.K. (England)(a)", "United Kingdom: England(a,b)")
    return re.sub(r"^Belgium \((\w+)\)", r"Belgium: \1", name)


def clean_estimates(tb: Table) -> tuple[Table, dict[str, int]]:
    """Parse years and numbers of Table HC3.1.A1.

    Returns the table without the United Kingdom (counted in households), and the year of every country, the United
    Kingdom included, since its rates are reported in Figure HC3.1.1.
    """
    tb = tb.copy()

    # Years: keep the last year of ranges like "2017-2021" or "July 2022 – June 2023".
    tb["year"] = tb["year"].astype(str).str.extract(r"(\d{4})\D*$", expand=False)
    assert tb["year"].notna().all(), "Unparsed years in Table HC3.1.A1."
    tb["year"] = tb["year"].astype(int)
    years = dict(zip(tb["country"], tb["year"]))

    # The United Kingdom reports households, not people.
    tb = tb[~tb["country"].str.startswith("United Kingdom")].reset_index(drop=True)

    # Share of the population: a fraction, except for Germany, which is written as "0,63%".
    share = tb["share_of_population"].astype("string").str.strip()
    is_percent = share.str.endswith("%").fillna(False)
    share = share.str.replace("%", "").str.replace(",", ".")
    tb["share"] = pr.to_numeric(share, errors="raise") * 100
    tb.loc[is_percent, "share"] = tb.loc[is_percent, "share"] / 100
    tb["share"] = tb["share"].copy_metadata(tb["share_of_population"])

    # Breakdown by ETHOS Light category: keep the numbers, written as "17 620" or "~5500". Text such as "Not included"
    # or "Included under ETHOS 3" means the category has no separate number.
    for col in ETHOS_COLUMNS:
        values = tb[col].astype("string").str.strip()
        is_text = values.str.contains(r"[A-Za-z]", regex=True).fillna(False)
        numbers = values.str.replace("[\\s\u00a0\u202f~,]", "", regex=True)
        numbers[is_text] = None
        tb[f"number_{col}"] = pr.to_numeric(numbers, errors="raise").copy_metadata(tb[col])

    return tb, years


def assign_years_from_estimates(tb: Table, years: dict[str, int]) -> Table:
    """Assign to each country the year of its headline estimate in Table HC3.1.A1."""
    tb = tb.copy()
    tb["year"] = tb["country"].map(lambda c: years.get(normalize_name(c)))
    assert tb["year"].notna().all(), f"Countries without a year: {tb.loc[tb['year'].isna(), 'country'].tolist()}"
    tb["year"] = tb["year"].astype(int)

    return tb


def assign_years_from_notes(tb: Table, years: dict, default: int) -> Table:
    """Assign years to countries, as listed in the notes of a figure or table."""
    tb = tb.copy()
    listed = [c for countries in years.values() for c in countries]
    duplicated = {c for c in listed if listed.count(c) > 1}
    assert not duplicated, f"Countries listed for more than one year: {duplicated}"

    tb["year"] = default
    for year, countries in years.items():
        tb.loc[tb["country"].isin(countries), "year"] = year

    return tb


def prepare_women(tb: Table) -> Table:
    """Assign years and categories to the share of women, and add the share of men."""
    tb = assign_years_from_notes(tb, years=YEARS_WOMEN, default=DEFAULT_YEAR_WOMEN)
    tb["share_of_women"] *= 100

    # Make one column per category (e.g. share_of_women_point_in_time_1_2_3).
    tb_wide = tb[["country", "year"]].copy()
    for category, countries in CATEGORIES_WOMEN.items():
        tb_wide[f"share_of_women_{category}"] = tb["share_of_women"].where(tb["country"].isin(countries))
        tb_wide[f"share_of_men_{category}"] = 100 - tb_wide[f"share_of_women_{category}"]

    return tb_wide


def prepare_number(tb: Table, tb_estimates: Table) -> Table:
    """Make Table HC3.1.A2 long, with the year of each value."""
    # The United Kingdom reports households, not people.
    tb = tb[~tb["country"].str.startswith("United Kingdom")].reset_index(drop=True)

    tables = []
    for column, years in YEARS_NUMBER.items():
        years = years.copy()
        default = years.pop("default")
        tb_column = tb[["country", column]].rename(columns={column: "number"})
        tb_column["number"] = pr.to_numeric(
            tb_column["number"].astype("string").str.replace("[\\s\u00a0\u202f]", "", regex=True), errors="raise"
        )
        tb_column = tb_column.dropna(subset=["number"])
        tables.append(assign_years_from_notes(tb_column, years=years, default=default))
    tb = pr.concat(tables, ignore_index=True)

    # Where a value is the headline estimate of Table HC3.1.A1, use the year given there, which can differ from the notes
    # of Table HC3.1.A2 (France, Colombia and South Africa in the 2026 release).
    estimates = tb_estimates[["country", "headline_estimate", "year"]].copy()
    estimates["headline_estimate"] = pr.to_numeric(estimates["headline_estimate"], errors="coerce")
    tb = pr.merge(tb, estimates.rename(columns={"year": "year_estimate"}), on="country", how="left")
    same_value = tb["number"] == tb["headline_estimate"]
    changed = tb[same_value & (tb["year"] != tb["year_estimate"])]
    if not changed.empty:
        log.info(
            "Using the year of Table HC3.1.A1 for the number of people experiencing homelessness",
            countries=dict(zip(changed["country"], zip(changed["year"], changed["year_estimate"]))),
        )
    tb.loc[same_value, "year"] = tb.loc[same_value, "year_estimate"]
    tb = tb.drop(columns=["headline_estimate", "year_estimate"])

    assert not tb.duplicated(subset=["country", "year"]).any(), "Two values for the same country and year."

    return tb


def create_strategy_table(tb: Table) -> Table:
    """Classify the strategy of each country to address homelessness."""
    tb = tb.copy()
    tb["year"] = STRATEGIES_YEAR
    tb["type_of_strategy"] = "No national strategy"
    tb.loc[tb["national_strategy"], "type_of_strategy"] = "Other strategy"
    tb.loc[tb["housing_first_national"], "type_of_strategy"] = "Housing First or housing-led approach"
    tb["type_of_strategy"] = tb["type_of_strategy"].copy_metadata(tb["national_strategy"])

    return tb[["country", "year", "type_of_strategy"]]


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate country-years."
    assert tb.columns[tb.isna().all()].empty, f"Fully empty columns: {tb.columns[tb.isna().all()].tolist()}"

    # Rates, numbers and shares are non-negative, and shares are percentages.
    numeric = [c for c in tb.columns if c not in ["country", "year", "type_of_strategy"]]
    assert (tb[numeric].astype(float).fillna(0) >= 0).all().all(), "Negative values."
    shares = [c for c in tb.columns if c.startswith("share")]
    assert (tb[shares].astype(float).fillna(0) <= 100).all().all(), "Shares above 100%."

    # The share of the population experiencing homelessness stays below 3% (New Zealand has the highest, 2.3%).
    assert tb["share"].max() < 3, f"Implausible share of population: {tb['share'].max()}"

    # Coverage didn't shrink much from the previous release (it had 22 countries with point-in-time rates).
    assert tb["point_in_time_total"].notna().sum() >= 22, "Fewer countries with point-in-time rates than before."

    # The ETHOS Light categories add up to at most the headline number of the same year (the headline can include
    # people outside ETHOS Light, so it can be larger). A margin of 2 allows for rounding (Luxembourg, Spain).
    breakdown = tb[[f"number_{col}" for col in ETHOS_COLUMNS]].astype(float).sum(axis=1, min_count=1)
    above = (breakdown > tb["number"] + 2) & ~tb["country"].isin(BREAKDOWN_OUTSIDE_HEADLINE)
    if above.any():
        log.warning(
            "ETHOS Light breakdown adds up to more than the headline estimate",
            countries=tb.loc[above, ["country", "year"]].values.tolist(),
        )
