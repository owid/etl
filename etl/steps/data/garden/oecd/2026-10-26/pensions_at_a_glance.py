"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# For each source measure, the garden table it goes to and the indicator name it gets there.
# Keys are (MEASURE, UNIT_MEASURE) codes of the OECD dataflow; each comment gives the OECD's labels for both.
# Measures that only differ by earnings level (50%, 100% and 200% of the average wage) become one indicator with an
# "earnings" dimension.
MEASURES = {
    # "Current retirement age for a person who entered the labour force at age 22" (Years).
    ("CRPLF22", "Y"): ("retirement", "current_normal_retirement_age"),
    # "Future retirement age for a person who entered the labour force at age 22" (Years).
    ("FRPLF22", "Y"): ("retirement", "future_normal_retirement_age"),
    # "Effective labour market exit age" (Years).
    ("ELMEA", "Y"): ("retirement", "effective_labor_market_exit_age"),
    # "Expected years after labour market exit" (Years).
    ("EYLME", "Y"): ("retirement", "expected_years_after_labor_market_exit"),
    # "Gross pension replacement rate at 50% / 100% / 200% of average wage"
    # (Percentage of gross pre-retirement earnings at the same income level).
    ("GPRR50", "PT_EARN_G_PRE_RET_INC"): ("replacement_rates", "gross_replacement_rate"),
    ("GPRR100", "PT_EARN_G_PRE_RET_INC"): ("replacement_rates", "gross_replacement_rate"),
    ("GPRR200", "PT_EARN_G_PRE_RET_INC"): ("replacement_rates", "gross_replacement_rate"),
    # "Net pension replacement rate at 50% / 100% / 200% of average wage"
    # (Percentage of net pre-retirement earnings at the same income level).
    ("NPRR50", "PT_EARN_N_PRE_RET_INC"): ("replacement_rates", "net_replacement_rate"),
    ("NPRR100", "PT_EARN_N_PRE_RET_INC"): ("replacement_rates", "net_replacement_rate"),
    ("NPRR200", "PT_EARN_N_PRE_RET_INC"): ("replacement_rates", "net_replacement_rate"),
    # "Gross pension wealth at 50% / 100% / 200% of average wage"
    # (Factor of gross individual wage at the same income level).
    ("GPW50", "FCTR_WG_SAL_G_I_INC"): ("pension_wealth", "gross_pension_wealth"),
    ("GPW100", "FCTR_WG_SAL_G_I_INC"): ("pension_wealth", "gross_pension_wealth"),
    ("GPW200", "FCTR_WG_SAL_G_I_INC"): ("pension_wealth", "gross_pension_wealth"),
    # "Net pension wealth at 50% / 100% / 200% of average wage"
    # (Factor of net individual wage at the same income level).
    ("NPW50", "FCTR_WG_SAL_N_I_INC"): ("pension_wealth", "net_pension_wealth"),
    ("NPW100", "FCTR_WG_SAL_N_I_INC"): ("pension_wealth", "net_pension_wealth"),
    ("NPW200", "FCTR_WG_SAL_N_I_INC"): ("pension_wealth", "net_pension_wealth"),
    # "Average tax rate of workers" (Percentage of average earnings).
    ("ATRW", "PT_EARN_A"): ("taxes_and_wages", "average_tax_rate_workers"),
    # "Average tax rate of average earner pensioners" (Percentage of pension).
    ("ATRAEP", "PT_PENS"): ("taxes_and_wages", "average_tax_rate_pensioners_average_earner"),
    # "Average tax rate of pensioners with pension equal to average earnings" (Percentage of average earnings).
    ("ATRPPAE", "PT_EARN_A"): ("taxes_and_wages", "average_tax_rate_pensioners_income_equal_to_average_wage"),
    # "Average worker gross wage" (US dollars, exchange rate converted).
    ("AWGW", "USD_EXC"): ("taxes_and_wages", "average_wage_usd"),
    # "Employment rate" (Percentage of population in the same age).
    ("ER", "PT_POP_AGE"): ("employment_rates", "employment_rate"),
    # "Equivalised disposable income of older people" (Percentage of average disposable income).
    ("EDIOP", "PT_B6N_A"): ("incomes_of_older_people", "relative_disposable_income"),
    # "Old age income poverty rate below 50% of median income" (Percentage of population in the same sex and age).
    ("OAIP", "PT_POP_SEX_AGE"): ("incomes_of_older_people", "poverty_rate"),
    # "Public transfers to older people", "Occupational transfers to older people", "Capital income of older people"
    # and "Employment income of older people"
    # (Percentage of gross disposable income of the population aged over 65 years).
    ("PTOP", "PT_B6G_A_POP_Y_GT65"): ("income_sources_of_older_people", "share_of_income_from_public_transfers"),
    ("OCOP", "PT_B6G_A_POP_Y_GT65"): ("income_sources_of_older_people", "share_of_income_from_occupational_transfers"),
    ("CIOP", "PT_B6G_A_POP_Y_GT65"): ("income_sources_of_older_people", "share_of_income_from_capital"),
    ("EIOP", "PT_B6G_A_POP_Y_GT65"): ("income_sources_of_older_people", "share_of_income_from_work"),
    # "Public expenditure on pensions" (Percentage of GDP).
    ("PEP", "PT_B1GQ"): ("pension_expenditure", "public_pension_expenditure"),
    # "Projections of public expenditure on pensions" (Percentage of GDP).
    ("PPEP", "PT_B1GQ"): ("pension_expenditure", "public_pension_expenditure_projected"),
}

# Measures we don't ingest:
# - "Fertility rate", "Life expectancy" and "Old-age to working-age ratio": the OECD takes them from the UN World
#   Population Prospects 2024, which we import directly.
# - "Average worker gross wage" (National currency): amounts in national currency can't be compared between
#   countries, so we keep only the US dollar version.
EXCLUDED_MEASURES = {("FR", "CH_W"), ("LE", "Y"), ("OAWAR", "PT_POP_Y20T64"), ("AWGW", "XDC")}

# Dimensions each garden table keeps (besides country and year).
TABLE_DIMENSIONS = {
    "retirement": ["sex"],
    "replacement_rates": ["sex", "earnings", "pension_schemes"],
    "pension_wealth": ["sex", "earnings"],
    "taxes_and_wages": [],
    "employment_rates": ["age"],
    "incomes_of_older_people": ["sex", "age"],
    "income_sources_of_older_people": [],
    "pension_expenditure": [],
}

# Labels for the dimension codes. "_Z" (not applicable) and "_T" (total) both mean "both sexes" or "all ages" here.
SEX = {"F": "Women", "M": "Men", "_T": "Both sexes", "_Z": "Both sexes"}
AGE = {
    "_Z": "",
    "BIRTH": "At birth",
    "Y65": "At 65",
    "Y55T59": "55–59 years",
    "Y60T64": "60–64 years",
    "Y65T69": "65–69 years",
    "Y66T75": "66–75 years",
    "Y_GE76": "76 years and older",
    "Y_GE66": "66 years and older",
    "Y_GE65": "65 years and older",
    "_T": "Total population",
}
OPTIONALITY = {"_Z": "", "M": "Mandatory", "MV": "Mandatory and voluntary"}
EARNINGS = {"50": "50% of average wage", "100": "Average wage", "200": "200% of average wage"}

# Measures whose sex dimension carries no information: the tax rates are tagged "Male" in the source, but the report
# describes them without reference to sex.
MEASURES_WITHOUT_SEX = ["ATRW", "ATRAEP", "ATRPPAE"]

# Values the source labels with a common year, but that refer to another year for some countries, as stated in the
# notes to the tables and figures of Pensions at a Glance 2025: (measure, year in the source) -> {country: actual year}.
# NOTE: Check the notes to Table 8.2 (public expenditure on pensions) and Figures 6.7 and 6.8 (employment rates) of the
#  new report, and update (or remove) these mappings.
ACTUAL_YEARS = {
    # Public expenditure on pensions: the "latest" column, labeled 2021.
    ("PEP", 2021): {
        "Australia": 2022,
        "Canada": 2022,
        "France": 2022,
        "Japan": 2022,
        "South Korea": 2022,
        "New Zealand": 2022,
        "United Kingdom": 2022,
        "Chile": 2023,
        "Colombia": 2023,
        "Costa Rica": 2023,
        "Israel": 2023,
        "Mexico": 2023,
        "United States": 2023,
    },
    # Employment rates by age.
    ("ER", 2024): {
        "Argentina": 2023,
        "Indonesia": 2019,
    },
}

# Measures from household income surveys. For years without a new survey, the source repeats the previous values.
INCOME_SURVEY_MEASURES = [
    # "Equivalised disposable income of older people".
    "EDIOP",
    # "Old age income poverty rate below 50% of median income".
    "OAIP",
    # "Public transfers to older people".
    "PTOP",
    # "Occupational transfers to older people".
    "OCOP",
    # "Capital income of older people".
    "CIOP",
    # "Employment income of older people".
    "EIOP",
]
# A country-year counts as a copy of the previous survey year when at least this share of its income series (among
# those with a value in both years) are identical. Not 100%, because a copied year can have a few series revised:
# Australia 2022 matches 2020 in 10 of 13 series. Years with a real new survey match in at most 1 of 13.
REPEATED_SURVEY_MIN_SHARE_EQUAL = 0.5


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("pensions_at_a_glance")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("pensions_at_a_glance")
    tb_taxes = ds_meadow.read("tax_rates")
    tb_age_80 = ds_meadow.read("replacement_rates_at_age_80")

    # Check that the source has the expected measures and dimensions.
    sanity_check_inputs(tb=tb)

    #
    # Process data.
    #
    # Drop the measures we don't ingest (matched on measure and unit, since a measure can come in several units).
    keep = [(m, u) not in EXCLUDED_MEASURES for m, u in zip(tb["measure"].astype(str), tb["unit_measure"].astype(str))]
    tb = tb[keep].reset_index(drop=True)

    # Harmonize country names.
    # The .countries.json maps two source labels to each of these entities:
    # - "OECD average country" (dataflow) and "OECD" (Figure 4.3 data) are the same OECD average, so both become
    #   "OECD countries", the name our other OECD datasets use.
    # - The dataflow labels some years of women's exit age "European Union (28 countries)" and the rest
    #   "European Union (27 countries)", with no break in the values (men's series is EU27 throughout). So both become
    #   "European Union (27)".
    # - The Figure 4.2 data labels countries by ISO code, which the .countries.json also maps.
    # NOTE: Check whether the source still mixes the two EU labels; if it labels the series consistently, keep only one.
    tb = paths.regions.harmonize_names(tb)
    tb_taxes = paths.regions.harmonize_names(tb_taxes)
    tb_age_80 = paths.regions.harmonize_names(tb_age_80)

    # Drop known errors in the source data (missing values published as zeros).
    tb = paths.apply_corrections(tb)

    # Drop series that the source publishes wrongly.
    tb = drop_wrong_series(tb=tb)

    # Drop survey years where the source repeats the previous survey's values.
    tb = drop_repeated_survey_years(tb=tb)

    # Assign values that the source labels with a common year to the year they actually refer to.
    tb = assign_actual_years(tb=tb)

    # Replace dimension codes with labels and split earnings levels out of the measure codes.
    tb = add_dimension_labels(tb=tb)

    # Create one wide table per group of indicators that share the same dimensions.
    tables = create_tables(tb=tb)

    # Take the tax rate of workers from the data of Figure 4.3, since the source publishes it wrongly.
    tables["taxes_and_wages"] = add_tax_rate_of_workers(tb=tables["taxes_and_wages"], tb_taxes=tb_taxes)

    # Add the gross replacement rate at age 80 from the data of Figure 4.2, which the source doesn't publish.
    tables["replacement_rates"] = add_replacement_rate_at_age_80(
        tb=tables["replacement_rates"], tb_age_80=tb_age_80, tb_retirement=tables["retirement"]
    )

    # Improve table formats.
    tables = {
        name: tb_.format(["country", "year"] + TABLE_DIMENSIONS[name], short_name=name) for name, tb_ in tables.items()
    }

    sanity_check_outputs(tables=tables)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=list(tables.values()), default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    measures = set(zip(tb["measure"].astype(str), tb["unit_measure"].astype(str)))
    expected = set(MEASURES) | EXCLUDED_MEASURES
    assert measures == expected, (
        f"Unexpected measures or units. New: {measures - expected}. Missing: {expected - measures}."
    )
    for column, labels in {"sex": SEX, "age": AGE, "optionality": OPTIONALITY}.items():
        unknown = set(tb[column].astype(str)) - set(labels)
        assert not unknown, f"Unknown codes in {column}: {unknown}"
    assert set(tb[tb["measure"].isin(MEASURES_WITHOUT_SEX)]["sex"].astype(str)) == {"M"}, (
        "Tax rates now come by sex: check whether they still need to drop the sex dimension."
    )
    assert not tb.duplicated(
        subset=["country", "year", "measure", "unit_measure", "sex", "age", "optionality"]
    ).any(), "Duplicate rows in the source."
    assert (tb["value"] >= 0).all(), "Negative values in the source."


def drop_repeated_survey_years(tb: Table) -> Table:
    """Drop the income survey values of a country and year when they (mostly) equal those of the previous survey year.

    The source fills years without a new survey by repeating the latest values (e.g. Iceland in 2018, 2020 and 2022).
    """
    mask_survey = tb["measure"].isin(INCOME_SURVEY_MEASURES)
    tb_survey = tb[mask_survey]
    tb_wide = tb_survey.pivot(
        index=["country", "year"], columns=["measure", "sex", "age"], values="value", join_column_levels_with="_"
    )
    columns = [column for column in tb_wide.columns if column not in ["country", "year"]]
    tb_wide = tb_wide.sort_values(["country", "year"]).reset_index(drop=True)
    tb_previous = tb_wide.groupby("country", observed=True)[columns].shift(1)
    both = tb_wide[columns].notna() & tb_previous.notna()
    share_equal = ((tb_wide[columns] == tb_previous) & both).sum(axis=1) / both.sum(axis=1)
    repeated = both.any(axis=1) & (share_equal >= REPEATED_SURVEY_MIN_SHARE_EQUAL)
    tb_repeated = tb_wide[repeated][["country", "year"]]
    log.info(
        "Dropping repeated income survey years",
        country_years=[f"{country} {year}" for country, year in tb_repeated.itertuples(index=False)],
    )
    # NOTE: There were 12 such country-years in the 2025 edition; check that any new ones are also carried-forward values.
    assert len(tb_repeated) <= 15, f"More repeated income survey years than expected ({len(tb_repeated)})."

    tb_repeated["repeated"] = True
    tb = pr.merge(tb, tb_repeated, on=["country", "year"], how="left")
    tb = tb[~(tb["measure"].isin(INCOME_SURVEY_MEASURES) & tb["repeated"].fillna(False).astype(bool))]
    tb = tb.drop(columns=["repeated"]).reset_index(drop=True)

    return tb


def drop_wrong_series(tb: Table) -> Table:
    """Drop series that the source publishes wrongly, checking first that they are still wrong."""
    # The gross replacement rates for mandatory and voluntary schemes are copies of those for mandatory schemes only,
    # while Table 4.5 of the report shows higher rates (e.g. 74.8% instead of 39.7% in the US at the average wage).
    # NOTE: If this assertion fails, the OECD may have fixed the series: compare it with Table 4.5 and consider keeping it.
    tb_gross = tb[tb["measure"].isin(["GPRR50", "GPRR100", "GPRR200"])]
    tb_gross_wide = tb_gross.pivot(index=["country", "year", "measure", "sex"], columns="optionality", values="value")
    tb_gross_wide = tb_gross_wide.dropna(subset=["MV"])
    assert (tb_gross_wide["MV"] == tb_gross_wide["M"]).all(), "Gross replacement rates with voluntary schemes changed."
    tb = tb[~(tb["measure"].isin(["GPRR50", "GPRR100", "GPRR200"]) & (tb["optionality"] == "MV"))]

    return tb.reset_index(drop=True)


def add_tax_rate_of_workers(tb: Table, tb_taxes: Table) -> Table:
    """Replace the tax rate of workers, which the source publishes wrongly, with the one in the data of Figure 4.3.

    This works around an error in the OECD's data. Figure 4.3 of the report shows the tax rates of workers and
    pensioners as stacked bars, and the source's "average tax rate of workers" (ATRW) holds the length of the workers'
    segment, i.e. the workers' rate minus the pensioners' rate (or 0 where pensioners pay more), not the rate itself.
    In Germany, for example, workers pay 38.0% and pensioners 21.6%, and the source gives 16.4%. The report's text gives
    27% for the OECD average, while the source gives 16.9%.

    So we take the workers' rate from the figure's data instead. The function first checks that the figure and the
    source agree on the pensioners' rates, and that the source's ATRW is still the difference, so that it fails once
    the OECD fixes the series.
    """
    tb_taxes = tb_taxes.copy()
    for column in tb_taxes.columns:
        if column.startswith("tax_rate"):
            tb_taxes[column] *= 100
    tb_check = pr.merge(tb, tb_taxes, on=["country", "year"], how="outer")
    assert len(tb_check) == len(tb), "Countries in the Figure 4.3 data are missing from the source."

    # Both sources must agree on the tax rates of pensioners.
    for column_source, column_figure in {
        "average_tax_rate_pensioners_average_earner": "tax_rate_pensioners_average_earner",
        "average_tax_rate_pensioners_income_equal_to_average_wage": "tax_rate_pensioners_income_equal_to_average_wage",
    }.items():
        difference = (tb_check[column_source] - tb_check[column_figure]).abs()
        assert (difference.dropna() < 0.1).all(), f"The source and Figure 4.3 disagree on {column_source}."

    # The source's rate of workers must still be the difference shown in the figure (or 0, where pensioners pay more).
    stacked = (tb_check["tax_rate_workers"] - tb_check["tax_rate_pensioners_average_earner"]).clip(lower=0)
    difference = (tb_check["average_tax_rate_workers"] - stacked).abs()
    # NOTE: If this assertion fails, the OECD may have fixed the series: consider using it instead of Figure 4.3.
    assert (difference.dropna() < 0.1).all(), (
        "The source's tax rate of workers is no longer the difference with pensioners."
    )

    tb = tb.drop(columns=["average_tax_rate_workers"])
    tb = pr.merge(
        tb,
        tb_taxes[["country", "year", "tax_rate_workers"]].rename(
            columns={"tax_rate_workers": "average_tax_rate_workers"}
        ),
        on=["country", "year"],
        how="left",
    )

    return tb


def add_replacement_rate_at_age_80(tb: Table, tb_age_80: Table, tb_retirement: Table) -> Table:
    """Add the gross replacement rate at age 80 of Figure 4.2, for men earning the average wage in mandatory schemes.

    The figure doesn't say which worker it models. Its rates at retirement match those of men earning the average wage
    in mandatory schemes (e.g. Australia 40.8%, against 38.5% for women), and its retirement ages match men's future
    normal retirement ages, which the function checks before adding the rates.
    """
    mask = (tb["sex"] == "Men") & (tb["earnings"] == "Average wage") & (tb["pension_schemes"] == "Mandatory")
    tb_check = pr.merge(tb_age_80, tb[mask], on=["country", "year"], how="left")
    assert tb_check["gross_replacement_rate"].notna().all(), (
        "Countries in the Figure 4.2 data are missing from the source."
    )
    difference = (tb_check["gross_replacement_rate_at_retirement"] - tb_check["gross_replacement_rate"]).abs()
    assert (difference < 0.1).all(), (
        "The rates at retirement of Figure 4.2 differ from those of men at the average wage."
    )

    tb_ages = tb_retirement[tb_retirement["sex"] == "Men"][["country", "future_normal_retirement_age"]].dropna()
    tb_check = pr.merge(tb_age_80, tb_ages, on="country", how="inner")
    difference = (tb_check["normal_retirement_age"] - tb_check["future_normal_retirement_age"]).abs()
    assert (difference.dropna() < 1).all(), "The retirement ages of Figure 4.2 differ from men's future ones."

    tb_age_80 = tb_age_80[["country", "year", "gross_replacement_rate_at_age_80"]].copy()
    tb_age_80["sex"] = "Men"
    tb_age_80["earnings"] = "Average wage"
    tb_age_80["pension_schemes"] = "Mandatory"
    tb = pr.merge(tb, tb_age_80, on=["country", "year", "sex", "earnings", "pension_schemes"], how="left")

    return tb


def assign_actual_years(tb: Table) -> Table:
    for (measure, year_in_source), actual_years in ACTUAL_YEARS.items():
        mask = (tb["measure"] == measure) & (tb["year"] == year_in_source)
        missing = set(actual_years) - set(tb[mask]["country"])
        assert not missing, f"Countries missing from {measure} in {year_in_source}: {missing}"
        for country, year in actual_years.items():
            # The actual year must not already be in the data, or we would create duplicates.
            assert not ((tb["measure"] == measure) & (tb["country"] == country) & (tb["year"] == year)).any()
            tb.loc[mask & (tb["country"] == country), "year"] = year

    return tb


def add_dimension_labels(tb: Table) -> Table:
    tb = tb.astype({"measure": str, "unit_measure": str, "sex": str, "age": str, "optionality": str})

    # Map each measure (and unit) to its garden table and indicator.
    keys = list(zip(tb["measure"], tb["unit_measure"]))
    tb["table"] = [MEASURES[key][0] for key in keys]
    tb["indicator"] = [MEASURES[key][1] for key in keys]

    # Earnings levels are encoded at the end of the measure code (e.g. GPRR50).
    tb["earnings"] = tb["measure"].str.extract(r"^(?:GPRR|NPRR|GPW|NPW)(\d+)$", expand=False).map(EARNINGS)
    tb["earnings"] = tb["earnings"].copy_metadata(tb["measure"])

    tb.loc[tb["measure"].isin(MEASURES_WITHOUT_SEX), "sex"] = "_Z"
    tb["sex"] = tb["sex"].map(SEX)
    tb["age"] = tb["age"].map(AGE)
    tb["pension_schemes"] = tb["optionality"].map(OPTIONALITY)

    return tb


def create_tables(tb: Table) -> dict[str, Table]:
    tables = {}
    for name, dimensions in TABLE_DIMENSIONS.items():
        tb_table = tb[tb["table"] == name]
        # Dimensions dropped from this table must carry no information within each indicator.
        for dimension in sorted({"sex", "age", "pension_schemes"} - set(dimensions)):
            n_values = tb_table.groupby("indicator")[dimension].nunique()
            assert (n_values == 1).all(), f"Table {name} varies by {dimension}, which it drops."
        tables[name] = tb_table.pivot(
            index=["country", "year"] + dimensions, columns="indicator", values="value", join_column_levels_with="_"
        )
    return tables


def sanity_check_outputs(tables: dict[str, Table]) -> None:
    for name, tb in tables.items():
        assert tb.columns[tb.isna().all()].empty, f"Table {name} has a fully empty column."

    # Shares of income sources should add up to about 100%.
    tb_sources = tables["income_sources_of_older_people"]
    total = tb_sources.sum(axis=1)
    assert ((total > 95) & (total < 101)).all(), (
        f"Income shares don't add up to 100%:\n{total[(total <= 95) | (total >= 101)]}"
    )

    # Value bounds that a unit mistake or a parsing error would break.
    bounds = {
        ("retirement", "current_normal_retirement_age"): (40, 75),
        ("retirement", "future_normal_retirement_age"): (50, 80),
        ("retirement", "effective_labor_market_exit_age"): (45, 85),
        ("retirement", "expected_years_after_labor_market_exit"): (3, 35),
        ("replacement_rates", "gross_replacement_rate"): (0, 150),
        ("replacement_rates", "net_replacement_rate"): (0, 150),
        ("replacement_rates", "gross_replacement_rate_at_age_80"): (0, 150),
        ("pension_wealth", "gross_pension_wealth"): (0, 40),
        ("pension_wealth", "net_pension_wealth"): (0, 40),
        ("taxes_and_wages", "average_tax_rate_workers"): (0, 60),
        ("employment_rates", "employment_rate"): (0, 100),
        ("incomes_of_older_people", "poverty_rate"): (0, 100),
        ("incomes_of_older_people", "relative_disposable_income"): (20, 200),
        ("pension_expenditure", "public_pension_expenditure"): (0, 30),
        ("pension_expenditure", "public_pension_expenditure_projected"): (0, 30),
    }
    for (name, column), (low, high) in bounds.items():
        values = tables[name][column].dropna()
        assert ((values >= low) & (values <= high)).all(), (
            f"{name}.{column} outside [{low}, {high}]: {values[(values < low) | (values > high)].to_dict()}"
        )
