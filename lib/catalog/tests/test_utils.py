import pandas as pd
import pytest

from owid.catalog import Origin, Table, VariableMeta, VariablePresentationMeta
from owid.catalog.utils import underscore


def test_underscore():
    assert (
        underscore(
            "Proportion of young women and men aged 18‑29 years who experienced sexual violence by age 18",
        )
        == "proportion_of_young_women_and_men_aged_18_29_years_who_experienced_sexual_violence_by_age_18"
    )
    assert (
        underscore(
            "`17.11.1 - Developing countries’ and least developed countries’ share of global merchandise exports (%) - TX_EXP_GBMRCH`"
        )
        == "_17_11_1__developing_countries_and_least_developed_countries_share_of_global_merchandise_exports__pct__tx_exp_gbmrch"
    )
    assert underscore("Urban population") == "urban_population"
    assert underscore("Urban population (% of total population)") == "urban_population__pct_of_total_population"
    assert (
        underscore("Women's share of population ages 15+ living with HIV (%)")
        == "womens_share_of_population_ages_15plus_living_with_hiv__pct"
    )
    assert (
        underscore("Water productivity, total (constant 2010 US$ GDP per cubic meter of total freshwater withdrawal)")
        == "water_productivity__total__constant_2010_usd_gdp_per_cubic_meter_of_total_freshwater_withdrawal"
    )
    assert (
        underscore("Agricultural machinery, tractors per 100 sq. km of arable land")
        == "agricultural_machinery__tractors_per_100_sq__km_of_arable_land"
    )
    assert (
        underscore("GDP per capita, PPP (current international $)")
        == "gdp_per_capita__ppp__current_international_dollar"
    )
    assert (
        underscore("Automated teller machines (ATMs) (per 100,000 adults)")
        == "automated_teller_machines__atms__per_100_000_adults"
    )
    assert (
        underscore("Political regimes - OWID based on Boix et al. (2013), V-Dem (v12), and Lührmann et al. (2018)")
        == "political_regimes__owid_based_on_boix_et_al__2013__v_dem__v12__and_luhrmann_et_al__2018"
    )
    assert (
        underscore("Adjusted savings: particulate emission damage (current US$)")
        == "adjusted_savings__particulate_emission_damage__current_usd"
    )
    assert (
        underscore(
            "Benefit incidence of unemployment benefits and ALMP to poorest quintile (% of total U/ALMP benefits)"
        )
        == "benefit_incidence_of_unemployment_benefits_and_almp_to_poorest_quintile__pct_of_total_u_almp_benefits"
    )
    assert (
        underscore("Business extent of disclosure index (0=less disclosure to 10=more disclosure)")
        == "business_extent_of_disclosure_index__0_less_disclosure_to_10_more_disclosure"
    )
    assert underscore("Firms that spend on R&D (% of firms)") == "firms_that_spend_on_r_and_d__pct_of_firms"
    assert (
        underscore(
            "Wages in the manufacturing sector vs. several food prices in the US – U.S. Bureau of Labor Statistics (2013)"
        )
        == "wages_in_the_manufacturing_sector_vs__several_food_prices_in_the_us__u_s__bureau_of_labor_statistics__2013"
    )
    assert (
        underscore('Tax "composition" –\tArroyo Abad  and P. Lindert (2016)')
        == "tax_composition__arroyo_abad__and_p__lindert__2016"
    )
    assert underscore("20th century deaths in US - CDC") == "_20th_century_deaths_in_us__cdc"
    assert (
        underscore("Poverty rate (<50% of median) (LIS Key Figures, 2018)")
        == "poverty_rate__lt_50pct_of_median__lis_key_figures__2018"
    )
    assert underscore("10") == "_10"
    assert (
        underscore(
            "Indicator 1.5.1: Death rate due to exposure to forces of nature (per 100,000 population) *Estimates reported here are based on a 10-year distributed lag for natural disaster mortality. - Past - Scaled"
        )
        == "indicator_1_5_1__death_rate_due_to_exposure_to_forces_of_nature__per_100_000_population__estimates_reported_here_are_based_on_a_10_year_distributed_lag_for_natural_disaster_mortality__past__scaled"
    )
    assert underscore("a|b") == "a_b"
    assert underscore("$/£ exchange rate") == "dollar_ps_exchange_rate"
    assert (
        underscore("‘cost of basic needs’ approach - share of population below poverty line")
        == "cost_of_basic_needs_approach__share_of_population_below_poverty_line"
    )

    # camelCase to snake_case
    assert underscore("camelCase") == "camelcase"
    assert underscore("camelcase", camel_to_snake=True) == "camelcase"
    assert underscore("camelCase", camel_to_snake=True) == "camel_case"
    assert underscore("CamelCase", camel_to_snake=True) == "camel_case"
    assert underscore("CAMELCase", camel_to_snake=True) == "camel_case"
    assert underscore("camelCase1", camel_to_snake=True) == "camel_case1"
    assert underscore("camelCase_1", camel_to_snake=True) == "camel_case_1"


def test_underscore_table():
    df = pd.DataFrame({"A": [1, 2, 3], "b": [1, 2, 3]})
    df.index.names = ["I"]

    t = Table(df)
    t["A"].metadata.description = "column A"
    t["b"].metadata.description = "column B"

    tt = t.underscore()
    assert list(tt.columns) == ["a", "b"]
    assert tt.index.names == ["i"]

    # original column name is moved to title if we underscored it
    assert tt["a"].metadata.description == "column A"
    assert tt["a"].metadata.title == "A"

    # title is None if underscoring didn't change the name
    assert tt["b"].metadata.description == "column B"
    assert tt["b"].metadata.title is None


def test_underscore_table_collision():
    df = pd.DataFrame({"A__x": [1, 2, 3], "B": [1, 2, 3], "A(x)": [1, 2, 3]})
    t = Table(df)
    t["A__x"].metadata.description = "desc1"
    t["B"].metadata.description = "desc2"
    t["A(x)"].metadata.description = "desc3"

    # raise error by default
    with pytest.raises(NameError):
        t.underscore()

    # add suffix
    tt = t.underscore(collision="rename")
    assert list(tt.columns) == ["a__x_1", "b", "a__x_2"]

    # make sure we retain metadata
    assert tt["a__x_1"].metadata.description == "desc1"
    assert tt["b"].metadata.description == "desc2"
    assert tt["a__x_2"].metadata.description == "desc3"


def test_pruned_json():
    meta = VariableMeta(
        origins=[Origin(title="Title", producer="Producer")],
        presentation=VariablePresentationMeta(title_public="Title public"),
    )
    assert meta.to_dict() == {
        "origins": [{"producer": "Producer", "title": "Title"}],
        "presentation": {"title_public": "Title public"},
    }
