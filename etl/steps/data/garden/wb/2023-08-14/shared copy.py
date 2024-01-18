"""
This file includes functions to get variables metadata in the `world_bank_pip` garden step
If new poverty lines or indicators are included, they need to be addressed here
"""

from owid.catalog import Table, VariableMeta

# This is text to include in description_key and description_processing fields

non_market_income_description = "Non-market sources of income, including food grown by subsistence farmers for their own consumption, are taken into account."

processing_description = """
For a small number of country-year observations, the World Bank PIP data contains two estimates: one based on income data and one based on consumption data. In these cases we keep only the consumption estimate in order to obtain a single series for each country.

You can find the data with all available income and consumption data points, including these overlapping estimates, in our [complete dataset](https://github.com/owid/poverty-data#a-global-dataset-of-poverty-and-inequality-measures-prepared-by-our-world-in-data-from-the-world-banks-poverty-and-inequality-platform-pip-database) of the World Bank PIP data.
"""

processing_description_relative_poverty = """
Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {povline} of the median – and then run a specific query on the PIP API to return the share of population below that line.
"""

processing_description_thr = """
Income and consumption thresholds are not directly available in the World Bank PIP API. We extract the metric primarily from [auxiliary percentiles data provided by the World Bank](https://datacatalog.worldbank.org/search/dataset/0063646). Missing country values and regional aggregations of the indicator are calculated by running multiple queries on the API to obtain the closest poverty line to each threshold.
"""

ppp_description = "The data is measured in international-$ at {ppp} prices – this adjusts for inflation and for differences in the cost of living between countries."

# Define default tolerance for each variable
TOLERANCE = 5

# These are parameters specifically defined for each type of variable
var_dict = {
    # POVERTY
    "headcount": {
        "title": "Number in poverty",
        "description": "Percentage of population living in households with an {inc_cons_dict[wel]['name']} per person below {povline}",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 0,
    },
    "headcount_ratio": {
        "title": "Share of population in poverty",
        "description": "Number of people in households with an {inc_cons_dict[wel]['name']} per person below {povline}",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "total_shortfall": {
        "title": "Total daily shortfall",
        "description": "This is the amount of money that would be theoretically needed to lift the {inc_cons_dict[wel]['name']} of all people in poverty up to {povline}. However, this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
        "unit": "international-$ in {ppp} prices",
        "short_unit": "$",
        "numDecimalPlaces": 2,
    },
    "avg_shortfall": {
        "title": "Average shortfall ($)",
        "description": "This is the amount of money that would be theoretically needed to lift the {inc_cons_dict[wel]['name']} of all people in poverty up to {povline}, averaged across the population in poverty.",
        "unit": "international-$ in {ppp} prices",
        "short_unit": "$",
        "numDecimalPlaces": 2,
    },
    "income_gap_ratio": {
        "title": "Average shortfall (%)",
        "description": "This is the average shortfall expressed as a share of the poverty line, sometimes called the 'income gap ratio'. It captures the depth of poverty of those living on less than {povline}.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "poverty_gap_index": {
        "title": "Poverty Gap Index",
        "description": "The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line).",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "watts": {
        "title": "Watts index",
        "description": "This is the mean across the population of the proportionate poverty gaps, as measured by the log of the ratio of the poverty line to income, where the mean is formed over the whole population, counting the nonpoor as having a zero poverty gap.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "poverty_severity": {
        "title": "Poverty severity",
        "description": "It is calculated as the square of the income gap ratio, the average shortfall expressed as a share of the poverty line.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    # INEQUALITY
    "gini": {
        "title": "Gini coefficient",
        "description": "The [Gini coefficient](#dod:gini) measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "palma_ratio": {
        "title": "Palma ratio",
        "description": "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "s80_s20_ratio": {
        "title": "S80/S20 ratio",
        "description": "The S80/S20 ratio is a measure of inequality that divides the share received by the richest 20% by the share of the poorest 20%. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p10_ratio": {
        "title": "P90/P10 ratio",
        "description": "P90 and P10 are the levels of {inc_cons_dict[wel]['name']} below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p50_ratio": {
        "title": "P90/P50 ratio",
        "description": "The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median {inc_cons_dict[wel]['name']}.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p50_p10_ratio": {
        "title": "P50/P10 ratio",
        "description": "The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median {inc_cons_dict[wel]['name']} is two times higher than that of someone just falling in the poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "mld": {
        "title": "Mean log deviation",
        "description": "The mean log deviation (MLD) is a measure of inequality. An MLD of zero indicates perfect equality and it takes on larger positive values as incomes become more unequal.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "polarization": {
        "title": "Polarization index",
        "description": "The polarization index, also known as the Wolfson polarization index, measures the extent to which the distribution of income or consumption is “spread out” and bi-modal. Like the Gini coefficient, the polarization index ranges from 0 (no polarization) to 1 (complete polarization).",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    # DISTRIBUTIONAL INDICATORS
    "mean": {
        "title": "Mean",
        "description": "Mean {inc_cons_dict[wel]['name']}.",
        "unit": "international-$ in {ppp} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "median": {
        "title": "Median",
        "description": "Median {inc_cons_dict[wel]['name']}.",
        "unit": "international-$ in {ppp} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "avg": {
        "title": "Average",
        "description": "The mean {inc_cons_dict[wel]['name_distribution']} per year within the {pct_dict[pct]['decile10']} (tenth of the population).",
        "unit": "international-$ in {ppp} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "share": {
        "title": "Share",
        "description": "The share of {inc_cons_dict[wel]['name_distribution']} {inc_cons_dict[wel]['verb']} by the {pct_dict[pct]['decile10']} (tenth of the population).",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "thr": {
        "title": "Threshold",
        "description": "The level of {inc_cons_dict[wel]['name_distribution']} per year below which {str(pct)}% of the population falls.",
        "unit": "international-$ in {ppp} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "share_bottom50": {
        "title": "Share of the bottom 50%",
        "description": "The share of {inc_cons_dict[wel]['name_distribution']} {inc_cons_dict[wel]['verb']} by the poorest 50%.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "share_middle40": {
        "title": "Share of the middle 40%",
        "description": "The share of {inc_cons_dict[wel]['name_distribution']} {inc_cons_dict[wel]['verb']} by the middle 40%. The middle 40% is the share of the population whose {inc_cons_dict[wel]['name']} lies between the poorest 50% and the richest 10%.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
}

# Details for each consumption or income variable
inc_cons_dict = {
    "income": {
        "name": "income",
        "name_distribution": "after tax income",
        "verb": "received",
        "description": "The data relates to income measured after taxes and benefits per capita. 'Per capita' means that the income of each household is attributed equally to each member of the household (including children).",
    },
    "consumption": {
        "name": "consumption",
        "name_distribution": "consumption",
        "verb": "spent",
        "description": "The data relates to consumption per capita. 'Per capita' means that the consumption of each household is attributed equally to each member of the household (including children).",
    },
    "income_consumption": {
        "name": "income or consumption",
        "name_distribution": "after tax income or consumption",
        "verb": "received",
        "description": "Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, per capita. 'Per capita' means that the income of each household is attributed equally to each member of the household (including children).",
    },
}

# Details for each relative poverty line
rel_dict = {40: "40% of the median", 50: "50% of the median", 60: "60% of the median"}

# Details for each absolute poverty line
abs_dict = {
    2011: {
        100: {"title": "$1 a day", "title_between": "$1"},
        190: {"title": "$1.90 a day", "title_between": "$1.90"},
        320: {"title": "$3.20 a day", "title_between": "$3.20"},
        550: {"title": "$5.50 a day", "title_between": "$5.50"},
        1000: {"title": "$10 a day", "title_between": "$10"},
        2000: {"title": "$20 a day", "title_between": "$20"},
        3000: {"title": "$30 a day", "title_between": "$30"},
    },
    2017: {
        100: {"title": "$1 a day", "title_between": "$1"},
        215: {"title": "$2.15 a day", "title_between": "$2.15"},
        365: {"title": "$3.65 a day", "title_between": "$3.65"},
        685: {"title": "$6.85 a day", "title_between": "$6.85"},
        1000: {"title": "$10 a day", "title_between": "$10"},
        2000: {"title": "$20 a day", "title_between": "$20"},
        3000: {"title": "$30 a day", "title_between": "$30"},
        4000: {"title": "$40 a day", "title_between": "$40"},
    },
}

# Details for naming each decile/percentile
pct_dict = {
    1: {"decile10": "Poorest decile", "decile9": "Poorest decile"},
    2: {"decile10": "2nd decile", "decile9": "2nd decile"},
    3: {"decile10": "3rd decile", "decile9": "3rd decile"},
    4: {"decile10": "4th decile", "decile9": "4th decile"},
    5: {"decile10": "5th decile", "decile9": "5th decile"},
    6: {"decile10": "6th decile", "decile9": "6th decile"},
    7: {"decile10": "7th decile", "decile9": "7th decile"},
    8: {"decile10": "8th decile", "decile9": "8th decile"},
    9: {"decile10": "9th decile", "decile9": "Richest decile"},
    10: {"decile10": "Richest decile", "decile9": ""},
}


# This function creates the metadata for each variable in the dataset, from the dictionaries defined above
def add_metadata_vars(tb_garden: Table, ppp_version: int, welfare_type: str) -> Table:
    """
    Add metadata for each variable in the dataset, using the dictionaries above and the functions below
    """
    # Create a list from povline_dict
    povline_list = list(abs_dict[ppp_version].keys())

    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in var_dict:
        # For variables uniquely defined for each country-year-welfare type-reporting level (mostly inequality indicators + mean and median)
        col_name = f"{var}"

        if col_name in cols:
            # Get the origins of the variable
            origins = tb_garden[col_name].metadata.origins

            # Create metadata for these variables
            tb_garden[col_name].metadata = var_metadata_inequality_mean_median(var, origins, ppp_version, welfare_type)
        for povline in abs_dict:
            # For variables that have "dollar a day" poverty lines
            col_name = f"{var}_{povline}{smooth}"

            if col_name in cols:
                # Get the origins of the variable
                origins = tb_garden[col_name].metadata.origins

                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_absolute(var, povline, origins, smooth)

                # Replace the {povline} placeholder in the description
                tb_garden[col_name].metadata.description_short = tb_garden[col_name].metadata.description_short.replace(
                    "{povline}", str(povline_dict[povline]["description"])
                )

            # For variables above poverty lines
            col_name = f"{var}_above_{povline}{smooth}"

            if col_name in cols:
                # Get the origins of the variable
                origins = tb_garden[col_name].metadata.origins

                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_absolute(var, povline, origins, smooth)

                # Replace the {povline} placeholder in the description
                tb_garden[col_name].metadata.description_short = tb_garden[col_name].metadata.description_short.replace(
                    "{povline}", str(povline_dict[povline]["description"])
                )

                # Replace "below" with "above" in the description
                tb_garden[col_name].metadata.description_short = tb_garden[col_name].metadata.description_short.replace(
                    "below", "above"
                )

                # Replace "in poverty" with "not in poverty" in the title
                tb_garden[col_name].metadata.title = tb_garden[col_name].metadata.title.replace(
                    "in poverty", "not in poverty"
                )

                # Replicate the title in the display name
                tb_garden[col_name].metadata.display["name"] = tb_garden[col_name].metadata.title

        for i in range(len(povline_list)):
            if i != 0:
                # For variables between poverty lines
                col_name = f"{var}_between_{povline_list[i-1]}_{povline_list[i]}{smooth}"

                if col_name in cols:
                    # Get the origins of the variable
                    origins = tb_garden[col_name].metadata.origins

                    # Create metadata for these variables
                    tb_garden[col_name].metadata = var_metadata_between(
                        var, povline_list[i - 1], povline_list[i], origins, smooth
                    )

                    # Replace the {povline} placeholder in the description with the range of poverty lines
                    tb_garden[col_name].metadata.description_short = tb_garden[
                        col_name
                    ].metadata.description_short.replace(
                        "{povline}",
                        f"living between {povline_dict[povline_list[i-1]]['title_between']} and {povline_dict[povline_list[i]]['title_between']} a day",
                    )

        for cbn in cbn_dict:
            # For variables that use the CBN method
            col_name = f"{var}_{cbn}{smooth}"

            if col_name in cols:
                # Get the origins of the variable
                origins = tb_garden[col_name].metadata.origins

                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_cbn(var, cbn, origins, smooth)

                # Replace the {povline} placeholder in the description
                tb_garden[col_name].metadata.description_short = tb_garden[col_name].metadata.description_short.replace(
                    "{povline}", cbn_dict[cbn]["description"]
                )

            # For variables above CBN
            col_name = f"{var}_above_{cbn}{smooth}"

            if col_name in cols:
                # Get the origins of the variable
                origins = tb_garden[col_name].metadata.origins

                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_cbn(var, cbn, origins, smooth)

                # Replace the {povline} placeholder in the description
                tb_garden[col_name].metadata.description_short = tb_garden[col_name].metadata.description_short.replace(
                    "{povline}", cbn_dict[cbn]["description"]
                )

                # Replace "below" with "above" in the description
                tb_garden[col_name].metadata.description_short = tb_garden[col_name].metadata.description_short.replace(
                    "below", "above"
                )

                # Replace "in poverty" with "not in poverty" in the title
                tb_garden[col_name].metadata.title = tb_garden[col_name].metadata.title.replace(
                    "in poverty", "not in poverty"
                )

                # Replicate the title in the display name
                tb_garden[col_name].metadata.display["name"] = tb_garden[col_name].metadata.title

    return tb_garden


# Metadata functions to show a clearer main code
def var_metadata_inequality_mean_median(var, origins, ppp_version, welfare_type) -> VariableMeta:
    """
    Create metadata for defined uniquely by their name
    """
    if var in ["mean", "median"]:
        meta = VariableMeta(
            title=f"{var_dict[var]['title']} {inc_cons_dict[welfare_type]['name']}",
            description_short=var_dict[var]["description"],
            description_key=[
                ppp_description,
                inc_cons_dict[welfare_type]["description"],
                non_market_income_description,
            ],
            description_processing=f"""
            {processing_description}
            """,
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
            origins=origins,
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
            "tolerance": TOLERANCE,
        }

        meta.presentation = {
            "title_public": meta.title,
        }

    else:
        meta = VariableMeta(
            title=f"{var_dict[var]['title']}",
            description_short=var_dict[var]["description"],
            description_key=[
                inc_cons_dict[welfare_type]["description"],
                non_market_income_description,
            ],
            description_processing=f"""
            {processing_description}
            """,
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
            origins=origins,
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
            "tolerance": TOLERANCE,
        }

        meta.presentation = {
            "title_public": meta.title,
        }

    return meta


def var_metadata_absolute(var, povline, origins, smooth) -> VariableMeta:
    """
    Create metadata for variables with absolute poverty lines
    """
    meta = VariableMeta(
        title=f"{povline_dict[povline]['title']} - {var_dict[var]['title']} ({smooth_dict[smooth]['title']})",
        description_short=var_dict[var]["description"],
        description_key=[ppp_description, dod_description],
        description_processing=f"""
        {processing_description}
{smooth_dict[smooth]['description']}""",
        unit=var_dict[var]["unit"],
        short_unit=var_dict[var]["short_unit"],
        origins=origins,
    )
    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        "entityAnnotationsMap": "Western offshoots (Moatsos): United States, Canada, Australia and New Zealand",
    }

    meta.presentation = {
        "title_public": f"{povline_dict[povline]['title']} - {var_dict[var]['title']}",
    }

    return meta


def var_metadata_between(var, povline1, povline2, origins, smooth) -> VariableMeta:
    """
    Create metadata for variables between poverty lines
    """
    meta = VariableMeta(
        title=f"{povline_dict[povline1]['title_between']}-{povline_dict[povline2]['title_between']} - {var_dict[var]['title']} ({smooth_dict[smooth]['title']})",
        description_short=var_dict[var]["description"],
        description_key=[ppp_description, dod_description],
        description_processing=f"""
        {processing_description}
{smooth_dict[smooth]['description']}""",
        unit=var_dict[var]["unit"],
        short_unit=var_dict[var]["short_unit"],
        origins=origins,
    )
    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        "entityAnnotationsMap": "Western offshoots (Moatsos): United States, Canada, Australia and New Zealand",
    }

    meta.presentation = {
        "title_public": f"{povline_dict[povline1]['title_between']}-{povline_dict[povline2]['title_between']} - {var_dict[var]['title']}",
    }

    return meta
