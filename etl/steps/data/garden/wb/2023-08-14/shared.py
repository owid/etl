"""
This file includes functions to get variables metadata in the `world_bank_pip` garden step
If new variables are included in the dataset (from LISSY) the dictionaries feeding metadata functions have to be updated (if not an error will show up)
"""

from owid.catalog import Table, VariableMeta

# This is text common to all variables

notes_title = "NOTES ON HOW WE PROCESSED THIS INDICATOR"

processing_description = """
        The Luxembourg Income Study data is created from standardized household survey microdata available in their [LISSY platform](https://www.lisdatacenter.org/data-access/lissy/). The estimations follow the methodology available in LIS, Key Figures and DART platform.

        After tax income is obtained by using the disposable household income variable (`dhi`)

        Before tax income is estimated by calculating the sum of income from labor and capital (variable `hifactor`), cash transfers and in-kind goods and services from privates (`hiprivate`) and private pensions (`hi33`). This is done only for surveys where tax and contributions are fully captured, collected or imputed.

        After tax income (cash) is obtained using the disposable household cash income variable (`dhci`).

        Consumption is obtained using the total consumption variable (`hcexp`).

        Income data is converted from local currency into international-$ by dividing by the [LIS PPP factor](https://www.lisdatacenter.org/resources/ppp-deflators/), available as an additional database in the system.

        Incomes are top and bottom-coded by replacing negative values with zeros and setting boundaries for extreme values of log income: at the top Q3 plus 3 times the interquartile range (Q3-Q1), and at the bottom Q1 minus 3 times the interquartile range.

        Incomes are equivalized by dividing each household observation by the square root of the number of household members (`nhhmem`). Per capita estimates are calculated by dividing incomes by the number of household members.
"""

processing_poverty = "Poverty indicators are obtained by using [Stata’s povdeco function](https://ideas.repec.org/c/boc/bocode/s366004.html). Weights are set as the product between the number of household members (`nhhmem`) and the normalized household weight (`hwgt`). The function generates FGT(0) and FGT(1), headcount ratio and poverty gap index. After extraction, further data processing steps are done to estimate other poverty indicators using these values, population and poverty lines for absolute and relative poverty."
processing_gini_mean_median = "Gini coefficients are obtained by using [Stata’s ineqdec0 function](https://ideas.repec.org/c/boc/bocode/s366007.html). Weights are set as the product between the number of household members (nhhmem) and the normalized household weight (hwgt). From this function, mean and median values are also calculated."
processing_distribution = "Income shares and thresholds by decile are obtained by using [Stata’s sumdist function](https://ideas.repec.org/c/boc/bocode/s366005.html). The parameters set are again the weight (`nhhmem*hwgt`) and the number of quantile groups (10). Threshold ratios, share ratios and averages by decile are estimated after the use of LISSY with this data."

ppp_description = "The data is measured in international-$ at 2017 prices – this adjusts for inflation and for differences in the cost of living between countries."

# These are parameters specifically defined for each type of variable
var_dict = {
    "avg": {
        "title": "Average",
        "description": "The mean {inc_cons_dict[wel]['type']} per year within the {pct_dict[pct]['decile10']} (tenth of the population).",
        "unit": "international-$ in 2017 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "share": {
        "title": "Share",
        "description": "The share of {inc_cons_dict[wel]['type']} {inc_cons_dict[wel]['verb']} by the {pct_dict[pct]['decile10']} (tenth of the population).",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "thr": {
        "title": "Threshold",
        "description": "The level of {inc_cons_dict[wel]['type']} per year below which {str(pct)}% of the population falls.",
        "unit": "international-$ in 2017 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "avg_shortfall": {
        "title": "Average shortfall ($)",
        "description": "This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to the poverty line of {povline}, averaged across the population in poverty.",
        "unit": "international-$ in 2017 prices",
        "short_unit": "$",
        "numDecimalPlaces": 2,
    },
    "headcount": {
        "title": "Number in poverty",
        "description": "Number of people living below the poverty line of {povline}",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 0,
    },
    "headcount_ratio": {
        "title": "Share of population in poverty",
        "description": "Share of the population living below the poverty line of {povline}",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "income_gap_ratio": {
        "title": "Average shortfall (%)",
        "description": "This is the average shortfall expressed as a share of the poverty line, sometimes called the 'income gap ratio'. It captures the depth of poverty in which those below {povline} are living.",
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
    "total_shortfall": {
        "title": "Total shortfall",
        "description": "This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povline}.",
        "unit": "international-$ in 2017 prices",
        "short_unit": "$",
        "numDecimalPlaces": 2,
    },
    "gini": {
        "title": "Gini coefficient",
        "description": "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "mean": {
        "title": "Mean",
        "description": "Mean {inc_cons_dict[wel]['type']}.",
        "unit": "international-$ in 2017 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "median": {
        "title": "Median",
        "description": "Median {inc_cons_dict[wel]['type']}.",
        "unit": "international-$ in 2017 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
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
        "description": "The S80/S20 ratio is the share of total income or consumption of the top 20% divided by the share of the bottom 20%.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p10_ratio": {
        "title": "P90/P10 ratio",
        "description": "P90 and P10 are the levels of income or consumption below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p50_ratio": {
        "title": "P90/P50 ratio",
        "description": "The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median income or consumption.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p50_p10_ratio": {
        "title": "P50/P10 ratio",
        "description": "The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median income or consumption is two times higher than that of someone just falling in the poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "share_bottom50": {
        "title": "Share of the bottom 50%",
        "description": "The share of {inc_cons_dict[wel]['type']} {inc_cons_dict[wel]['verb']} by the poorest 50%.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "share_middle40": {
        "title": "Share of the middle 40%",
        "description": "The share of {inc_cons_dict[wel]['type']} {inc_cons_dict[wel]['verb']} by the middle 40%. The middle 40% is the share of the population whose income or consumption lies between the poorest 50% and the richest 10%.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
}

# Details for each consumption or income variable
inc_cons_dict = {
    "dhi": {
        "name": "Disposable household income",
        "type": "income",
        "verb": "received",
        "description": "Income is ‘post-tax’ — measured after taxes have been paid and most government benefits have been received.",
    },
    "dhci": {
        "name": "Disposable household cash income",
        "type": "income",
        "verb": "received",
        "description": "Income is ‘post-tax’ — measured after taxes have been paid and most government benefits have been received and excluding fringe benefits, home production, in-kind benefits and transfers",
    },
    "mi": {
        "name": "Market income",
        "type": "income",
        "verb": "received",
        "description": "Income is ‘pre-tax’ — measured before taxes have been paid and most government benefits have been received.",
    },
    "hcexp": {
        "name": "Total consumption",
        "type": "consumption",
        "verb": "spent",
        "description": "This measure is related to total consumption, including that stemming from goods and services that have been purchased by the household, and goods ans services that have not been purchased, but either given to the household from somebody else, or self-produced.",
    },
}

# Details for each equivalence scale (equivalized with LIS scale and per capita)
equivalence_scales_dict = {
    "eq": {
        "name": "equivalized",
        "description": "Income has been equivalized – adjusted to account for the fact that people in the same household can share costs like rent and heating.",
    },
    "pc": {
        "name": "per capita",
        "description": "Income is per capita, which means that household income is divided by the total number of household members.",
    },
}

# Details for each relative poverty line
rel_dict = {40: "40% of the median", 50: "50% of the median", 60: "60% of the median"}

# Details for each absolute poverty line
abs_dict = {
    100: "$1 a day",
    200: "$2 a day",
    215: "$2.15 a day",
    365: "$3.65 a day",
    500: "$5 a day",
    685: "$6.85 a day",
    1000: "$10 a day",
    2000: "$20 a day",
    3000: "$30 a day",
    4000: "$40 a day",
}

# Details for naming each decile/percentile
pct_dict = {
    10: {"decile10": "Poorest decile", "decile9": "Poorest decile"},
    20: {"decile10": "2nd decile", "decile9": "2nd decile"},
    30: {"decile10": "3rd decile", "decile9": "3rd decile"},
    40: {"decile10": "4th decile", "decile9": "4th decile"},
    50: {"decile10": "5th decile", "decile9": "5th decile"},
    60: {"decile10": "6th decile", "decile9": "6th decile"},
    70: {"decile10": "7th decile", "decile9": "7th decile"},
    80: {"decile10": "8th decile", "decile9": "8th decile"},
    90: {"decile10": "9th decile", "decile9": "Richest decile"},
    99: {"decile10": "Top 1%", "decile9": "Top 1%"},
    100: {"decile10": "Richest decile", "decile9": ""},
}


# This function creates the metadata for each variable in the LIS dataset, from the dictionaries defined above
def add_metadata_vars(tb_garden: Table):
    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in var_dict:
        for wel in inc_cons_dict:
            for e in equivalence_scales_dict:
                # For variables that use income variable and equivalence scale
                col_name = f"{var}_{wel}_{e}"

                if col_name in cols:
                    # Create metadata for these variables
                    tb_garden[col_name].metadata = var_metadata_income_and_equivalence_scale(var, wel, e)

                    # Replace income/wealth words according to `wel`
                    tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                        "{inc_cons_dict[wel]['verb']}", str(inc_cons_dict[wel]["verb"])
                    )
                    tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                        "{inc_cons_dict[wel]['type']}", str(inc_cons_dict[wel]["type"])
                    )

                for rel in rel_dict:
                    # For variables that use income variable, equivalence scale and relative poverty lines
                    col_name = f"{var}_{rel}_median_{wel}_{e}"

                    if col_name in cols:
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_equivalence_scale_relative(var, wel, e, rel)

                        # Replace values in description according to `rel`
                        tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                            "{povline}", rel_dict[rel]
                        )

                for abs in abs_dict:
                    # For variables that use income variable, equivalence scale and absolute poverty lines
                    col_name = f"{var}_{wel}_{e}_{abs}"

                    if col_name in cols:
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_equivalence_scale_absolute(var, wel, e, abs)

                        # Replace values in description according to `abs`
                        tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                            "{povline}", abs_dict[abs]
                        )

                for pct in pct_dict:
                    # For variables that use income variable, equivalence scale and percentiles (deciles)
                    col_name = f"{var}_p{pct}_{wel}_{e}"

                    if col_name in cols:
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_equivalence_scale_percentiles(
                            var, wel, e, pct
                        )

                        # Replace values in description according to `pct`, depending on `var`
                        if var == "thr":
                            tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                                "{str(pct)}", str(pct)
                            )

                        else:
                            tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                                "{pct_dict[pct]['decile10']}", pct_dict[pct]["decile10"].lower()
                            )

                        # Replace income/wealth words according to `wel`
                        tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                            "{inc_cons_dict[wel]['verb']}", str(inc_cons_dict[wel]["verb"])
                        )
                        tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                            "{inc_cons_dict[wel]['type']}", str(inc_cons_dict[wel]["type"])
                        )

    return tb_garden


# Metadata functions to show a clearer main code
def var_metadata_income_and_equivalence_scale(var, wel, e) -> VariableMeta:
    # Add ppp description to monetary variables
    if var == "mean" or var == "median":
        meta = VariableMeta(
            title=f"{var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {equivalence_scales_dict[e]['description']}

            {ppp_description}

            {notes_title}

            {processing_description}

            {processing_gini_mean_median}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }
    # For inequality vars we don't need to add ppp description
    else:
        meta = VariableMeta(
            title=f"{var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {equivalence_scales_dict[e]['description']}

            {notes_title}

            {processing_description}

            {processing_gini_mean_median}

            {processing_distribution}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }
    return meta


def var_metadata_income_equivalence_scale_relative(var, wel, e, rel) -> VariableMeta:
    meta = VariableMeta(
        title=f"{rel_dict[rel]} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
        description=f"""{var_dict[var]['description']}

        {inc_cons_dict[wel]['description']}

        {equivalence_scales_dict[e]['description']}

        {notes_title}

        {processing_description}

        {processing_poverty}""",
        unit=var_dict[var]["unit"],
        short_unit=var_dict[var]["short_unit"],
    )
    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
    }
    return meta


def var_metadata_income_equivalence_scale_absolute(var, wel, e, abs) -> VariableMeta:
    meta = VariableMeta(
        title=f"{abs_dict[abs]} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
        description=f"""{var_dict[var]['description']}

        {inc_cons_dict[wel]['description']}

        {equivalence_scales_dict[e]['description']}

        {ppp_description}

        {notes_title}

        {processing_description}

        {processing_poverty}""",
        unit=var_dict[var]["unit"],
        short_unit=var_dict[var]["short_unit"],
    )
    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
    }
    return meta


def var_metadata_income_equivalence_scale_percentiles(var, wel, e, pct) -> VariableMeta:
    if var == "thr":
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile9']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {equivalence_scales_dict[e]['description']}

            {ppp_description}

            {notes_title}

            {processing_description}

            {processing_distribution}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }

    elif var == "avg":
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile10']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {equivalence_scales_dict[e]['description']}

            {ppp_description}

            {notes_title}

            {processing_description}

            {processing_distribution}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }
    # Shares do not show PPP description
    else:
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile10']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}, {equivalence_scales_dict[e]['name']})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {equivalence_scales_dict[e]['description']}

            {notes_title}

            {processing_description}

            {processing_distribution}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }
    return meta
