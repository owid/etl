"""
This function creates the metadata for each variable in the WID dataset, from the dictionaries defined below
If new variables are included in the dataset (from `wid` command in Stata) the dictionaries feeding metadata functions have to be updated (if not an error will show up)
"""

from typing import List

from owid.catalog import Table, VariableMeta, VariablePresentationMeta

# Define PPP year
# NOTE: Change the year when needed
PPP_YEAR = 2023

# Define default tolerance for each variable
TOLERANCE = 5

# This is text common to all variables

METHODOLOGY_DESCRIPTION = [
    "The data is estimated from a combination of household surveys, tax records and national accounts data. This combination can provide a more accurate picture of the incomes of the richest, which tend to be captured poorly in household survey data alone.",
    "These underlying data sources are not always available. For some countries, observations are extrapolated from data relating to other years, or are sometimes modeled based on data observed in other countries. For more information on this methodology, see this related [technical note](https://wid.world/document/countries-with-regional-income-imputations-on-wid-world-world-inequality-lab-technical-note-2021-15/).",
]

METHODOLOGY_DESCRIPTION_WEALTH = [
    "The data is constructed using the Mixed Income Capitalization-Survey (MICS) method, which combines capitalized income flows from tax data with survey-based estimates for assets that do not generate taxable income. This is done because wealth distributions are usually not directly available in administrative data, and because wealth surveys coverage is even more limited than for income.",
    "Even with those considerations, the underlying data is not always available. For some countries, observations are extrapolated from data relating to other years, or are sometimes modeled based on data observed in other countries. For more information on this methodology, see this related [technical note](https://wid.world/document/global-wealth-inequality-on-wid-world-estimates-and-imputations-wid-world-technical-note-2023-11/).",
]

POST_TAX_NATIONAL_DESCRIPTION = [
    "In the case of national post-tax income, when the data sources are not available, distributions are constructed by using the more widely available pre-tax distributions, combined with tax revenue and government expenditure aggregates. This method is described in more detail in this [technical note](https://wid.world/document/preliminary-estimates-of-global-posttax-income-distributions-world-inequality-lab-technical-note-2023-02/)."
]

RELATIVE_POVERTY_DESCRIPTION = "This data has been estimated by calculating the {povline}, and then checking that value against the closest threshold in the percentile distribution. The headcount ratio is then the percentile, the share of the population below that threshold."

PROCESSING_DESCRIPTION = """We extract estimations of Gini, mean, percentile thresholds, averages, and shares via the [`wid` Stata command](https://github.com/thomasblanchet/wid-stata-tool). We calculate threshold and share ratios by dividing different thresholds and shares, respectively."""

PPP_DESCRIPTION = f"The data is measured in international-$ at {PPP_YEAR} prices – this adjusts for inflation and for differences in living costs between countries."

# These are parameters specifically defined for each type of variable
VAR_DICT = {
    "avg": {
        "title": "Average",
        "description": "The mean {WELFARE_DICT[wel]['type']} per year within the {PCT_DICT[pct]['decile10_extra'].lower()}.",
        "unit": f"international-$ in {PPP_YEAR} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "share": {
        "title": "Share",
        "description": "The share of {WELFARE_DICT[wel]['type']} {WELFARE_DICT[wel]['verb']} by the {PCT_DICT[pct]['decile10_extra'].lower()}.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "p50p90_share": {
        "title": "Middle 40% - Share",
        "description": "The share of {WELFARE_DICT[wel]['type']} {WELFARE_DICT[wel]['verb']} by the middle 40%. The middle 40% is the share of the population whose {WELFARE_DICT[wel]['type']} lies between the poorest 50% and the richest 10%.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "thr": {
        "title": "Threshold",
        "description": "The level of {WELFARE_DICT[wel]['type']} per year below which {str(PCT_DICT[pct]['thr_number'])}% of the population falls.",
        "unit": f"international-$ in {PPP_YEAR} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "p0p100_gini": {
        "title": "Gini coefficient",
        "description": "The [Gini coefficient](#dod:gini) measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p0p100_avg": {
        "title": "Mean",
        "description": "Mean {WELFARE_DICT[wel]['type']}.",
        "unit": f"international-$ in {PPP_YEAR} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "median": {
        "title": "Median",
        "description": "Median {WELFARE_DICT[wel]['type']}.",
        "unit": f"international-$ in {PPP_YEAR} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "palma_ratio": {
        "title": "Palma ratio",
        "description": "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 1,
    },
    "s80_s20_ratio": {
        "title": "S80/S20 ratio",
        "description": "The S80/S20 ratio is a measure of inequality that divides the share received by the richest 20% by the share of the poorest 20%. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 1,
    },
    "p90_p10_ratio": {
        "title": "P90/P10 ratio",
        "description": "P90 and P10 are the levels of {WELFARE_DICT[wel]['type']} below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 1,
    },
    "p90_p50_ratio": {
        "title": "P90/P50 ratio",
        "description": "The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median {WELFARE_DICT[wel]['type']}.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 1,
    },
    "p50_p10_ratio": {
        "title": "P50/P10 ratio",
        "description": "The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median {WELFARE_DICT[wel]['type']} is two times higher than that of someone just falling in the poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 1,
    },
    "headcount_ratio": {
        "title": "Share of population in poverty",
        "description": "Share of the population living below the poverty line of {povline}",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 0,
    },
}

# Details for each income variable
WELFARE_DICT = {
    "pretax": {
        "name": "Pretax",
        "type": "income",
        "verb": "received",
        "description": 'Income is "pre-tax" — measured before taxes have been paid and most government benefits have been received. It is, however, measured after the operation of pension schemes, both private and public.',
    },
    "posttax_dis": {
        "name": "Post-tax disposable",
        "type": "income",
        "verb": "received",
        "description": 'Income is "post-tax" — measured after taxes have been paid and most government benefits have been received, but does not include in-kind benefits and therefore does not add up to national income.',
    },
    "posttax_nat": {
        "name": "Post-tax national",
        "type": "income",
        "verb": "received",
        "description": 'Income is "post-tax" — measured after taxes have been paid and most government benefits have been received.',
    },
    "wealth": {
        "name": "Net national wealth",
        "type": "wealth",
        "verb": "owned",
        "description": "This measure is related to net national wealth, which is the total value of non-financial (housing, land) and financial assets (deposits, bonds, equities, etc.) held by households, minus their debts.",
    },
}

# Details for naming each decile/percentile
PCT_DICT = {
    "p0p10": {
        "decile10": "Poorest decile",
        "decile9": "Minimum value",
        "thr_number": 0,
        "decile10_extra": "Poorest decile (tenth of the population)",
    },
    "p10p20": {
        "decile10": "2nd decile",
        "decile9": "Poorest decile",
        "thr_number": 10,
        "decile10_extra": "2nd decile (tenth of the population)",
    },
    "p20p30": {
        "decile10": "3rd decile",
        "decile9": "2nd decile",
        "thr_number": 20,
        "decile10_extra": "3rd decile (tenth of the population)",
    },
    "p30p40": {
        "decile10": "4th decile",
        "decile9": "3rd decile",
        "thr_number": 30,
        "decile10_extra": "4th decile (tenth of the population)",
    },
    "p40p50": {
        "decile10": "5th decile",
        "decile9": "4th decile",
        "thr_number": 40,
        "decile10_extra": "5th decile (tenth of the population)",
    },
    "p50p60": {
        "decile10": "6th decile",
        "decile9": "5th decile (median)",
        "thr_number": 50,
        "decile10_extra": "6th decile (tenth of the population)",
    },
    "p60p70": {
        "decile10": "7th decile",
        "decile9": "6th decile",
        "thr_number": 60,
        "decile10_extra": "7th decile (tenth of the population)",
    },
    "p70p80": {
        "decile10": "8th decile",
        "decile9": "7th decile",
        "thr_number": 70,
        "decile10_extra": "8th decile (tenth of the population)",
    },
    "p80p90": {
        "decile10": "9th decile",
        "decile9": "8th decile",
        "thr_number": 80,
        "decile10_extra": "9th decile (tenth of the population)",
    },
    "p90p100": {
        "decile10": "Richest decile",
        "decile9": "Richest decile",
        "thr_number": 90,
        "decile10_extra": "Richest decile (tenth of the population)",
    },
    "p99p100": {"decile10": "Top 1%", "decile9": "Top 1%", "thr_number": 99, "decile10_extra": "Richest 1%"},
    "p99_9p100": {"decile10": "Top 0.1%", "decile9": "Top 0.1%", "thr_number": 99.9, "decile10_extra": "Richest 0.1%"},
    "p99_99p100": {
        "decile10": "Top 0.01%",
        "decile9": "Top 0.01%",
        "thr_number": 99.99,
        "decile10_extra": "Richest 0.01%",
    },
    "p99_999p100": {
        "decile10": "Top 0.001%",
        "decile9": "Top 0.001%",
        "thr_number": 99.999,
        "decile10_extra": "Richest 0.001%",
    },
    "p0p50": {"decile10": "Bottom 50%", "decile9": "Bottom 50%", "thr_number": "", "decile10_extra": "Poorest 50%"},
    "p90p99": {
        "decile10": "Between 90th and 99th percentiles",
        "decile9": "",
        "thr_number": "",
        "decile10_extra": "People between the 90th and 99th percentiles",
    },
}

# Details for each relative poverty line
REL_DICT = {40: "40% of the median", 50: "50% of the median", 60: "60% of the median"}

# Details for extrapolations or estimations
EXTRAPOLATION_DICT = {
    "": {
        "title": "Estimated",
        "description": "Interpolations and extrapolations are excluded by using the option `exclude` in the Stata command.",
    },
    "_extrapolated": {
        "title": "Extrapolated",
        "description": "Interpolations and extrapolations are included.",
    },
}


def add_metadata_vars(tb_garden: Table) -> Table:
    """
    This function adds metadata to all the variables in the WID dataset
    """
    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in VAR_DICT:
        for wel in WELFARE_DICT:
            for ext in EXTRAPOLATION_DICT:
                # For variables that use income variable
                col_name = f"{var}_{wel}{ext}"

                if col_name in cols:
                    # Get the origins of the variable
                    origins = tb_garden[col_name].metadata.origins
                    # Create metadata for these variables
                    tb_garden[col_name].metadata = var_metadata_income(var, origins, wel, ext)
                    # Replace income/wealth words according to `wel`
                    tb_garden[col_name].metadata.description_short = (
                        tb_garden[col_name]
                        .metadata.description_short.replace(
                            "{WELFARE_DICT[wel]['type']}", str(WELFARE_DICT[wel]["type"])
                        )
                        .replace("{WELFARE_DICT[wel]['verb']}", str(WELFARE_DICT[wel]["verb"]))
                    )

                for rel in REL_DICT:
                    # For variables that use income variable, equivalence scale and relative poverty lines
                    col_name = f"{var}_{rel}_median_{wel}{ext}"

                    if col_name in cols:
                        # Get the origins of the variable
                        origins = tb_garden[col_name].metadata.origins
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_relative(var, origins, wel, rel, ext)

                        # Replace values in description_short according to `rel`
                        tb_garden[col_name].metadata.description_short = tb_garden[
                            col_name
                        ].metadata.description_short.replace("{povline}", REL_DICT[rel])

                        # Replace values in description_processing according to `rel`
                        tb_garden[col_name].metadata.description_processing = tb_garden[
                            col_name
                        ].metadata.description_processing.replace("{povline}", REL_DICT[rel])

                for pct in PCT_DICT:
                    # For variables that use income variable and percentiles (deciles)
                    col_name = f"{pct}_{var}_{wel}{ext}"

                    if col_name in cols:
                        # Get the origins of the variable
                        origins = tb_garden[col_name].metadata.origins
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_percentiles(var, origins, wel, pct, ext)

                        # Replace values in description_short according to `pct`, depending on `var`
                        if var == "thr":
                            tb_garden[col_name].metadata.description_short = tb_garden[
                                col_name
                            ].metadata.description_short.replace(
                                "{str(PCT_DICT[pct]['thr_number'])}", str(PCT_DICT[pct]["thr_number"])
                            )

                        else:
                            tb_garden[col_name].metadata.description_short = tb_garden[
                                col_name
                            ].metadata.description_short.replace(
                                "{PCT_DICT[pct]['decile10_extra'].lower()}",
                                PCT_DICT[pct]["decile10_extra"].lower(),
                            )

                        # Replace income/wealth words according to `wel`
                        tb_garden[col_name].metadata.description_short = tb_garden[
                            col_name
                        ].metadata.description_short.replace(
                            "{WELFARE_DICT[wel]['verb']}", str(WELFARE_DICT[wel]["verb"])
                        )
                        tb_garden[col_name].metadata.description_short = tb_garden[
                            col_name
                        ].metadata.description_short.replace(
                            "{WELFARE_DICT[wel]['type']}", str(WELFARE_DICT[wel]["type"])
                        )

    return tb_garden


# Metadata functions to show a clearer main code


def var_metadata_income(var, origins, wel, ext) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the variables not depending on deciles
    """
    # Add descriptions depending on welfare variable
    description_welfare_list = add_descriptions_depending_on_welfare(wel)

    # For monetary variables I include the PPP description
    if var == "p0p100_avg" or var == "median":
        meta = VariableMeta(
            title=f"{VAR_DICT[var]['title']} ({WELFARE_DICT[wel]['name']}) ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT[var]["description"],
            description_key=[PPP_DESCRIPTION] + description_welfare_list,
            description_processing=f"""{PROCESSING_DESCRIPTION}

{EXTRAPOLATION_DICT[ext]['description']}""",
            unit=VAR_DICT[var]["unit"],
            short_unit=VAR_DICT[var]["short_unit"],
            origins=origins,
        )

    else:
        meta = VariableMeta(
            title=f"{VAR_DICT[var]['title']} ({WELFARE_DICT[wel]['name']}) ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT[var]["description"],
            description_key=description_welfare_list,
            description_processing=f"""{PROCESSING_DESCRIPTION}

{EXTRAPOLATION_DICT[ext]['description']}""",
            unit=VAR_DICT[var]["unit"],
            short_unit=VAR_DICT[var]["short_unit"],
            origins=origins,
        )

    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": VAR_DICT[var]["numDecimalPlaces"],
        "tolerance": TOLERANCE,
    }

    meta.presentation = VariablePresentationMeta(title_public=meta.title)

    return meta


def var_metadata_income_percentiles(var, origins, wel, pct, ext) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the variables depending on deciles
    """
    # Add descriptions depending on welfare variable
    description_welfare_list = add_descriptions_depending_on_welfare(wel)

    if var == "thr":
        meta = VariableMeta(
            title=f"{PCT_DICT[pct]['decile9']} - {VAR_DICT[var]['title']} ({WELFARE_DICT[wel]['name']}) ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT[var]["description"],
            description_key=[PPP_DESCRIPTION] + description_welfare_list,
            description_processing=f"""{PROCESSING_DESCRIPTION}

{EXTRAPOLATION_DICT[ext]['description']}""",
            unit=VAR_DICT[var]["unit"],
            short_unit=VAR_DICT[var]["short_unit"],
            origins=origins,
        )

    elif var == "avg":
        meta = VariableMeta(
            title=f"{PCT_DICT[pct]['decile10']} - {VAR_DICT[var]['title']} ({WELFARE_DICT[wel]['name']}) ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT[var]["description"],
            description_key=[PPP_DESCRIPTION] + description_welfare_list,
            description_processing=f"""{PROCESSING_DESCRIPTION}

{EXTRAPOLATION_DICT[ext]['description']}""",
            unit=VAR_DICT[var]["unit"],
            short_unit=VAR_DICT[var]["short_unit"],
            origins=origins,
        )

    # Shares do not have PPP description
    else:
        meta = VariableMeta(
            title=f"{PCT_DICT[pct]['decile10']} - {VAR_DICT[var]['title']} ({WELFARE_DICT[wel]['name']}) ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT[var]["description"],
            description_key=description_welfare_list,
            description_processing=f"""{PROCESSING_DESCRIPTION}

{EXTRAPOLATION_DICT[ext]['description']}""",
            unit=VAR_DICT[var]["unit"],
            short_unit=VAR_DICT[var]["short_unit"],
            origins=origins,
        )

    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": VAR_DICT[var]["numDecimalPlaces"],
        "tolerance": TOLERANCE,
    }

    meta.presentation = VariablePresentationMeta(title_public=meta.title)

    return meta


def var_metadata_income_relative(var, origins, wel, rel, ext) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the variables depending on relative poverty lines
    """
    # Add descriptions depending on welfare variable
    description_welfare_list = add_descriptions_depending_on_welfare(wel)

    meta = VariableMeta(
        title=f"{REL_DICT[rel]} - {VAR_DICT[var]['title']} ({WELFARE_DICT[wel]['name']}) ({EXTRAPOLATION_DICT[ext]['title']})",
        description_short=VAR_DICT[var]["description"],
        description_key=description_welfare_list,
        description_processing=f"""{PROCESSING_DESCRIPTION}

{RELATIVE_POVERTY_DESCRIPTION}

{EXTRAPOLATION_DICT[ext]['description']}""",
        unit=VAR_DICT[var]["unit"],
        short_unit=VAR_DICT[var]["short_unit"],
        origins=origins,
    )

    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": VAR_DICT[var]["numDecimalPlaces"],
        "tolerance": TOLERANCE,
    }

    meta.presentation = VariablePresentationMeta(title_public=meta.title)

    return meta


def add_descriptions_depending_on_welfare(wel: str) -> List[str]:
    """
    Add different descriptions depending on the welfare variable (pretax, posttax_dis, posttax_nat, wealth)
    """
    # Add descriptions depending on welfare variable
    if wel == "pretax" or wel == "posttax_dis":
        description_welfare_list = [WELFARE_DICT[wel]["description"]] + METHODOLOGY_DESCRIPTION
    elif wel == "posttax_nat":
        description_welfare_list = (
            [WELFARE_DICT[wel]["description"]] + METHODOLOGY_DESCRIPTION + POST_TAX_NATIONAL_DESCRIPTION
        )
    elif wel == "wealth":
        description_welfare_list = [WELFARE_DICT[wel]["description"]] + METHODOLOGY_DESCRIPTION_WEALTH

    return description_welfare_list


##############################################################################################################
# This is the code for the distribution variables
##############################################################################################################

VAR_DICT_DISTRIBUTION = {
    "avg": {
        "title": "Average",
        "description": "The mean income or wealth per year within each percentile.",
        "unit": f"international-$ in {PPP_YEAR} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "share": {
        "title": "Share",
        "description": "The share of income or wealth received/owned by each percentile.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "thr": {
        "title": "Threshold",
        "description": "The level of income or wealth per year below which 1%, 2%, 3%, ... , 99%, 99.9%, 99.99%, 99.999% of the population falls.",
        "unit": f"international-$ in {PPP_YEAR} prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
}

# Define welfare variables

WELFARE_DEFINITIONS = [
    "Data refers to four types of welfare measures:",
    "`welfare = 'pretax'` is ‘pre-tax’ income — measured before taxes have been paid and most government benefits have been received. It is, however, measured after the operation of pension schemes, both private and public.",
    "`welfare = 'posttax_dis'` is ‘post-tax’ income — measured after taxes have been paid and most government benefits have been received, but does not include in-kind benefits and therefore does not add up to national income.",
    "`welfare = 'posttax_nat'` is ‘post-tax’ income — measured after taxes have been paid and most government benefits have been received.",
    "`welfare = 'wealth'` is net national wealth, which is the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.",
]

PROCESSING_DESCRIPTION_DISTRIBUTIONS = (
    """Estimations are extracted via the [`wid` Stata command](https://github.com/thomasblanchet/wid-stata-tool)."""
)


def add_metadata_vars_distribution(tb_garden: Table) -> Table:
    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in VAR_DICT_DISTRIBUTION:
        for ext in EXTRAPOLATION_DICT:
            # All the variables follow whis structure
            col_name = f"{var}{ext}"

            if col_name in cols:
                # Get the origins of the variable
                origins = tb_garden[col_name].metadata.origins
                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_distribution(var, origins, ext)

    return tb_garden


def var_metadata_distribution(var: str, origins, ext: str) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the distribution variables
    """
    # Shares do not include PPP description
    if var == "share":
        meta = VariableMeta(
            title=f"Income or wealth {VAR_DICT_DISTRIBUTION[var]['title'].lower()} ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT_DISTRIBUTION[var]["description"],
            description_key=WELFARE_DEFINITIONS + METHODOLOGY_DESCRIPTION,
            description_processing=f"{PROCESSING_DESCRIPTION_DISTRIBUTIONS} {EXTRAPOLATION_DICT[ext]['description']}",
            unit=VAR_DICT_DISTRIBUTION[var]["unit"],
            short_unit=VAR_DICT_DISTRIBUTION[var]["short_unit"],
            origins=origins,
        )

    # For monetary variables I include the PPP description
    else:
        meta = VariableMeta(
            title=f"{VAR_DICT_DISTRIBUTION[var]['title']} income or wealth ({EXTRAPOLATION_DICT[ext]['title']})",
            description_short=VAR_DICT_DISTRIBUTION[var]["description"],
            description_key=[PPP_DESCRIPTION] + WELFARE_DEFINITIONS + METHODOLOGY_DESCRIPTION,
            description_processing=f"{PROCESSING_DESCRIPTION_DISTRIBUTIONS} {EXTRAPOLATION_DICT[ext]['description']}",
            unit=VAR_DICT_DISTRIBUTION[var]["unit"],
            short_unit=VAR_DICT_DISTRIBUTION[var]["short_unit"],
            origins=origins,
        )

    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": VAR_DICT_DISTRIBUTION[var]["numDecimalPlaces"],
        "tolerance": TOLERANCE,
    }

    meta.presentation = VariablePresentationMeta(title_public=meta.title)

    return meta
