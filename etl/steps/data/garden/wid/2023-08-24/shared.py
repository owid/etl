"""
This function creates the metadata for each variable in the WID dataset, from the dictionaries defined below
If new variables are included in the dataset (from `wid` command in Stata) the dictionaries feeding metadata functions have to be updated (if not an error will show up)
"""

from owid.catalog import Table, VariableMeta

# This is text common to all variables

additional_description = """
The data is estimated from a combination of household surveys, tax records and national accounts data. This combination can provide a more accurate picture of the incomes of the richest, which tend to be captured poorly in household survey data alone.
These underlying data sources are not always available. For some countries, observations are extrapolated from data relating to other years, or are sometimes modeled based on data observed in other countries.
"""

relative_poverty_descritption = """
This data has been estimated by calculating the {povline}, and then checking that value against the closest threshold in the percentile distribution. The headcount ratio is then the percentile, the share of the population below that threshold.
"""


# NOTE: Change the year when needed
ppp_description = "The data is measured in international-$ at 2022 prices – this adjusts for inflation and for differences in the cost of living between countries."

# These are parameters specifically defined for each type of variable
var_dict = {
    "avg": {
        "title": "Average",
        "description": "The mean {inc_cons_dict[wel]['type']} per year within the {pct_dict[pct]['decile10_extra'].lower()}.",
        "unit": "international-$ in 2022 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "share": {
        "title": "Share",
        "description": "The share of {inc_cons_dict[wel]['type']} {inc_cons_dict[wel]['verb']} by the {pct_dict[pct]['decile10_extra'].lower()}.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "p50p90_share": {
        "title": "Middle 40% - Share",
        "description": "This is the income or wealth of the middle 40% as a share of total income or wealth. The middle 40% is the share of the population whose income or consumption lies between the poorest 50% and the richest 10%.",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 1,
    },
    "thr": {
        "title": "Threshold",
        "description": "The level of {inc_cons_dict[wel]['type']} per year below which {str(pct_dict[pct]['thr_number'])}% of the population falls.",
        "unit": "international-$ in 2022 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "p0p100_gini": {
        "title": "Gini coefficient",
        "description": "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p0p100_avg": {
        "title": "Mean",
        "description": "Mean {inc_cons_dict[wel]['type']}.",
        "unit": "international-$ in 2022 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
    "median": {
        "title": "Median",
        "description": "Median {inc_cons_dict[wel]['type']}.",
        "unit": "international-$ in 2022 prices",
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
        "description": "The S80/S20 ratio is the share of total {inc_cons_dict[wel]['type']} of the top 20% divided by the share of the bottom 20%.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p10_ratio": {
        "title": "P90/P10 ratio",
        "description": "P90 and P10 are the levels of {inc_cons_dict[wel]['type']} below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p50_ratio": {
        "title": "P90/P50 ratio",
        "description": "The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median {inc_cons_dict[wel]['type']}.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p50_p10_ratio": {
        "title": "P50/P10 ratio",
        "description": "The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median {inc_cons_dict[wel]['type']} is two times higher than that of someone just falling in the poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "headcount_ratio": {
        "title": "Share of population in poverty",
        "description": "Share of the population living below the poverty line of {povline}",
        "unit": "%",
        "short_unit": "%",
        "numDecimalPlaces": 0,
    },
}

# Details for each consumption or income variable
inc_cons_dict = {
    "pretax": {
        "name": "Pretax",
        "type": "income",
        "verb": "received",
        "description": "Income is ‘pre-tax’ — measured before taxes have been paid and most government benefits have been received. It is, however, measured after the operation of pension schemes, both private and public.",
    },
    "posttax_dis": {
        "name": "Post-tax disposable",
        "type": "income",
        "verb": "received",
        "description": "Income is ‘post-tax’ — measured after taxes have been paid and most government benefits have been received, but does not include in-kind benefits and therefore does not add up to national income.",
    },
    "posttax_nat": {
        "name": "Post-tax national",
        "type": "income",
        "verb": "received",
        "description": "Income is ‘post-tax’ — measured after taxes have been paid and most government benefits have been received.",
    },
    "wealth": {
        "name": "Net national wealth",
        "type": "wealth",
        "verb": "owned",
        "description": "This measure is related to net national wealth, which is the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.",
    },
}

# Details for naming each decile/percentile
pct_dict = {
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
rel_dict = {40: "40% of the median", 50: "50% of the median", 60: "60% of the median"}

# Details for extrapolations or estimations
extrapolation_dict = {"": "Estimated", "_extrapolated": "Extrapolated"}


def add_metadata_vars(tb_garden: Table) -> Table:
    """
    This function adds metadata to all the variables in the WID dataset
    """
    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in var_dict:
        for wel in inc_cons_dict:
            for ext in extrapolation_dict:
                # For variables that use income variable
                col_name = f"{var}_{wel}{ext}"

                if col_name in cols:
                    # Create metadata for these variables
                    tb_garden[col_name].metadata = var_metadata_income(var, wel, ext)
                    # Replace income/wealth words according to `wel`
                    tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                        "{inc_cons_dict[wel]['type']}", str(inc_cons_dict[wel]["type"])
                    )

                for rel in rel_dict:
                    # For variables that use income variable, equivalence scale and relative poverty lines
                    col_name = f"{var}_{rel}_median_{wel}{ext}"

                    if col_name in cols:
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_relative(var, wel, rel, ext)

                        # Replace values in description according to `rel`
                        tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                            "{povline}", rel_dict[rel]
                        )

                for pct in pct_dict:
                    # For variables that use income variable and percentiles (deciles)
                    col_name = f"{pct}_{var}_{wel}{ext}"

                    if col_name in cols:
                        # Create metadata for these variables
                        tb_garden[col_name].metadata = var_metadata_income_percentiles(var, wel, pct, ext)

                        # Replace values in description according to `pct`, depending on `var`
                        if var == "thr":
                            tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                                "{str(pct_dict[pct]['thr_number'])}", str(pct_dict[pct]["thr_number"])
                            )

                        else:
                            tb_garden[col_name].metadata.description = tb_garden[col_name].metadata.description.replace(
                                "{pct_dict[pct]['decile10_extra'].lower()}",
                                pct_dict[pct]["decile10_extra"].lower(),
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
def var_metadata_income(var, wel, ext) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the variables not depending on deciles
    """
    # For monetary variables I include the PPP description
    if var == "p0p100_avg" or var == "median":
        meta = VariableMeta(
            title=f"{var_dict[var]['title']} ({inc_cons_dict[wel]['name']}) ({extrapolation_dict[ext]})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {ppp_description}

            {additional_description}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }

    else:
        meta = VariableMeta(
            title=f"{var_dict[var]['title']} ({inc_cons_dict[wel]['name']}) ({extrapolation_dict[ext]})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {additional_description}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }

    return meta


def var_metadata_income_percentiles(var, wel, pct, ext) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the variables depending on deciles
    """
    if var == "thr":
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile9']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}) ({extrapolation_dict[ext]})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {ppp_description}

            {additional_description}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }

    elif var == "avg":
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile10']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}) ({extrapolation_dict[ext]})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {ppp_description}

            {additional_description}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }

    # Shares do not have PPP description
    else:
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile10']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}) ({extrapolation_dict[ext]})",
            description=f"""{var_dict[var]['description']}

            {inc_cons_dict[wel]['description']}

            {additional_description}""",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }
    return meta


def var_metadata_income_relative(var, wel, rel, ext) -> VariableMeta:
    meta = VariableMeta(
        title=f"{rel_dict[rel]} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']}) ({extrapolation_dict[ext]})",
        description=f"""{var_dict[var]['description']}

        {inc_cons_dict[wel]['description']}

        {additional_description}

        {relative_poverty_descritption}""",
        unit=var_dict[var]["unit"],
        short_unit=var_dict[var]["short_unit"],
    )

    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
    }

    return meta


##############################################################################################################
# This is the code for the distribution variables
##############################################################################################################

var_dict_distribution = {
    "avg": {
        "title": "Average",
        "description": "The mean income or wealth per year within each percentile.",
        "unit": "international-$ in 2022 prices",
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
        "unit": "international-$ in 2022 prices",
        "short_unit": "$",
        "numDecimalPlaces": 0,
    },
}

# Define welfare variables

welfare_definitions = """Data refers to four types of welfare measures:

- `welfare = "pretax"` is ‘pre-tax’ income — measured before taxes have been paid and most government benefits have been received. It is, however, measured after the operation of pension schemes, both private and public.

- `welfare = "posttax_dis"` is ‘post-tax’ income — measured after taxes have been paid and most government benefits have been received, but does not include in-kind benefits and therefore does not add up to national income.

- `welfare = "posttax_nat"` is ‘post-tax’ income — measured after taxes have been paid and most government benefits have been received.

- `welfare = "wealth"` is net national wealth, which is the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.
"""


def add_metadata_vars_distribution(tb_garden: Table) -> Table:
    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in var_dict_distribution:
        for ext in extrapolation_dict:
            # All the variables follow whis structure
            col_name = f"{var}{ext}"

            if col_name in cols:
                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_distribution(var, ext)

    return tb_garden


def var_metadata_distribution(var: str, ext: str) -> VariableMeta:
    """
    This function assigns each of the metadata fields for the distribution variables
    """
    # Shares do not include PPP description
    if var == "share":
        meta = VariableMeta(
            title=f"Income or wealth {var_dict_distribution[var]['title'].lower()} ({extrapolation_dict[ext]})",
            description=f"""{var_dict_distribution[var]['description']}

            {welfare_definitions}

            {additional_description}""",
            unit=var_dict_distribution[var]["unit"],
            short_unit=var_dict_distribution[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict_distribution[var]["numDecimalPlaces"],
        }

    # For monetary variables I include the PPP description
    else:
        meta = VariableMeta(
            title=f"{var_dict_distribution[var]['title']} income or wealth ({extrapolation_dict[ext]})",
            description=f"""{var_dict_distribution[var]['description']}

            {welfare_definitions}

            {ppp_description}

            {additional_description}""",
            unit=var_dict_distribution[var]["unit"],
            short_unit=var_dict_distribution[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict_distribution[var]["numDecimalPlaces"],
        }

    return meta
