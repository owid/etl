"""
This function creates the metadata for each variable in the WID dataset, from the dictionaries defined below
If new variables are included in the dataset (from `wid` command in Stata) the dictionaries feeding metadata functions have to be updated (if not an error will show up)
"""

from owid.catalog import Table, VariableMeta

# These are parameters specifically defined for each type of variable
var_dict = {
    "avg": {
        "title": "Average",
        "description": "This is the mean income or wealth within the {pct_dict[pct]['decile10_extra'].lower()}.",
        "unit": "international-$ in 2021 prices",
        "short_unit": "$",
        "numDecimalPlaces": 1,
    },
    "share": {
        "title": "Share",
        "description": "This is the income or wealth of the {pct_dict[pct]['decile10_extra'].lower()} as a share of total income or wealth.",
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
        "description": "This is the level of income or wealth per year below which {str(pct_dict[pct]['thr_number'])}% of the population falls.",
        "unit": "international-$ in 2021 prices",
        "short_unit": "$",
        "numDecimalPlaces": 1,
    },
    "p0p100_gini": {
        "title": "Gini coefficient",
        "description": "The Gini coefficient is a measure of the inequality of the income or wealth distribution in a population. Higher values indicate a higher level of inequality.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 3,
    },
    "p0p100_avg": {
        "title": "Mean",
        "description": "Mean income or wealth.",
        "unit": "international-$ in 2021 prices",
        "short_unit": "$",
        "numDecimalPlaces": 1,
    },
    "median": {
        "title": "Median",
        "description": "Median income or wealth.",
        "unit": "international-$ in 2021 prices",
        "short_unit": "$",
        "numDecimalPlaces": 1,
    },
    "palma_ratio": {
        "title": "Palma ratio",
        "description": "The Palma ratio is the share of total income or wealth of the top 10% divided by the share of the bottom 40%.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 1,
    },
    "s80_s20_ratio": {
        "title": "S80/S20 ratio",
        "description": "The S80/S20 ratio is the share of total income or wealth of the top 20% divided by the share of the bottom 20%.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p10_ratio": {
        "title": "P90/P10 ratio",
        "description": "P90 and P10 are the levels of income or wealth below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p90_p50_ratio": {
        "title": "P90/P50 ratio",
        "description": "The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median income or wealth.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
    "p50_p10_ratio": {
        "title": "P50/P10 ratio",
        "description": "The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median income or wealth is two times higher than that of someone just falling in the poorest tenth of the population.",
        "unit": "",
        "short_unit": "",
        "numDecimalPlaces": 2,
    },
}

# Details for each consumption or income variable
inc_cons_dict = {
    "pretax": {
        "name": "Pretax",
        "description": "This measure is related to <b>pretax national income</b>, which is income before the payment and receipt of taxes and benefits, but after payment of public and private pensions.",
    },
    "posttax_dis": {
        "name": "Post-tax disposable",
        "description": "This measure is related to <b>post-tax disposable income</b>, which includes all cash redistribution through the tax and transfer system, but does not include in-kind benefits and therefore does not add up to national income.",
    },
    "posttax_nat": {
        "name": "Post-tax national",
        "description": "This measure is related to <b>post-tax national income</b>. which includes all cash redistribution through the tax and transfer system and also all in-kind transfers (i.e., government consumption expenditures) to individuals.",
    },
    "wealth": {
        "name": "Net national wealth",
        "description": "This measure is related to <b>net national wealth</b>, which is the total value of non-financial and financial assets (housing, land, deposits, bonds, equities, etc.) held by households, minus their debts.",
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
    "p99p100": {"decile10": "Top 1%", "decile9": "Top 1%", "thr_number": 99, "decile10_extra": "Top 1%"},
    "p99_9p100": {"decile10": "Top 0.1%", "decile9": "Top 0.1%", "thr_number": 99.9, "decile10_extra": "Top 0.1%"},
    "p99_99p100": {
        "decile10": "Top 0.01%",
        "decile9": "Top 0.01%",
        "thr_number": 99.99,
        "decile10_extra": "Top 0.01%",
    },
    "p99_999p100": {
        "decile10": "Top 0.001%",
        "decile9": "Top 0.001%",
        "thr_number": 99.999,
        "decile10_extra": "Top 0.001%",
    },
    "p0p50": {"decile10": "Bottom 50%", "decile9": "Bottom 50%", "thr_number": "", "decile10_extra": "Bottom 50%"},
}


def add_metadata_vars(tb_garden: Table) -> Table:

    # Get a list of all the variables available
    cols = list(tb_garden.columns)

    for var in var_dict:
        for wel in inc_cons_dict:

            # For variables that use income variable
            col_name = f"{var}_{wel}"

            if col_name in cols:

                # Create metadata for these variables
                tb_garden[col_name].metadata = var_metadata_income(var, wel)

            for pct in pct_dict:

                # For variables that use income variable and percentiles (deciles)
                col_name = f"{pct}_{var}_{wel}"

                if col_name in cols:

                    # Create metadata for these variables
                    tb_garden[col_name].metadata = var_metadata_income_percentiles(var, wel, pct)

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

    return tb_garden


# Metadata functions to show a clearer main code
def var_metadata_income(var, wel) -> VariableMeta:
    meta = VariableMeta(
        title=f"{var_dict[var]['title']} ({inc_cons_dict[wel]['name']})",
        description=f"{var_dict[var]['description']}\n\n{inc_cons_dict[wel]['description']}",
        unit=var_dict[var]["unit"],
        short_unit=var_dict[var]["short_unit"],
    )
    meta.display = {
        "name": meta.title,
        "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
    }
    return meta


def var_metadata_income_percentiles(var, wel, pct) -> VariableMeta:
    if var == "thr":

        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile9']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']})",
            description=f"{var_dict[var]['description']}\n\n{inc_cons_dict[wel]['description']}",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }

    else:
        meta = VariableMeta(
            title=f"{pct_dict[pct]['decile10']} - {var_dict[var]['title']} ({inc_cons_dict[wel]['name']})",
            description=f"{var_dict[var]['description']}\n\n{inc_cons_dict[wel]['description']}",
            unit=var_dict[var]["unit"],
            short_unit=var_dict[var]["short_unit"],
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": var_dict[var]["numDecimalPlaces"],
        }
    return meta
