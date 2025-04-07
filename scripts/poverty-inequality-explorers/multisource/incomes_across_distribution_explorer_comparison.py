# # Incomes Across the Distribution Explorer - Source Comparison
# This code creates the tsv file for the incomes across the distribution comparison explorer, available [here](https://owid.cloud/admin/explorers/preview/incomes-across-distribution-comparison)

import numpy as np
import pandas as pd

from ..common_parameters import *

# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each type of welfare measure or tables considered.

# MULTI-SOURCE
# Read Google sheets
sheet_id = "1wcFsNZCEn_6SJ05BFkXKLUyvCrnigfR8eeemGKgAYsI"

# Merged sheet (this contains PIP, WID and LIS dataset information together in one file)
sheet_name = "merged_tables"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
merged_tables = pd.read_csv(url, keep_default_na=False)

# Source checkbox covers all the possible combinations to get for the multi-source selector
sheet_name = "source_checkbox"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
source_checkbox = pd.read_csv(url, keep_default_na=False, dtype={"pip": "str", "wid": "str", "lis": "str"})

# Only get the combinations where all the sources are available (pre and post tax)
source_checkbox = source_checkbox[
    (
        (source_checkbox["type"] == "pre")
        & (source_checkbox["wid"] == "true")
        & (source_checkbox["pip"] == "false")
        & (source_checkbox["lis"] == "true")
    )
    | (
        (source_checkbox["type"] == "post")
        & (source_checkbox["wid"] == "true")
        & (source_checkbox["pip"] == "true")
        & (source_checkbox["lis"] == "true")
    )
].reset_index(drop=True)

# Deciles9 sheet (needed to handle thresholds data)
sheet_name = "deciles9"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
deciles9 = pd.read_csv(url, keep_default_na=False, dtype={"dropdown": "str", "decile": "str"})

# Deciles10 sheet (needed to handle average and share data)
sheet_name = "deciles10"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
deciles10 = pd.read_csv(url, keep_default_na=False, dtype={"dropdown": "str", "decile": "str"})

# LUXEMBOURG INCOME STUDY
# Read Google sheets
sheet_id = "1UFdwB1iBpP2tEP6GtxCHvW1GGhjsFflh42FWR80rYIg"

# Welfare type sheet
sheet_name = "welfare"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_welfare = pd.read_csv(url, keep_default_na=False)

# Equivalence scales
sheet_name = "equivalence_scales"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_equivalence_scales = pd.read_csv(url, keep_default_na=False)

# Relative poverty sheet
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_povlines_rel = pd.read_csv(url)

# Deciles9 sheet (needed to handle thresholds data)
sheet_name = "deciles9"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_deciles9 = pd.read_csv(url, keep_default_na=False)

# Deciles10 sheet (needed to handle average and share data)
sheet_name = "deciles10"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_deciles10 = pd.read_csv(url, keep_default_na=False)

# Income aggregation sheet (day, month, year)
sheet_name = "income_aggregation"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_income_aggregation = pd.read_csv(url, keep_default_na=False, dtype={"multiplier": "str"})

# WORLD INEQUALITY DATABASE
# Read Google sheets
sheet_id = "18T5IGnpyJwb8KL9USYvME6IaLEcYIo26ioHCpkDnwRQ"

# Welfare type sheet
sheet_name = "welfare"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_welfare = pd.read_csv(url, keep_default_na=False)

# Deciles9 sheet (needed to handle thresholds data)
sheet_name = "deciles9"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_deciles9 = pd.read_csv(url, keep_default_na=False)

# Deciles10 sheet (needed to handle average and share data)
sheet_name = "deciles10"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_deciles10 = pd.read_csv(url, keep_default_na=False)

# Income aggregation sheet (day, month, year)
sheet_name = "income_aggregation"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_income_aggregation = pd.read_csv(url, keep_default_na=False, dtype={"multiplier": "str"})

# WORLD BANK POVERTY AND INEQUALITY PLATFORM
# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Survey type sheet
sheet_name = "table"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_tables = pd.read_csv(url)

# Settings for 10 deciles variables (share, avg) sheet
sheet_name = "deciles10"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_deciles10 = pd.read_csv(url, dtype={"dropdown": "str", "decile": "str"})

# Settings for 9 deciles variables (thr) sheet
sheet_name = "deciles9"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_deciles9 = pd.read_csv(url, dtype={"dropdown": "str", "decile": "str"})

# Income aggregation sheet (day, month, year)
sheet_name = "income_aggregation"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_income_aggregation = pd.read_csv(url, keep_default_na=False, dtype={"multiplier": "str"})

# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Incomes Across the Distribution - World Bank, WID, and LIS",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Compare World Bank, WID, and LIS data on the distribution of incomes.",
    "isPublished": "true",
    "googleSheet": "",
    "wpBlockId": "57742",
    "entityType": "country or region",
    "pickerColumnSlugs": "mean_year median_year p0p100_avg_posttax_nat_year median_posttax_nat_year mean_dhi_pc_year median_dhi_pc_year",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# ## Tables
# Variables are grouped by type of welfare to iterate by different survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.
# ### Tables for variables not showing breaks between surveys
# These variables consider a continous series, without breaks due to changes in surveys' methodology


###########################################################################################
# WORLD BANK POVERTY AND INEQUALITY PLATFORM
###########################################################################################
sourceName = SOURCE_NAME_PIP
dataPublishedBy = DATA_PUBLISHED_BY_PIP
sourceLink = SOURCE_LINK_PIP
colorScaleNumericMinValue = COLOR_SCALE_NUMERIC_MIN_VALUE
tolerance = TOLERANCE
colorScaleEqualSizeBins = COLOR_SCALE_EQUAL_SIZEBINS
tableSlug = "poverty_inequality"
new_line = NEW_LINE

additional_description = ADDITIONAL_DESCRIPTION_PIP_COMPARISON

notes_title = NOTES_TITLE_PIP

processing_description = PROCESSING_DESCRIPTION_PIP_INCOMES_ACROSS_DISTRIBUTION
ppp_description = PPP_DESCRIPTION_PIP_2017

# Table generation
df_tables_pip = pd.DataFrame()
j = 0

for tab in range(len(pip_tables)):
    # Define country as entityName
    df_tables_pip.loc[j, "name"] = "Country"
    df_tables_pip.loc[j, "slug"] = "country"
    df_tables_pip.loc[j, "type"] = "EntityName"
    j += 1

    # Define year as Year
    df_tables_pip.loc[j, "name"] = "Year"
    df_tables_pip.loc[j, "slug"] = "year"
    df_tables_pip.loc[j, "type"] = "Year"
    j += 1

    # I need the original variables to not break the aggregations
    # mean
    df_tables_pip.loc[j, "name"] = f"Mean {pip_tables.text[tab]} (PIP data)"
    df_tables_pip.loc[j, "slug"] = "mean"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            f"Mean {pip_tables.text[tab]}.",
            additional_description,
            ppp_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
    df_tables_pip.loc[j, "shortUnit"] = "$"
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100;100.0001"
    df_tables_pip.loc[j, "colorScaleScheme"] = "BuGn"
    j += 1

    # median
    df_tables_pip.loc[j, "name"] = f"Median {pip_tables.text[tab]} (PIP data)"
    df_tables_pip.loc[j, "slug"] = "median"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            f"The level of {pip_tables.text[tab]} per day below which half of the population falls.",
            additional_description,
            ppp_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
    df_tables_pip.loc[j, "shortUnit"] = "$"
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100;100.0001"
    df_tables_pip.loc[j, "colorScaleScheme"] = "Blues"
    j += 1

    for dec9 in range(len(pip_deciles9)):
        # thresholds
        df_tables_pip.loc[j, "name"] = f"{pip_deciles9.ordinal[dec9].capitalize()} (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"decile{pip_deciles9.decile[dec9]}_thr"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The level of {pip_tables.text[tab]} per day below which {pip_deciles9.decile[dec9]}0% of the population falls.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100;100.0001"
        df_tables_pip.loc[j, "colorScaleScheme"] = "Purples"
        j += 1

    for dec10 in range(len(pip_deciles10)):
        # averages
        df_tables_pip.loc[j, "name"] = f"{pip_deciles10.ordinal[dec10].capitalize()} (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"decile{pip_deciles10.decile[dec10]}_avg"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The mean {pip_tables.text[tab]} per day within the {pip_deciles10.ordinal[dec10]} (tenth of the population).",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100;100.0001"
        df_tables_pip.loc[j, "colorScaleScheme"] = "Greens"
        j += 1

    for dec10 in range(len(pip_deciles10)):
        # shares
        df_tables_pip.loc[j, "name"] = f"{pip_deciles10.ordinal[dec10].capitalize()} (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"decile{pip_deciles10.decile[dec10]}_share"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The share of {pip_tables.text[tab]} received by the {pip_deciles10.ordinal[dec10]} (tenth of the population).",
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_deciles10.scale_share[dec10]
        df_tables_pip.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

    # Daily, monthly, annual aggregations
    for agg in range(len(pip_income_aggregation)):
        # mean
        df_tables_pip.loc[j, "name"] = f"Mean {pip_tables.text[tab]} (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"mean{pip_income_aggregation.slug_suffix[agg]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The mean level of {pip_tables.text[tab]} per {pip_income_aggregation.aggregation[agg]}.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_income_aggregation.scale[agg]
        df_tables_pip.loc[j, "colorScaleScheme"] = "BuGn"
        df_tables_pip.loc[j, "transform"] = f"multiplyBy mean {pip_income_aggregation.multiplier[agg]}"
        j += 1

        # median
        df_tables_pip.loc[j, "name"] = f"Median {pip_tables.text[tab]} (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"median{pip_income_aggregation.slug_suffix[agg]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The level of {pip_tables.text[tab]} per {pip_income_aggregation.aggregation[agg]} below which half of the population falls.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_income_aggregation.scale[agg]
        df_tables_pip.loc[j, "colorScaleScheme"] = "Blues"
        df_tables_pip.loc[j, "transform"] = f"multiplyBy median {pip_income_aggregation.multiplier[agg]}"
        j += 1

        for dec9 in range(len(pip_deciles9)):
            # thresholds
            df_tables_pip.loc[j, "name"] = f"{pip_deciles9.ordinal[dec9].capitalize()} (PIP data)"
            df_tables_pip.loc[j, "slug"] = (
                f"decile{pip_deciles9.decile[dec9]}_thr{pip_income_aggregation.slug_suffix[agg]}"
            )
            df_tables_pip.loc[j, "description"] = new_line.join(
                [
                    f"The level of {pip_tables.text[tab]} per {pip_income_aggregation.aggregation[agg]} below which {pip_deciles9.decile[dec9]}0% of the population falls.",
                    additional_description,
                    ppp_description,
                    notes_title,
                    processing_description,
                ]
            )
            df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
            df_tables_pip.loc[j, "shortUnit"] = "$"
            df_tables_pip.loc[j, "type"] = "Numeric"
            df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_income_aggregation.scale[agg]
            df_tables_pip.loc[j, "colorScaleScheme"] = "Purples"
            df_tables_pip.loc[j, "transform"] = (
                f"multiplyBy decile{pip_deciles9.decile[dec9]}_thr {pip_income_aggregation.multiplier[agg]}"
            )
            j += 1

        for dec10 in range(len(pip_deciles10)):
            # averages
            df_tables_pip.loc[j, "name"] = f"{pip_deciles10.ordinal[dec10].capitalize()} (PIP data)"
            df_tables_pip.loc[j, "slug"] = (
                f"decile{pip_deciles10.decile[dec10]}_avg{pip_income_aggregation.slug_suffix[agg]}"
            )
            df_tables_pip.loc[j, "description"] = new_line.join(
                [
                    f"The mean {pip_tables.text[tab]} per {pip_income_aggregation.aggregation[agg]} within the {pip_deciles10.ordinal[dec10]} (tenth of the population).",
                    additional_description,
                    ppp_description,
                    notes_title,
                    processing_description,
                ]
            )
            df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
            df_tables_pip.loc[j, "shortUnit"] = "$"
            df_tables_pip.loc[j, "type"] = "Numeric"
            df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_income_aggregation.scale[agg]
            df_tables_pip.loc[j, "colorScaleScheme"] = "Greens"
            df_tables_pip.loc[j, "transform"] = (
                f"multiplyBy decile{pip_deciles10.decile[dec10]}_avg {pip_income_aggregation.multiplier[agg]}"
            )
            j += 1

df_tables_pip["tableSlug"] = tableSlug
df_tables_pip["sourceName"] = sourceName
df_tables_pip["dataPublishedBy"] = dataPublishedBy
df_tables_pip["sourceLink"] = sourceLink
df_tables_pip["colorScaleNumericMinValue"] = colorScaleNumericMinValue
df_tables_pip["tolerance"] = tolerance
df_tables_pip["colorScaleEqualSizeBins"] = colorScaleEqualSizeBins

###########################################################################################
# WORLD INEQUALITY DATABASE (WID)
###########################################################################################

# Table generation

sourceName = SOURCE_NAME_WID
dataPublishedBy = DATA_PUBLISHED_BY_WID
sourceLink = SOURCE_LINK_WID
colorScaleNumericMinValue = COLOR_SCALE_NUMERIC_MIN_VALUE
tolerance = TOLERANCE
colorScaleEqualSizeBins = COLOR_SCALE_EQUAL_SIZEBINS
new_line = NEW_LINE

additional_description = ADDITIONAL_DESCRIPTION_WID
ppp_description = PPP_DESCRIPTION_WID

df_tables_wid = pd.DataFrame()
j = 0

for tab in range(len(merged_tables)):
    for wel in range(len(wid_welfare)):
        # Define additional description depending on the welfare type
        if wel == 0:
            additional_description = ADDITIONAL_DESCRIPTION_WID_POST_TAX
        else:
            additional_description = ADDITIONAL_DESCRIPTION_WID

        # I need the original variables to not break the aggregations
        # Mean
        df_tables_wid.loc[j, "name"] = f"Mean {wid_welfare['welfare_type'][wel]} (WID data)"
        df_tables_wid.loc[j, "slug"] = f"p0p100_avg_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                f"Mean {wid_welfare['welfare_type'][wel]}",
                wid_welfare["description"][wel],
                ppp_description,
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
        df_tables_wid.loc[j, "shortUnit"] = "$"
        df_tables_wid.loc[j, "type"] = "Numeric"
        # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_mean"][wel]
        df_tables_wid.loc[j, "colorScaleScheme"] = "BuGn"
        j += 1

        # Median
        df_tables_wid.loc[j, "name"] = f"Median {wid_welfare['welfare_type'][wel]} (WID data)"
        df_tables_wid.loc[j, "slug"] = f"median_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                f"This is the level of {wid_welfare['welfare_type'][wel]} below which half of the population falls.",
                wid_welfare["description"][wel],
                ppp_description,
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
        df_tables_wid.loc[j, "shortUnit"] = "$"
        df_tables_wid.loc[j, "type"] = "Numeric"
        # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_median"][wel]
        df_tables_wid.loc[j, "colorScaleScheme"] = "Blues"
        j += 1

        # Thresholds - Deciles
        for dec9 in range(len(wid_deciles9)):
            df_tables_wid.loc[j, "name"] = f"{wid_deciles9['ordinal'][dec9].capitalize()} (WID data)"
            df_tables_wid.loc[j, "slug"] = f"{wid_deciles9['wid_notation'][dec9]}_thr_{wid_welfare['slug'][wel]}"
            df_tables_wid.loc[j, "description"] = new_line.join(
                [
                    f"The level of {wid_welfare['welfare_type'][wel]} below which {wid_deciles9['decile'][dec9]}0% of the population falls.",
                    wid_welfare["description"][wel],
                    ppp_description,
                    additional_description,
                ]
            )
            df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
            df_tables_wid.loc[j, "shortUnit"] = "$"
            df_tables_wid.loc[j, "type"] = "Numeric"
            # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_deciles9["scale_thr"][
            #     dec9
            # ]
            df_tables_wid.loc[j, "colorScaleScheme"] = "Purples"
            j += 1

        # Averages - Deciles
        for dec10 in range(len(wid_deciles10)):
            df_tables_wid.loc[j, "name"] = f"{wid_deciles10['ordinal'][dec10].capitalize()} (WID data)"
            df_tables_wid.loc[j, "slug"] = f"{wid_deciles10['wid_notation'][dec10]}_avg_{wid_welfare['slug'][wel]}"
            df_tables_wid.loc[j, "description"] = new_line.join(
                [
                    f"The mean {wid_welfare['welfare_type'][wel]} within the {wid_deciles10['ordinal'][dec10]} (tenth of the population).",
                    wid_welfare["description"][wel],
                    ppp_description,
                    additional_description,
                ]
            )
            df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
            df_tables_wid.loc[j, "shortUnit"] = "$"
            df_tables_wid.loc[j, "type"] = "Numeric"
            # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_deciles10["scale_avg"][
            #     dec10
            # ]
            df_tables_wid.loc[j, "colorScaleScheme"] = "Greens"
            j += 1

        # Shares - Deciles
        for dec10 in range(len(wid_deciles10)):
            df_tables_wid.loc[j, "name"] = f"{wid_deciles10['ordinal'][dec10].capitalize()} (WID data)"
            df_tables_wid.loc[j, "slug"] = f"{wid_deciles10['wid_notation'][dec10]}_share_{wid_welfare['slug'][wel]}"
            df_tables_wid.loc[j, "description"] = new_line.join(
                [
                    f"The share of {wid_welfare['welfare_type'][wel]} received by the {wid_deciles10['ordinal'][dec10]} (tenth of the population).",
                    wid_welfare["description"][wel],
                    additional_description,
                ]
            )
            df_tables_wid.loc[j, "unit"] = "%"
            df_tables_wid.loc[j, "shortUnit"] = "%"
            df_tables_wid.loc[j, "type"] = "Numeric"
            # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_deciles10[
            #     "scale_share"
            # ][dec10]
            df_tables_wid.loc[j, "colorScaleScheme"] = "OrRd"
            j += 1

        # Daily, monthly, annual aggregations
        for agg in range(len(wid_income_aggregation)):
            # Mean
            df_tables_wid.loc[j, "name"] = f"Mean {wid_welfare['welfare_type'][wel]} (WID data)"
            df_tables_wid.loc[j, "slug"] = (
                f"p0p100_avg_{wid_welfare['slug'][wel]}{wid_income_aggregation['slug_suffix'][agg]}"
            )
            df_tables_wid.loc[j, "description"] = new_line.join(
                [
                    f"Mean {wid_welfare['welfare_type'][wel]}.",
                    wid_welfare["description"][wel],
                    ppp_description,
                    additional_description,
                ]
            )
            df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
            df_tables_wid.loc[j, "shortUnit"] = "$"
            df_tables_wid.loc[j, "type"] = "Numeric"
            # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_mean"][
            #     wel
            # ]
            df_tables_wid.loc[j, "colorScaleScheme"] = "BuGn"
            df_tables_wid.loc[j, "transform"] = (
                f"multiplyBy p0p100_avg_{wid_welfare['slug'][wel]} {wid_income_aggregation['multiplier'][agg]}"
            )
            j += 1

            # Median
            df_tables_wid.loc[j, "name"] = f"Median {wid_welfare['welfare_type'][wel]} (WID data)"
            df_tables_wid.loc[j, "slug"] = (
                f"median_{wid_welfare['slug'][wel]}{wid_income_aggregation['slug_suffix'][agg]}"
            )
            df_tables_wid.loc[j, "description"] = new_line.join(
                [
                    f"This is the level of {wid_welfare['welfare_type'][wel]} below which 50% of the population falls.",
                    wid_welfare["description"][wel],
                    ppp_description,
                    additional_description,
                ]
            )
            df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
            df_tables_wid.loc[j, "shortUnit"] = "$"
            df_tables_wid.loc[j, "type"] = "Numeric"
            # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_median"][
            #     wel
            # ]
            df_tables_wid.loc[j, "colorScaleScheme"] = "Blues"
            df_tables_wid.loc[j, "transform"] = (
                f"multiplyBy median_{wid_welfare['slug'][wel]} {wid_income_aggregation['multiplier'][agg]}"
            )
            j += 1

            # Thresholds - Deciles
            for dec9 in range(len(wid_deciles9)):
                df_tables_wid.loc[j, "name"] = f"{wid_deciles9['ordinal'][dec9].capitalize()} (WID data)"
                df_tables_wid.loc[j, "slug"] = (
                    f"{wid_deciles9['wid_notation'][dec9]}_thr_{wid_welfare['slug'][wel]}{wid_income_aggregation['slug_suffix'][agg]}"
                )
                df_tables_wid.loc[j, "description"] = new_line.join(
                    [
                        f"The level of {wid_welfare['welfare_type'][wel]} below which {wid_deciles9['decile'][dec9]}0% of the population falls.",
                        wid_welfare["description"][wel],
                        ppp_description,
                        additional_description,
                    ]
                )
                df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
                df_tables_wid.loc[j, "shortUnit"] = "$"
                df_tables_wid.loc[j, "type"] = "Numeric"
                # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_deciles9[
                #     "scale_thr"
                # ][dec9]
                df_tables_wid.loc[j, "colorScaleScheme"] = "Purples"
                df_tables_wid.loc[j, "transform"] = (
                    f"multiplyBy {wid_deciles9['wid_notation'][dec9]}_thr_{wid_welfare['slug'][wel]} {wid_income_aggregation['multiplier'][agg]}"
                )
                j += 1

            # Averages - Deciles
            for dec10 in range(len(wid_deciles10)):
                df_tables_wid.loc[j, "name"] = f"{wid_deciles10['ordinal'][dec10].capitalize()} (WID data)"
                df_tables_wid.loc[j, "slug"] = (
                    f"{wid_deciles10['wid_notation'][dec10]}_avg_{wid_welfare['slug'][wel]}{wid_income_aggregation['slug_suffix'][agg]}"
                )
                df_tables_wid.loc[j, "description"] = new_line.join(
                    [
                        f"The mean {wid_welfare['welfare_type'][wel]} within the {wid_deciles10['ordinal'][dec10]} (tenth of the population).",
                        wid_welfare["description"][wel],
                        ppp_description,
                        additional_description,
                    ]
                )
                df_tables_wid.loc[j, "unit"] = f"international-$ in {PPP_YEAR_WID} prices"
                df_tables_wid.loc[j, "shortUnit"] = "$"
                df_tables_wid.loc[j, "type"] = "Numeric"
                # df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_deciles10[
                #     "scale_avg"
                # ][dec10]
                df_tables_wid.loc[j, "colorScaleScheme"] = "Greens"
                df_tables_wid.loc[j, "transform"] = (
                    f"multiplyBy {wid_deciles10['wid_notation'][dec10]}_avg_{wid_welfare['slug'][wel]} {wid_income_aggregation['multiplier'][agg]}"
                )
                j += 1

    df_tables_wid["tableSlug"] = merged_tables["name"][tab]

df_tables_wid["sourceName"] = sourceName
df_tables_wid["dataPublishedBy"] = dataPublishedBy
df_tables_wid["sourceLink"] = sourceLink
df_tables_wid["colorScaleNumericMinValue"] = colorScaleNumericMinValue
df_tables_wid["tolerance"] = tolerance
df_tables_wid["colorScaleEqualSizeBins"] = colorScaleEqualSizeBins

###########################################################################################
# LUXEMBOURG INCOME STUDY (LIS)
###########################################################################################
sourceName = SOURCE_NAME_LIS
dataPublishedBy = DATA_PUBLISHED_BY_LIS
sourceLink = SOURCE_LINK_LIS
colorScaleNumericMinValue = COLOR_SCALE_NUMERIC_MIN_VALUE
tolerance = TOLERANCE
colorScaleEqualSizeBins = COLOR_SCALE_EQUAL_SIZEBINS
new_line = NEW_LINE

notes_title = NOTES_TITLE_LIS

processing_description = PROCESSING_DESCRIPTION_LIS

processing_poverty = PROCESSING_POVERTY_LIS
processing_gini_mean_median = PROCESSING_GINI_MEAN_MEDIAN_LIS
processing_distribution = PROCESSING_DISTRIBUTION_LIS

ppp_description = PPP_DESCRIPTION_LIS

df_tables_lis = pd.DataFrame()
j = 0

for tab in range(len(merged_tables)):
    for wel in range(len(lis_welfare)):
        for eq in range(len(lis_equivalence_scales)):
            # I need the original variables to not break the aggregations
            # Mean
            df_tables_lis.loc[j, "name"] = f"Mean {lis_welfare['welfare_type'][wel]} (LIS data)"
            df_tables_lis.loc[j, "slug"] = f"mean_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    f"Mean {lis_welfare['welfare_type'][wel]}.",
                    lis_welfare["description"][wel],
                    lis_equivalence_scales["description"][eq],
                    ppp_description,
                    notes_title,
                    processing_description,
                    processing_gini_mean_median,
                ]
            )
            df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
            df_tables_lis.loc[j, "shortUnit"] = "$"
            df_tables_lis.loc[j, "type"] = "Numeric"
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_mean"][wel]
            df_tables_lis.loc[j, "colorScaleScheme"] = "BuGn"
            df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
            j += 1

            # Median
            df_tables_lis.loc[j, "name"] = f"Median {lis_welfare['welfare_type'][wel]} (LIS data)"
            df_tables_lis.loc[j, "slug"] = f"median_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    f"The level of {lis_welfare['welfare_type'][wel]} below which half of the population falls.",
                    lis_welfare["description"][wel],
                    lis_equivalence_scales["description"][eq],
                    ppp_description,
                    notes_title,
                    processing_description,
                    processing_gini_mean_median,
                ]
            )
            df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
            df_tables_lis.loc[j, "shortUnit"] = "$"
            df_tables_lis.loc[j, "type"] = "Numeric"
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_median"][wel]
            df_tables_lis.loc[j, "colorScaleScheme"] = "Blues"
            df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
            j += 1

            # Thresholds - Deciles
            for dec9 in range(len(lis_deciles9)):
                df_tables_lis.loc[j, "name"] = f"{lis_deciles9['ordinal'][dec9].capitalize()} (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"thr_{lis_deciles9['lis_notation'][dec9]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The level of {lis_welfare['welfare_type'][wel]} below which {lis_deciles9['decile'][dec9]}0% of the population falls.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_distribution,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_deciles9[
                #     "scale_thr"
                # ][dec9]
                df_tables_lis.loc[j, "colorScaleScheme"] = "Purples"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Averages - Deciles
            for dec10 in range(len(lis_deciles10)):
                df_tables_lis.loc[j, "name"] = f"{lis_deciles10['ordinal'][dec10].capitalize()} (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"avg_{lis_deciles10['lis_notation'][dec10]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The mean {lis_welfare['welfare_type'][wel]} within the {lis_deciles10['ordinal'][dec10]} (tenth of the population).",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_distribution,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_deciles10[
                #     "scale_avg"
                # ][dec10]
                df_tables_lis.loc[j, "colorScaleScheme"] = "Greens"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Shares - Deciles
            for dec10 in range(len(lis_deciles10)):
                df_tables_lis.loc[j, "name"] = f"{lis_deciles10['ordinal'][dec10].capitalize()} (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"share_{lis_deciles10['lis_notation'][dec10]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The share of {lis_welfare['welfare_type'][wel]} received by the {lis_deciles10['ordinal'][dec10]} (tenth of the population).",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_distribution,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_deciles10[
                #     "scale_share"
                # ][dec10]
                df_tables_lis.loc[j, "colorScaleScheme"] = "OrRd"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Daily, monthly, annual aggregations
            for agg in range(len(lis_income_aggregation)):
                # Mean
                df_tables_lis.loc[j, "name"] = f"Mean {lis_welfare['welfare_type'][wel]} (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"mean_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}{lis_income_aggregation['slug_suffix'][agg]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"Mean {lis_welfare['welfare_type'][wel]}.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_gini_mean_median,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare[
                #     "scale_mean"
                # ][wel]
                df_tables_lis.loc[j, "colorScaleScheme"] = "BuGn"
                df_tables_lis.loc[j, "transform"] = (
                    f"multiplyBy mean_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]} {lis_income_aggregation['multiplier'][agg]}"
                )
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

                # Median
                df_tables_lis.loc[j, "name"] = f"Median {lis_welfare['welfare_type'][wel]} (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"median_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}{lis_income_aggregation['slug_suffix'][agg]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The level of {lis_welfare['welfare_type'][wel]} below which half of the population falls.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_gini_mean_median,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare[
                #     "scale_median"
                # ][wel]
                df_tables_lis.loc[j, "colorScaleScheme"] = "Blues"
                df_tables_lis.loc[j, "transform"] = (
                    f"multiplyBy median_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]} {lis_income_aggregation['multiplier'][agg]}"
                )
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

                # Thresholds - Deciles
                for dec9 in range(len(lis_deciles9)):
                    df_tables_lis.loc[j, "name"] = f"{lis_deciles9['ordinal'][dec9].capitalize()} (LIS data)"
                    df_tables_lis.loc[j, "slug"] = (
                        f"thr_{lis_deciles9['lis_notation'][dec9]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}{lis_income_aggregation['slug_suffix'][agg]}"
                    )
                    df_tables_lis.loc[j, "description"] = new_line.join(
                        [
                            f"The level of {lis_welfare['welfare_type'][wel]} below which {lis_deciles9['decile'][dec9]}0% of the population falls.",
                            lis_welfare["description"][wel],
                            lis_equivalence_scales["description"][eq],
                            ppp_description,
                            notes_title,
                            processing_description,
                            processing_distribution,
                        ]
                    )
                    df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                    df_tables_lis.loc[j, "shortUnit"] = "$"
                    df_tables_lis.loc[j, "type"] = "Numeric"
                    # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_deciles9[
                    #     "scale_thr"
                    # ][dec9]
                    df_tables_lis.loc[j, "colorScaleScheme"] = "Purples"
                    df_tables_lis.loc[j, "transform"] = (
                        f"multiplyBy thr_{lis_deciles9['lis_notation'][dec9]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]} {lis_income_aggregation['multiplier'][agg]}"
                    )
                    df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                    j += 1

                # Averages - Deciles
                for dec10 in range(len(lis_deciles10)):
                    df_tables_lis.loc[j, "name"] = f"{lis_deciles10['ordinal'][dec10].capitalize()} (LIS data)"
                    df_tables_lis.loc[j, "slug"] = (
                        f"avg_{lis_deciles10['lis_notation'][dec10]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}{lis_income_aggregation['slug_suffix'][agg]}"
                    )
                    df_tables_lis.loc[j, "description"] = new_line.join(
                        [
                            f"The mean {lis_welfare['welfare_type'][wel]} within the {lis_deciles10['ordinal'][dec10]} (tenth of the population).",
                            lis_welfare["description"][wel],
                            lis_equivalence_scales["description"][eq],
                            ppp_description,
                            notes_title,
                            processing_description,
                            processing_distribution,
                        ]
                    )
                    df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                    df_tables_lis.loc[j, "shortUnit"] = "$"
                    df_tables_lis.loc[j, "type"] = "Numeric"
                    # df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_deciles10[
                    #     "scale_avg"
                    # ][dec10]
                    df_tables_lis.loc[j, "colorScaleScheme"] = "Greens"
                    df_tables_lis.loc[j, "transform"] = (
                        f"multiplyBy avg_{lis_deciles10['lis_notation'][dec10]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]} {lis_income_aggregation['multiplier'][agg]}"
                    )
                    df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                    j += 1

    df_tables_lis["tableSlug"] = merged_tables["name"][tab]

df_tables_lis["sourceName"] = sourceName
df_tables_lis["dataPublishedBy"] = dataPublishedBy
df_tables_lis["sourceLink"] = sourceLink
df_tables_lis["colorScaleNumericMinValue"] = colorScaleNumericMinValue
df_tables_lis["tolerance"] = tolerance
df_tables_lis["colorScaleEqualSizeBins"] = colorScaleEqualSizeBins

# Remove all the rows that have the "equivalized" value in the equivalized column
df_tables_lis = df_tables_lis[df_tables_lis["equivalized"] != "equivalized"].reset_index(drop=True)

# Drop the equivalized column
df_tables_lis = df_tables_lis.drop(columns=["equivalized"])

# Concatenate all the tables into one
df_tables = pd.concat([df_tables_pip, df_tables_wid, df_tables_lis], ignore_index=True)
# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# ### Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by welfare type.

# Grapher table generation

yAxisMin = Y_AXIS_MIN
selectedFacetStrategy = "entity"
hasMapTab = "false"
tab_parameter = "chart"

df_graphers = pd.DataFrame()

j = 0

for tab in range(len(merged_tables)):
    for view in range(len(source_checkbox)):
        for agg in range(len(lis_income_aggregation)):
            # Mean
            df_graphers.loc[j, "title"] = (
                f"Mean income per {lis_income_aggregation['aggregation'][agg]} ({source_checkbox['type_title'][view]})"
            )
            df_graphers.loc[j, "ySlugs"] = source_checkbox["mean"][view].replace(
                "{agg}", lis_income_aggregation["slug_suffix"][agg]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption"
            df_graphers.loc[j, "Decile Dropdown"] = np.nan
            df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
            df_graphers.loc[j, "Period Radio"] = lis_income_aggregation["aggregation"][agg].capitalize()
            df_graphers.loc[j, "hideRelativeToggle"] = "false"
            df_graphers.loc[j, "subtitle"] = f"{source_checkbox['note'][view]}"
            df_graphers.loc[j, "note"] = f"{source_checkbox['note_ppp'][view]}"
            df_graphers.loc[j, "yScaleToggle"] = "true"
            j += 1

            # Median
            df_graphers.loc[j, "title"] = (
                f"Median income per {lis_income_aggregation['aggregation'][agg]} ({source_checkbox['type_title'][view]})"
            )
            df_graphers.loc[j, "ySlugs"] = source_checkbox["median"][view].replace(
                "{agg}", lis_income_aggregation["slug_suffix"][agg]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Median income or consumption"
            df_graphers.loc[j, "Decile Dropdown"] = np.nan
            df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
            df_graphers.loc[j, "Period Radio"] = lis_income_aggregation["aggregation"][agg].capitalize()
            df_graphers.loc[j, "hideRelativeToggle"] = "false"
            df_graphers.loc[j, "subtitle"] = f"{source_checkbox['note'][view]}"
            df_graphers.loc[j, "note"] = f"{source_checkbox['note_ppp'][view]}"
            df_graphers.loc[j, "yScaleToggle"] = "true"
            j += 1

            # Thresholds - Deciles
            for dec9 in range(len(deciles9)):
                df_graphers.loc[j, "title"] = (
                    f"Threshold income marking the {deciles9['ordinal'][dec9]} ({source_checkbox['type_title'][view]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    source_checkbox["thr"][view]
                    .replace("{agg}", lis_income_aggregation["slug_suffix"][agg])
                    .replace("{dec9_pip}", deciles9["decile"][dec9])
                    .replace("{dec9_wid}", deciles9["wid_notation"][dec9])
                    .replace("{dec9_lis}", deciles9["lis_notation"][dec9])
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Decile thresholds"
                df_graphers.loc[j, "Decile Dropdown"] = deciles9["dropdown"][dec9]
                df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
                df_graphers.loc[j, "Period Radio"] = lis_income_aggregation["aggregation"][agg].capitalize()
                df_graphers.loc[j, "hideRelativeToggle"] = "false"
                df_graphers.loc[j, "subtitle"] = (
                    f"The level of income per {lis_income_aggregation['aggregation'][agg]} below which {deciles9['decile'][dec9]}0% of the population falls. {source_checkbox['note'][view]}"
                )
                df_graphers.loc[j, "note"] = f"{source_checkbox['note_ppp'][view]}"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

            # Averages - Deciles
            for dec10 in range(len(deciles10)):
                df_graphers.loc[j, "title"] = (
                    f"Mean income within the {deciles10['ordinal'][dec10]} ({source_checkbox['type_title'][view]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    source_checkbox["avg"][view]
                    .replace("{agg}", lis_income_aggregation["slug_suffix"][agg])
                    .replace("{dec10_pip}", deciles10["decile"][dec10])
                    .replace("{dec10_wid}", deciles10["wid_notation"][dec10])
                    .replace("{dec10_lis}", deciles10["lis_notation"][dec10])
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption, by decile"
                df_graphers.loc[j, "Decile Dropdown"] = deciles10["dropdown"][dec10]
                df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
                df_graphers.loc[j, "Period Radio"] = lis_income_aggregation["aggregation"][agg].capitalize()
                df_graphers.loc[j, "hideRelativeToggle"] = "false"
                df_graphers.loc[j, "subtitle"] = (
                    f"The mean income per {lis_income_aggregation['aggregation'][agg]} within the {deciles10['ordinal'][dec10]} (tenth of the population). {source_checkbox['note'][view]}"
                )
                df_graphers.loc[j, "note"] = f"{source_checkbox['note_ppp'][view]}"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

        # Shares - Deciles
        for dec10 in range(len(deciles10)):
            df_graphers.loc[j, "title"] = (
                f"Income share of the {deciles10['ordinal'][dec10]} ({source_checkbox['type_title'][view]})"
            )
            df_graphers.loc[j, "ySlugs"] = (
                source_checkbox["share"][view]
                .replace("{dec10_pip}", deciles10["decile"][dec10])
                .replace("{dec10_wid}", deciles10["wid_notation"][dec10])
                .replace("{dec10_lis}", deciles10["lis_notation"][dec10])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Decile shares"
            df_graphers.loc[j, "Decile Dropdown"] = deciles10["dropdown"][dec10]
            df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
            df_graphers.loc[j, "subtitle"] = (
                f"The share of income received by the {deciles10['ordinal'][dec10]}. {source_checkbox['note'][view]}"
            )
            df_graphers.loc[j, "note"] = np.nan
            j += 1

    df_graphers["tableSlug"] = merged_tables["name"][tab]

# Add yAxisMin and other columns
df_graphers["yAxisMin"] = yAxisMin
df_graphers["selectedFacetStrategy"] = selectedFacetStrategy
df_graphers["hasMapTab"] = hasMapTab
df_graphers["tab"] = tab_parameter

# Drop rows with empty ySlugs (they make the checkbox system fail)
df_graphers = df_graphers[df_graphers["ySlugs"] != ""].reset_index(drop=True)

# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# Select one default view
df_graphers.loc[
    (df_graphers["Indicator Dropdown"] == "Mean income or consumption")
    & (df_graphers["Income measure Dropdown"] == "After tax")
    & (df_graphers["Period Radio"] == "Year"),
    ["defaultView"],
] = "true"

# Reorder dropdown menus
# Decile dropdown
decile_dropdown_list = [
    np.nan,
    "1 (poorest)",
    "2",
    "3",
    "4",
    "5",
    "5 (median)",
    "6",
    "7",
    "8",
    "9",
    "9 (richest)",
    "10 (richest)",
    "All deciles",
]

df_graphers_mapping = pd.DataFrame(
    {
        "decile_dropdown": decile_dropdown_list,
    }
)
df_graphers_mapping = df_graphers_mapping.reset_index().set_index("decile_dropdown")
df_graphers["decile_dropdown_aux"] = df_graphers["Decile Dropdown"].map(df_graphers_mapping["index"])

# Metric dropdown
metric_dropdown_list = [
    "Mean income or consumption",
    "Mean income or consumption, by decile",
    "Median income or consumption",
    "Decile thresholds",
    "Decile shares",
]

df_graphers_mapping = pd.DataFrame(
    {
        "metric_dropdown": metric_dropdown_list,
    }
)
df_graphers_mapping = df_graphers_mapping.reset_index().set_index("metric_dropdown")
df_graphers["metric_dropdown_aux"] = df_graphers["Indicator Dropdown"].map(df_graphers_mapping["index"])

# Sort by auxiliary variables and drop
df_graphers = df_graphers.sort_values(["decile_dropdown_aux", "metric_dropdown_aux"], ignore_index=True)
df_graphers = df_graphers.drop(columns=["metric_dropdown_aux", "decile_dropdown_aux"])

# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

save("incomes-across-distribution-comparison", merged_tables, df_header, df_graphers, df_tables)  # type: ignore
