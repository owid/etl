# %% [markdown]
# # Inequality Data Explorer - Source Comparison
# This code creates the tsv file for the poverty comparison explorer, available [here](https://owid.cloud/admin/explorers/preview/poverty-comparison)

import numpy as np

# %%
import pandas as pd

from ..common_parameters import *

# %% [markdown]
# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each type of welfare measure or tables considered.

# %%
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
# Only get the combination where PIP and LIS are true
source_checkbox = source_checkbox[
    (source_checkbox["wid"] == "false") & (source_checkbox["pip"] == "true") & (source_checkbox["lis"] == "true")
].reset_index(drop=True)

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

# Absolute poverty sheet
sheet_name = "povlines_abs"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_povlines_abs = pd.read_csv(url, dtype={"dollars_text": "str"})

# Relative poverty sheet
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
lis_povlines_rel = pd.read_csv(url)

# WORLD BANK POVERTY AND INEQUALITY PLATFORM
# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Survey type sheet
sheet_name = "table"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_tables = pd.read_csv(url)

# Absolute poverty sheet
sheet_name = "povlines_abs"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_povlines_abs = pd.read_csv(url, dtype={"dollars_text": "str"})

# Relative poverty sheet
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_povlines_rel = pd.read_csv(url)

# %% [markdown]
# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# %%
# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Poverty - World Bank, and LIS",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Compare World Bank and Luxembourg Income Study data on poverty.",
    "isPublished": "true",
    "googleSheet": "",
    "wpBlockId": "57728",
    "entityType": "country or region",
    "pickerColumnSlugs": "headcount_ratio_215 headcount_ratio_365 headcount_ratio_685 headcount_ratio_3000 headcount_215 headcount_365 headcount_685 headcount_3000 headcount_ratio_50_median headcount_50_median headcount_ratio_dhi_pc_215 headcount_ratio_dhi_pc_365 headcount_ratio_dhi_pc_685 headcount_ratio_dhi_pc_3000 headcount_dhi_pc_215 headcount_dhi_pc_365 headcount_dhi_pc_3000 headcount_dhi_pc_685 headcount_ratio_50_median_dhi_pc headcount_50_median_dhi_pc",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# %% [markdown]
# ## Tables
# Variables are grouped by type of welfare to iterate by different survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.
# ### Tables for variables not showing breaks between surveys
# These variables consider a continous series, without breaks due to changes in surveys' methodology

# %%

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

processing_description = PROCESSING_DESCRIPTION_PIP_POVERTY
ppp_description = PPP_DESCRIPTION_PIP_2017
relative_poverty_description = RELATIVE_POVERTY_DESCRIPTION_PIP

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

    # Headcount ratio (abs)
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Share below ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"headcount_ratio_{pip_povlines_abs.cents[p]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with {pip_tables.text[tab]} below ${pip_povlines_abs.dollars_text[p]} a day.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "3;10;20;30;40;50;60;70;80;90;100"
        df_tables_pip.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

    # Headcount (abs)
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Number below ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"headcount_{pip_povlines_abs.cents[p]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with {pip_tables.text[tab]} per person below ${pip_povlines_abs.dollars_text[p]} a day.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = np.nan
        df_tables_pip.loc[j, "shortUnit"] = np.nan
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;1000000001"
        )
        df_tables_pip.loc[j, "colorScaleScheme"] = "Reds"
        j += 1

    # Total shortfall (abs)
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Total daily shortfall - ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"total_shortfall_{pip_povlines_abs.cents[p]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of ${pip_povlines_abs.dollars_text[p]} a day. This is the amount of money that would be theoretically needed to lift the {pip_tables.text[tab]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;1000000001"
        )
        df_tables_pip.loc[j, "colorScaleScheme"] = "Oranges"
        j += 1

    # Total shortfall (abs): Yearly value
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Total shortfall - ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"total_shortfall_{pip_povlines_abs.cents[p]}_year"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of ${pip_povlines_abs.dollars_text[p]} a day. This is the amount of money that would be theoretically needed to lift the {pip_tables.text[tab]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_povlines_abs["scale_total_shortfall"][p]
        df_tables_pip.loc[j, "colorScaleScheme"] = "Oranges"
        df_tables_pip.loc[j, "transform"] = f"multiplyBy total_shortfall_{pip_povlines_abs.cents[p]} 365"
        j += 1

    # Average shortfall ($ per day)
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Average shortfall - ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"avg_shortfall_{pip_povlines_abs.cents[p]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The average shortfall from a poverty line of ${pip_povlines_abs.dollars_text[p]} a day (averaged across the population in poverty).",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = pip_povlines_abs.scale_avg_shortfall[p]
        df_tables_pip.loc[j, "colorScaleScheme"] = "Purples"
        j += 1

    # Average shortfall (% of poverty line) [this is the income gap ratio]
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Income gap ratio - ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"income_gap_ratio_{pip_povlines_abs.cents[p]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f'The average shortfall from a poverty line of ${pip_povlines_abs.dollars_text[p]} a day (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60;70;80;90;100"
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrRd"
        j += 1

    # Poverty gap index
    for p in range(len(pip_povlines_abs)):
        df_tables_pip.loc[j, "name"] = f"Poverty gap index - ${pip_povlines_abs.dollars_text[p]} a day (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"poverty_gap_index_{pip_povlines_abs.cents[p]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The poverty gap index calculated at a poverty line of ${pip_povlines_abs.dollars_text[p]} a day. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                additional_description,
                ppp_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60;70;80;90;100"
        df_tables_pip.loc[j, "colorScaleScheme"] = "RdPu"
        j += 1

    # Headcount ratio (rel)
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Share below {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"headcount_ratio_{pip_povlines_rel.slug_suffix[pct]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {pip_tables.text[tab]} per person below {pip_povlines_rel.percent[pct]} of the median.",
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30;30.0001"
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    # Headcount (rel)
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Number below {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"headcount_{pip_povlines_rel.slug_suffix[pct]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {pip_tables.text[tab]} per person below {pip_povlines_rel.percent[pct]} of the median.",
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = np.nan
        df_tables_pip.loc[j, "shortUnit"] = np.nan
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;1000000001"
        )
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    # Total shortfall (rel)
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Total daily shortfall - {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"total_shortfall_{pip_povlines_rel.slug_suffix[pct]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of {pip_povlines_rel.text[pct]} {pip_tables.text[tab]}. This is the amount of money that would be theoretically needed to lift the {pip_tables.text[tab]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = np.nan
        df_tables_pip.loc[j, "shortUnit"] = np.nan
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;1000000001"
        )
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    # Total shortfall (rel): Yearly value
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Total shortfall - {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"total_shortfall_{pip_povlines_rel.slug_suffix[pct]}_year"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of {pip_povlines_rel.text[pct]} {pip_tables.text[tab]}. This is the amount of money that would be theoretically needed to lift the {pip_tables.text[tab]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = np.nan
        df_tables_pip.loc[j, "shortUnit"] = np.nan
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;1000000001"
        )
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables_pip.loc[j, "transform"] = f"multiplyBy total_shortfall_{pip_povlines_rel.slug_suffix[pct]} 365"
        j += 1

    # Average shortfall ($ per day)
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Average shortfall - {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"avg_shortfall_{pip_povlines_rel.slug_suffix[pct]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The average shortfall from a poverty line of of {pip_povlines_rel.text[pct]} {pip_tables.text[tab]} (averaged across the population in poverty).",
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables_pip.loc[j, "shortUnit"] = "$"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;20.0001"
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    # Average shortfall (% of poverty line) [this is the income gap ratio]
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Income gap ratio - {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"income_gap_ratio_{pip_povlines_rel.slug_suffix[pct]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f'The average shortfall from a poverty line of of {pip_povlines_rel.text[pct]} {pip_tables.text[tab]} (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60;70;80;90;100"
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    # Poverty gap index
    for pct in range(len(pip_povlines_rel)):
        df_tables_pip.loc[j, "name"] = f"Poverty gap index - {pip_povlines_rel.percent[pct]} of median (PIP data)"
        df_tables_pip.loc[j, "slug"] = f"poverty_gap_index_{pip_povlines_rel.slug_suffix[pct]}"
        df_tables_pip.loc[j, "description"] = new_line.join(
            [
                f"The poverty gap index calculated at a poverty line of {pip_povlines_rel.text[pct]} {pip_tables.text[tab]}. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                relative_poverty_description,
                additional_description,
                notes_title,
                f"Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case {pip_povlines_rel.text[pct]} – and then run a specific query on the PIP API to return the share of population below that line.",
                processing_description,
            ]
        )
        df_tables_pip.loc[j, "unit"] = "%"
        df_tables_pip.loc[j, "shortUnit"] = "%"
        df_tables_pip.loc[j, "type"] = "Numeric"
        df_tables_pip.loc[j, "colorScaleNumericBins"] = "3;6;9;12;15;18;21"
        df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

df_tables_pip["tableSlug"] = tableSlug
df_tables_pip["sourceName"] = sourceName
df_tables_pip["dataPublishedBy"] = dataPublishedBy
df_tables_pip["sourceLink"] = sourceLink
df_tables_pip["colorScaleNumericMinValue"] = colorScaleNumericMinValue
df_tables_pip["tolerance"] = tolerance

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
relative_poverty_description = RELATIVE_POVERTY_DESCRIPTION_LIS

df_tables_lis = pd.DataFrame()
j = 0

# NOTE: # I am using the PIP poverty lines to compare with LIS
for tab in range(len(merged_tables)):
    for wel in range(len(lis_welfare)):
        for eq in range(len(lis_equivalence_scales)):
            # Headcount ratio (abs)
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = f"Share below ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"headcount_ratio_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"% of population living in households with {lis_welfare['welfare_type'][wel]} below ${pip_povlines_abs['dollars_text'][p]} a day.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "3;10;20;30;40;50;60;70;80;90;100"
                df_tables_lis.loc[j, "colorScaleScheme"] = "OrRd"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Headcount (abs)
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = f"Number below ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"headcount_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"Number of people living in households with {lis_welfare['welfare_type'][wel]} below ${pip_povlines_abs['dollars_text'][p]} a day.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = np.nan
                df_tables_lis.loc[j, "shortUnit"] = np.nan
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = (
                    "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000"
                )
                df_tables_lis.loc[j, "colorScaleScheme"] = "Reds"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Total shortfall (abs)
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = (
                    f"Total shortfall - ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"total_shortfall_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs.cents[p]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The total shortfall from a poverty line of ${pip_povlines_abs['dollars_text'][p]} a day. This is the amount of money that would be theoretically needed to lift the {lis_welfare['welfare_type'][wel]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = pip_povlines_abs["scale_total_shortfall"][p]
                df_tables_lis.loc[j, "colorScaleScheme"] = "Oranges"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Average shortfall ($)
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = (
                    f"Average yearly shortfall - ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"avg_shortfall_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of ${pip_povlines_abs['dollars_text'][p]} (averaged across the population in poverty).",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = pip_povlines_abs["scale_avg_shortfall"][p]
                df_tables_lis.loc[j, "colorScaleScheme"] = "Purples"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Average shortfall ($): Daily value
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = (
                    f"Average shortfall - ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"avg_shortfall_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]}_day"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of ${pip_povlines_abs['dollars_text'][p]} (averaged across the population in poverty).",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = pip_povlines_abs["scale_avg_shortfall"][p]
                df_tables_lis.loc[j, "colorScaleScheme"] = "Purples"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                df_tables_lis.loc[j, "transform"] = (
                    f"multiplyBy avg_shortfall_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]} 0.00274"
                )
                j += 1

            # Average shortfall (% of poverty line) [this is the income gap ratio]
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = (
                    f"Income gap ratio - ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"income_gap_ratio_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f'The average shortfall from a poverty line of ${pip_povlines_abs.dollars_text[p]} a day (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60;70;80;90;100"
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrRd"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Poverty gap index
            for p in range(len(pip_povlines_abs)):
                df_tables_lis.loc[j, "name"] = (
                    f"Poverty gap index - ${pip_povlines_abs['dollars_text'][p]} a day (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"poverty_gap_index_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_{pip_povlines_abs['cents'][p]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The poverty gap index calculated at a poverty line of ${pip_povlines_abs['dollars_text'][p]} a day. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60"
                df_tables_lis.loc[j, "colorScaleScheme"] = "RdPu"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Headcount ratio (rel)
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = f"Share below {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"headcount_ratio_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        "% of population living in households with {welfare['welfare_type'][wel]} below {povlines_rel['percent'][pct]} of the median {welfare['welfare_type'][wel]}.",
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30"
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Headcount (rel)
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = f"Number below {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                df_tables_lis.loc[j, "slug"] = (
                    f"headcount_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"Number of people living in households with {lis_welfare['welfare_type'][wel]} below {lis_povlines_rel['percent'][pct]} of the median {lis_welfare['welfare_type'][wel]}.",
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = np.nan
                df_tables_lis.loc[j, "shortUnit"] = np.nan
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = (
                    "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000"
                )
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Total shortfall (rel)
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = (
                    f"Total shortfall - {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"total_shortfall_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The total shortfall from a poverty line of {lis_povlines_rel['text'][pct]} {lis_welfare['welfare_type'][wel]}. This is the amount of money that would be theoretically needed to lift the {lis_welfare['welfare_type'][wel]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = np.nan
                df_tables_lis.loc[j, "shortUnit"] = np.nan
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_povlines_rel["scale_total_shortfall"][pct]
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Average shortfall ($)
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = (
                    f"Average yearly shortfall - {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"avg_shortfall_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of of {lis_povlines_rel['text'][pct]} {lis_welfare['welfare_type'][wel]} (averaged across the population in poverty).",
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "1000;2000;3000;4000;5000"
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Average shortfall ($): Daily value
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = (
                    f"Average shortfall - {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"avg_shortfall_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}_day"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of of {lis_povlines_rel['text'][pct]} {lis_welfare['welfare_type'][wel]} (averaged across the population in poverty).",
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables_lis.loc[j, "shortUnit"] = "$"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "1000;2000;3000;4000;5000"
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                df_tables_lis.loc[j, "transform"] = (
                    f"multiplyBy avg_shortfall_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]} 0.00274"
                )
                j += 1

            # Average shortfall (% of poverty line) [this is the income gap ratio]
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = (
                    f"Income gap ratio - {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"income_gap_ratio_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f'The average shortfall from a poverty line of of {lis_povlines_rel.text[pct]} {lis_welfare.welfare_type[wel]} (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30;35;40"
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
                j += 1

            # Poverty gap index
            for pct in range(len(lis_povlines_rel)):
                df_tables_lis.loc[j, "name"] = (
                    f"Poverty gap index - {lis_povlines_rel['percent'][pct]} of median (LIS data)"
                )
                df_tables_lis.loc[j, "slug"] = (
                    f"poverty_gap_index_{lis_povlines_rel['slug_suffix'][pct]}_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
                )
                df_tables_lis.loc[j, "description"] = new_line.join(
                    [
                        f"The poverty gap index calculated at a poverty line of {lis_povlines_rel['text'][pct]} {lis_welfare['welfare_type'][wel]}. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                        relative_poverty_description,
                        lis_welfare["description"][wel],
                        lis_equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables_lis.loc[j, "unit"] = "%"
                df_tables_lis.loc[j, "shortUnit"] = "%"
                df_tables_lis.loc[j, "type"] = "Numeric"
                df_tables_lis.loc[j, "colorScaleNumericBins"] = "2;4;6;8;10;12"
                df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
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
df_tables = pd.concat([df_tables_pip, df_tables_lis], ignore_index=True)
# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# %% [markdown]
# ### Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by welfare type.

# %%
# Grapher table generation

yAxisMin = Y_AXIS_MIN
selectedFacetStrategy = "entity"
hasMapTab = "false"
tab_parameter = "chart"

datasets_description = "LIS data relates to income after taxes and benefits [per capita](#dod:per-capita). Depending on the country and year, PIP data relates to income measured after taxes and benefits, or to consumption, per capita."

df_graphers = pd.DataFrame()

j = 0

for tab in range(len(merged_tables)):
    for view in range(len(source_checkbox)):
        for p in range(len(pip_povlines_abs)):
            # Headcount ratio (abs)
            df_graphers.loc[j, "title"] = f"{pip_povlines_abs['title_share'][p]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["headcount_ratio"][view].replace(
                "{p}", str(pip_povlines_abs["cents"][p])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "subtitle"] = datasets_description
            df_graphers.loc[j, "note"] = (
                f"{pip_povlines_abs['subtitle'][p]} This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Headcount (abs)
            df_graphers.loc[j, "title"] = f"{pip_povlines_abs.title_number[p]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["headcount"][view].replace(
                "{p}", str(pip_povlines_abs["cents"][p])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "subtitle"] = datasets_description
            df_graphers.loc[j, "note"] = (
                f"{pip_povlines_abs['subtitle'][p]} This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Total shortfall (abs)
            df_graphers.loc[j, "title"] = f"{pip_povlines_abs['title_total_shortfall'][p]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["total_shortfall"][view].replace(
                "{p}", str(pip_povlines_abs["cents"][p])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "subtitle"] = f"{pip_povlines_abs['subtitle_total_shortfall'][p]}"
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Average shortfall ($)
            df_graphers.loc[j, "title"] = f"{pip_povlines_abs['title_avg_shortfall'][p]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["avg_shortfall"][view].replace(
                "{p}", str(pip_povlines_abs["cents"][p])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "subtitle"] = f"{pip_povlines_abs['subtitle_avg_shortfall'][p]}"
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Average shortfall (% of poverty line)
            df_graphers.loc[j, "title"] = f"{pip_povlines_abs['title_income_gap_ratio'][p]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["income_gap_ratio"][view].replace(
                "{p}", str(pip_povlines_abs["cents"][p])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_abs.povline_dropdown[p]}"
            df_graphers.loc[j, "subtitle"] = f"{pip_povlines_abs['subtitle_income_gap_ratio'][p]}"
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Poverty gap index
            df_graphers.loc[j, "title"] = f"Poverty gap index at ${pip_povlines_abs['dollars_text'][p]} a day"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["poverty_gap_index"][view].replace(
                "{p}", str(pip_povlines_abs["cents"][p])
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "subtitle"] = (
                f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line)."
            )
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

        # Headcount ratio (rel)
        for pct in range(len(pip_povlines_rel)):
            df_graphers.loc[j, "title"] = f"{pip_povlines_rel['title_share'][pct]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["headcount_ratio_rel"][view].replace(
                "{pct}", pip_povlines_rel["slug_suffix"][pct]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "subtitle"] = datasets_description
            df_graphers.loc[j, "note"] = (
                f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {pip_povlines_rel['text'][pct]}"
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Headcount (rel)
            df_graphers.loc[j, "title"] = f"{pip_povlines_rel['title_number'][pct]}"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["headcount_rel"][view].replace(
                "{pct}", pip_povlines_rel["slug_suffix"][pct]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "subtitle"] = datasets_description
            df_graphers.loc[j, "note"] = (
                f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {pip_povlines_rel['text'][pct]}"
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Total shortfall (rel)
            df_graphers.loc[j, "title"] = (
                f"Total shortfall from a poverty line of {pip_povlines_rel['text'][pct]} income"
            )
            df_graphers.loc[j, "ySlugs"] = source_checkbox["total_shortfall_rel"][view].replace(
                "{pct}", pip_povlines_rel["slug_suffix"][pct]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "subtitle"] = (
                f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {pip_povlines_rel.text[pct]}"
            )
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Average shortfall ($) (rel)
            df_graphers.loc[j, "title"] = (
                f"Average shortfall from a poverty line of {pip_povlines_rel['text'][pct]} income"
            )
            df_graphers.loc[j, "ySlugs"] = source_checkbox["avg_shortfall_rel"][view].replace(
                "{pct}", pip_povlines_rel["slug_suffix"][pct]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "subtitle"] = (
                f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {pip_povlines_rel['text'][pct]} income, averaged across the population in poverty."
            )
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Average shortfall (% of poverty line) (rel)
            df_graphers.loc[j, "title"] = (
                f"Average shortfall from a poverty line of {pip_povlines_rel['text'][pct]} income (as a share of the poverty line)"
            )
            df_graphers.loc[j, "ySlugs"] = source_checkbox["income_gap_ratio_rel"][view].replace(
                "{pct}", pip_povlines_rel["slug_suffix"][pct]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "subtitle"] = (
                f'This is the average shortfall expressed as a share of the poverty line, sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than {pip_povlines_rel.text[pct]} income.'
            )
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

            # Poverty gap index (rel)
            df_graphers.loc[j, "title"] = f"Poverty gap index at {pip_povlines_rel['text'][pct]} income"
            df_graphers.loc[j, "ySlugs"] = source_checkbox["poverty_gap_index_rel"][view].replace(
                "{pct}", pip_povlines_rel["slug_suffix"][pct]
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{pip_povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "subtitle"] = (
                f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line)."
            )
            df_graphers.loc[j, "note"] = (
                f"{datasets_description} This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            j += 1

    df_graphers["tableSlug"] = merged_tables["name"][tab]

# Add yAxisMin
df_graphers["yAxisMin"] = yAxisMin
df_graphers["selectedFacetStrategy"] = selectedFacetStrategy
df_graphers["hasMapTab"] = hasMapTab
df_graphers["tab"] = tab_parameter

# Drop rows with empty ySlugs (they make the checkbox system fail)
df_graphers = df_graphers[df_graphers["ySlugs"] != ""].reset_index(drop=True)

# %% [markdown]
# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# %%
# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# Select one default view
df_graphers.loc[
    (df_graphers["Indicator Dropdown"] == "Share in poverty")
    & (df_graphers["Poverty line Dropdown"] == "$2.15 per day: International Poverty Line"),
    ["defaultView"],
] = "true"


# %% [markdown]
# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

# %%
save("poverty-comparison", merged_tables, df_header, df_graphers, df_tables)  # type: ignore
