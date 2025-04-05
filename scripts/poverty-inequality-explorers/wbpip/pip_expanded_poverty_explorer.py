# %% [markdown]
# # Poverty Data Explorer of World Bank data: Expanded metrics
# This code creates the tsv file for the expanded poverty metrics explorer from the World Bank PIP data, available [here](https://owid.cloud/admin/explorers/preview/poverty-explorer-expanded)

import textwrap

import numpy as np

# %%
import pandas as pd

from ..common_parameters import *

# %% [markdown]
# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each poverty line or survey type.

# %%
# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Absolute poverty sheet
sheet_name = "povlines_abs"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_abs = pd.read_csv(url, dtype={"dollars_text": "str"})

# Relative poverty sheet
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_rel = pd.read_csv(url)

# Survey type sheet
sheet_name = "survey_type"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
survey_type = pd.read_csv(url)

# %% [markdown]
# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# %%
# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Poverty - World Bank",
    "selection": ["Mozambique", "Nigeria", "Kenya", "Bangladesh", "Bolivia", "World"],
    "explorerSubtitle": "Explore World Bank data on poverty.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57756",
    "entityType": "country or region",
    "pickerColumnSlugs": "headcount_ratio_215 headcount_ratio_365 headcount_ratio_685 headcount_ratio_3000 headcount_215 headcount_365 headcount_685 headcount_3000 headcount_ratio_50_median headcount_50_median",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# %% [markdown]
# ## Tables
# Variables are grouped by type to iterate by different poverty lines and survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.
# ### Tables for variables not showing breaks between surveys
# These variables consider a continous series, without breaks due to changes in surveys' methodology

# %%
sourceName = SOURCE_NAME_PIP
dataPublishedBy = DATA_PUBLISHED_BY_PIP
sourceLink = SOURCE_LINK_PIP
colorScaleNumericMinValue = COLOR_SCALE_NUMERIC_MIN_VALUE
tolerance = TOLERANCE
colorScaleEqualSizeBins = COLOR_SCALE_EQUAL_SIZEBINS
new_line = NEW_LINE

yAxisMin = Y_AXIS_MIN

additional_description = ADDITIONAL_DESCRIPTION_PIP

notes_title = NOTES_TITLE_PIP

processing_description = PROCESSING_DESCRIPTION_PIP
ppp_description = PPP_DESCRIPTION_PIP_2017
relative_poverty_description = RELATIVE_POVERTY_DESCRIPTION_PIP

# Table generation
df_tables = pd.DataFrame()
j = 0

for survey in range(len(survey_type)):
    # Define country as entityName
    df_tables.loc[j, "name"] = "Country"
    df_tables.loc[j, "slug"] = "country"
    df_tables.loc[j, "type"] = "EntityName"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Define year as Year
    df_tables.loc[j, "name"] = "Year"
    df_tables.loc[j, "slug"] = "year"
    df_tables.loc[j, "type"] = "Year"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Headcount ratio (abs)
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"Share below ${povlines_abs.dollars_text[p]} a day"
        df_tables.loc[j, "slug"] = f"headcount_ratio_{povlines_abs.cents[p]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {survey_type.text[survey]} per person below ${povlines_abs.dollars_text[p]} a day.",
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = "3;10;20;30;40;50;60;70;80;90;100"
        df_tables.loc[j, "colorScaleScheme"] = "OrRd"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Headcount (abs)
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"Number below ${povlines_abs.dollars_text[p]} a day"
        df_tables.loc[j, "slug"] = f"headcount_{povlines_abs.cents[p]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {survey_type.text[survey]} per person below ${povlines_abs.dollars_text[p]} a day.",
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = np.nan
        df_tables.loc[j, "shortUnit"] = np.nan
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000"
        )
        df_tables.loc[j, "colorScaleScheme"] = "Reds"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Total shortfall (abs)
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"${povlines_abs.dollars_text[p]} a day - Total daily shortfall"
        df_tables.loc[j, "slug"] = f"total_shortfall_{povlines_abs.cents[p]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of ${povlines_abs.dollars_text[p]} a day. This is the amount of money that would be theoretically needed to lift the {survey_type.text[survey]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables.loc[j, "shortUnit"] = "$"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;3000000000;10000000000"
        )
        df_tables.loc[j, "colorScaleScheme"] = "Oranges"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Total shortfall (abs): Yearly value
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"${povlines_abs.dollars_text[p]} a day - Total shortfall"
        df_tables.loc[j, "slug"] = f"total_shortfall_{povlines_abs.cents[p]}_year"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of ${povlines_abs.dollars_text[p]} a day. This is the amount of money that would be theoretically needed to lift the {survey_type.text[survey]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables.loc[j, "shortUnit"] = "$"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs.scale_total_shortfall[p]
        df_tables.loc[j, "colorScaleScheme"] = "Oranges"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        df_tables.loc[j, "transform"] = f"multiplyBy total_shortfall_{povlines_abs.cents[p]} 365"
        j += 1

    # Average shortfall ($ per day)
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"${povlines_abs.dollars_text[p]} a day - Average daily shortfall"
        df_tables.loc[j, "slug"] = f"avg_shortfall_{povlines_abs.cents[p]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The average shortfall from a poverty line of ${povlines_abs.dollars_text[p]} a day (averaged across the population in poverty).",
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables.loc[j, "shortUnit"] = "$"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs.scale_avg_shortfall[p]
        df_tables.loc[j, "colorScaleScheme"] = "Purples"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Average shortfall (% of poverty line) [this is the income gap ratio]
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"${povlines_abs.dollars_text[p]} a day - Income gap ratio"
        df_tables.loc[j, "slug"] = f"income_gap_ratio_{povlines_abs.cents[p]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f'The average shortfall from a poverty line of ${povlines_abs.dollars_text[p]} a day (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60;70;80;90;100"
        df_tables.loc[j, "colorScaleScheme"] = "YlOrRd"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Poverty gap index
    for p in range(len(povlines_abs)):
        df_tables.loc[j, "name"] = f"${povlines_abs.dollars_text[p]} a day - Poverty gap index"
        df_tables.loc[j, "slug"] = f"poverty_gap_index_{povlines_abs.cents[p]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The poverty gap index calculated at a poverty line of ${povlines_abs.dollars_text[p]} a day. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                ppp_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs.scale_poverty_gap_index[p]
        df_tables.loc[j, "colorScaleScheme"] = "RdPu"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Headcount ratio (rel)
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"Share below {povlines_rel.percent[pct]} of median"
        df_tables.loc[j, "slug"] = f"headcount_ratio_{povlines_rel.slug_suffix[pct]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {survey_type.text[survey]} per person below {povlines_rel.percent[pct]} of the median.",
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_rel["scale_headcount_ratio"][pct]
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Headcount (rel)
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"Number below {povlines_rel.percent[pct]} of median"
        df_tables.loc[j, "slug"] = f"headcount_{povlines_rel.slug_suffix[pct]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {survey_type.text[survey]} per person below {povlines_rel.percent[pct]} of the median.",
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = np.nan
        df_tables.loc[j, "shortUnit"] = np.nan
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = "100000;300000;1000000;3000000;10000000;30000000;100000000"
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Total shortfall (rel)
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"{povlines_rel.percent[pct]} of median - Total daily shortfall"
        df_tables.loc[j, "slug"] = f"total_shortfall_{povlines_rel.slug_suffix[pct]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of {povlines_rel.text[pct]} {survey_type.text[survey]}. This is the amount of money that would be theoretically needed to lift the {survey_type.text[survey]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = np.nan
        df_tables.loc[j, "shortUnit"] = np.nan
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = (
            "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000;1000000001"
        )
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Total shortfall (rel): Yearly value
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"{povlines_rel.percent[pct]} of median - Total shortfall"
        df_tables.loc[j, "slug"] = f"total_shortfall_{povlines_rel.slug_suffix[pct]}_year"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The total shortfall from a poverty line of {povlines_rel.text[pct]} {survey_type.text[survey]}. This is the amount of money that would be theoretically needed to lift the {survey_type.text[survey]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = np.nan
        df_tables.loc[j, "shortUnit"] = np.nan
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_rel.scale_total_shortfall[pct]
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        df_tables.loc[j, "transform"] = f"multiplyBy total_shortfall_{povlines_rel.slug_suffix[pct]} 365"
        j += 1

    # Average shortfall ($ per day)
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"{povlines_rel.percent[pct]} of median - Average daily shortfall"
        df_tables.loc[j, "slug"] = f"avg_shortfall_{povlines_rel.slug_suffix[pct]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The average shortfall from a poverty line of of {povlines_rel.text[pct]} {survey_type.text[survey]} (averaged across the population in poverty).",
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
        df_tables.loc[j, "shortUnit"] = "$"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_rel.scale_avg_shortfall[pct]
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Average shortfall (% of poverty line) [this is the income gap ratio]
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"{povlines_rel.percent[pct]} of median - Income gap ratio"
        df_tables.loc[j, "slug"] = f"income_gap_ratio_{povlines_rel.slug_suffix[pct]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f'The average shortfall from a poverty line of of {povlines_rel.text[pct]} {survey_type.text[survey]} (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30;35;40;45;50"
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Poverty gap index
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = f"{povlines_rel.percent[pct]} of median - Poverty gap index"
        df_tables.loc[j, "slug"] = f"poverty_gap_index_{povlines_rel.slug_suffix[pct]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The poverty gap index calculated at a poverty line of {povlines_rel.text[pct]} {survey_type.text[survey]}. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                relative_poverty_description,
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = povlines_rel.scale_poverty_gap_index[pct]
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

df_tables["sourceName"] = sourceName
df_tables["dataPublishedBy"] = dataPublishedBy
df_tables["sourceLink"] = sourceLink
df_tables["colorScaleNumericMinValue"] = colorScaleNumericMinValue
df_tables["tolerance"] = tolerance
df_tables["colorScaleEqualSizeBins"] = colorScaleEqualSizeBins

# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# %% [markdown]
# ### Tables for variables showing breaks between surveys
# These variables consider a breaks in the series due to changes in surveys' methodology.

# %%
# Create master table for line breaks
df_spells = pd.DataFrame()
j = 0

for i in range(len(df_tables)):
    # Define country as entityName
    df_spells.loc[j, "master_var"] = df_tables.slug[i]
    df_spells.loc[j, "name"] = "Country"
    df_spells.loc[j, "slug"] = "country"
    df_spells.loc[j, "type"] = "EntityName"
    df_spells.loc[j, "survey_type"] = df_tables.survey_type[i]
    j += 1

    # Define year as Year
    df_spells.loc[j, "master_var"] = df_tables.slug[i]
    df_spells.loc[j, "name"] = "Year"
    df_spells.loc[j, "slug"] = "year"
    df_spells.loc[j, "type"] = "Year"
    df_spells.loc[j, "survey_type"] = df_tables.survey_type[i]
    j += 1
    for c_spell in range(1, CONSUMPTION_SPELLS_PIP + 1):
        df_spells.loc[j, "master_var"] = df_tables.slug[i]
        df_spells.loc[j, "name"] = "Consumption surveys"
        df_spells.loc[j, "slug"] = f"consumption_spell_{c_spell}"
        df_spells.loc[j, "sourceName"] = df_tables.sourceName[i]
        df_spells.loc[j, "description"] = df_tables.description[i]
        df_spells.loc[j, "sourceLink"] = df_tables.sourceLink[i]
        df_spells.loc[j, "dataPublishedBy"] = df_tables.dataPublishedBy[i]
        df_spells.loc[j, "unit"] = df_tables.unit[i]
        df_spells.loc[j, "shortUnit"] = df_tables.shortUnit[i]
        df_spells.loc[j, "tolerance"] = df_tables.tolerance[i]
        df_spells.loc[j, "type"] = df_tables.type[i]
        df_spells.loc[j, "colorScaleNumericMinValue"] = df_tables.colorScaleNumericMinValue[i]
        df_spells.loc[j, "colorScaleNumericBins"] = df_tables.colorScaleNumericBins[i]
        df_spells.loc[j, "colorScaleEqualSizeBins"] = df_tables.colorScaleEqualSizeBins[i]
        df_spells.loc[j, "colorScaleScheme"] = df_tables.colorScaleScheme[i]
        df_spells.loc[j, "survey_type"] = df_tables.survey_type[i]
        j += 1

    for i_spell in range(1, INCOME_SPELLS_PIP + 1):
        df_spells.loc[j, "master_var"] = df_tables.slug[i]
        df_spells.loc[j, "name"] = "Income surveys"
        df_spells.loc[j, "slug"] = f"income_spell_{i_spell}"
        df_spells.loc[j, "sourceName"] = df_tables.sourceName[i]
        df_spells.loc[j, "description"] = df_tables.description[i]
        df_spells.loc[j, "sourceLink"] = df_tables.sourceLink[i]
        df_spells.loc[j, "dataPublishedBy"] = df_tables.dataPublishedBy[i]
        df_spells.loc[j, "unit"] = df_tables.unit[i]
        df_spells.loc[j, "shortUnit"] = df_tables.shortUnit[i]
        df_spells.loc[j, "tolerance"] = df_tables.tolerance[i]
        df_spells.loc[j, "type"] = df_tables.type[i]
        df_spells.loc[j, "colorScaleNumericMinValue"] = df_tables.colorScaleNumericMinValue[i]
        df_spells.loc[j, "colorScaleNumericBins"] = df_tables.colorScaleNumericBins[i]
        df_spells.loc[j, "colorScaleEqualSizeBins"] = df_tables.colorScaleEqualSizeBins[i]
        df_spells.loc[j, "colorScaleScheme"] = df_tables.colorScaleScheme[i]
        df_spells.loc[j, "survey_type"] = df_tables.survey_type[i]
        j += 1

# Delete rows for country and year
df_spells = df_spells[(df_spells["master_var"] != "country") & (df_spells["master_var"] != "year")].reset_index(
    drop=True
)

# Create new rows for total shortfall, which is converted to a yearly value

# Delete rows that have yearly data (there are no files names as such)
df_spells = df_spells[~df_spells["master_var"].str.contains("_year")].reset_index(drop=True)

# Create a new dataframe df_spells_shortfall, which keeps master_var that contains total_shortfall
df_spells_shortfall = df_spells[df_spells["master_var"].str.contains("total_shortfall")].reset_index(drop=True)

# Remove country and year slugs
df_spells_shortfall = df_spells_shortfall[
    (df_spells_shortfall["slug"] != "country") & (df_spells_shortfall["slug"] != "year")
].reset_index(drop=True)

# Create yearly columns
df_spells_shortfall["transform"] = "multiplyBy " + df_spells_shortfall["slug"] + " 365"
df_spells_shortfall["slug"] = df_spells_shortfall["slug"] + "_year"
df_spells_shortfall["description"] = df_spells_shortfall["description"].str.replace("day", "year")

# Concatenate all the spells tables
df_spells = pd.concat([df_spells, df_spells_shortfall], ignore_index=True)

# Make tolerance integer (to not break the parameter in the platform)
df_spells["tolerance"] = df_spells["tolerance"].astype("Int64")

# %% [markdown]
# ## Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by survey type and poverty lines.

# %%
# Grapher table generation

df_graphers = pd.DataFrame()

j = 0

for survey in range(len(survey_type)):
    # Headcount ratio (abs)
    for p in range(len(povlines_abs)):
        df_graphers.loc[j, "title"] = f"{povlines_abs.title_share[p]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_{povlines_abs.cents[p]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_abs.subtitle[p]}"
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Headcount (abs)
    for p in range(len(povlines_abs)):
        df_graphers.loc[j, "title"] = f"{povlines_abs.title_number[p]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_{povlines_abs.cents[p]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_abs.subtitle[p]}"
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Total shortfall (abs)
    for p in range(len(povlines_abs)):
        df_graphers.loc[j, "title"] = f"{povlines_abs.title_total_shortfall[p]}"
        df_graphers.loc[j, "ySlugs"] = f"total_shortfall_{povlines_abs.cents[p]}_year"
        df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_abs.subtitle_total_shortfall[p]}"
        df_graphers.loc[j, "note"] = (
            "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. The cost of closing the poverty gap does not take into account costs and inefficiencies from making the necessary transfers."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Average shortfall ($ per day)
    for p in range(len(povlines_abs)):
        df_graphers.loc[j, "title"] = f"{povlines_abs.title_avg_shortfall[p]}"
        df_graphers.loc[j, "ySlugs"] = f"avg_shortfall_{povlines_abs.cents[p]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($ per day)"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_abs.subtitle_avg_shortfall[p]}"
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Average shortfall (% of poverty line)
    for p in range(len(povlines_abs)):
        df_graphers.loc[j, "title"] = f"{povlines_abs.title_income_gap_ratio[p]}"
        df_graphers.loc[j, "ySlugs"] = f"income_gap_ratio_{povlines_abs.cents[p]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_abs.subtitle_income_gap_ratio[p]}"
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Poverty gap index
    for p in range(len(povlines_abs)):
        df_graphers.loc[j, "title"] = f"Poverty gap index at ${povlines_abs.dollars_text[p]} a day"
        df_graphers.loc[j, "ySlugs"] = f"poverty_gap_index_{povlines_abs.cents[p]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line)."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # MULTIPLE LINES
    # Headcount ratio (abs) - Multiple lines
    df_graphers.loc[j, "title"] = "Share of population living below a range of poverty lines"
    df_graphers.loc[j, "ySlugs"] = (
        "headcount_ratio_100 headcount_ratio_215 headcount_ratio_365 headcount_ratio_685 headcount_ratio_1000 headcount_ratio_2000 headcount_ratio_3000 headcount_ratio_4000"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
    df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Headcount (abs) - Multiple lines
    df_graphers.loc[j, "title"] = "Number of people living below a range of poverty lines"
    df_graphers.loc[j, "ySlugs"] = (
        "headcount_100 headcount_215 headcount_365 headcount_685 headcount_1000 headcount_2000 headcount_3000 headcount_4000"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
    df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Total shortfall (abs) - Multiple lines
    df_graphers.loc[j, "title"] = "Total shortfall from a range of poverty lines"
    df_graphers.loc[j, "ySlugs"] = (
        "total_shortfall_100_year total_shortfall_215_year total_shortfall_365_year total_shortfall_685_year total_shortfall_1000_year total_shortfall_2000_year total_shortfall_3000_year total_shortfall_4000_year"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
    df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. The cost of closing the poverty gap does not take into account costs and inefficiencies from making the necessary transfers."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Average shortfall - $ per day - Multiple lines
    df_graphers.loc[j, "title"] = "Average shortfall from a range of poverty lines"
    df_graphers.loc[j, "ySlugs"] = (
        "avg_shortfall_100 avg_shortfall_215 avg_shortfall_365 avg_shortfall_685 avg_shortfall_1000 avg_shortfall_2000 avg_shortfall_3000 avg_shortfall_4000"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($ per day)"
    df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Average shortfall (% of poverty line) - Multiple lines
    df_graphers.loc[j, "title"] = "Average shortfall from a range of poverty lines (as a share of the poverty line)"
    df_graphers.loc[j, "ySlugs"] = (
        "income_gap_ratio_100 income_gap_ratio_215 income_gap_ratio_365 income_gap_ratio_685 income_gap_ratio_1000 income_gap_ratio_2000 income_gap_ratio_3000 income_gap_ratio_4000"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
    df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Poverty gap index - Multiple lines
    df_graphers.loc[j, "title"] = "Poverty gap index at a range of poverty lines"
    df_graphers.loc[j, "ySlugs"] = (
        "poverty_gap_index_100 poverty_gap_index_215 poverty_gap_index_365 poverty_gap_index_685 poverty_gap_index_1000 poverty_gap_index_2000 poverty_gap_index_3000 poverty_gap_index_4000"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
    df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = ""
    df_graphers.loc[j, "note"] = (
        "This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Headcount ratio (rel)
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"{povlines_rel.title_share[pct]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_{povlines_rel.slug_suffix[pct]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel.dropdown[pct]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {povlines_rel.text[pct]} {survey_type.text[survey]}."
        )
        df_graphers.loc[j, "note"] = (
            f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Headcount (rel)
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"{povlines_rel.title_number[pct]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_{povlines_rel.slug_suffix[pct]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel.dropdown[pct]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {povlines_rel.text[pct]} {survey_type.text[survey]}."
        )
        df_graphers.loc[j, "note"] = (
            f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Total shortfall (rel)
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = (
            f"Total shortfall from a poverty line of {povlines_rel.text[pct]} {survey_type.text[survey]}"
        )
        df_graphers.loc[j, "ySlugs"] = f"total_shortfall_{povlines_rel.slug_suffix[pct]}_year"
        df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel.dropdown[pct]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povlines_rel.text[pct]} {survey_type.text[survey]}."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Average shortfall ($ per day) (rel)
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = (
            f"Average shortfall from a poverty line of {povlines_rel.text[pct]} {survey_type.text[survey]}"
        )
        df_graphers.loc[j, "ySlugs"] = f"avg_shortfall_{povlines_rel.slug_suffix[pct]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($ per day)"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel.dropdown[pct]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povlines_rel.text[pct]} {survey_type.text[survey]}, averaged across the population in poverty."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Average shortfall (% of poverty line) (rel)
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = (
            f"Average shortfall from a poverty line of {povlines_rel.text[pct]} {survey_type.text[survey]} (as a share of the poverty line)"
        )
        df_graphers.loc[j, "ySlugs"] = f"income_gap_ratio_{povlines_rel.slug_suffix[pct]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel.dropdown[pct]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f'This is the average shortfall expressed as a share of the poverty line, sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than {povlines_rel.text[pct]} {survey_type.text[survey]}'
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Poverty gap index (rel)
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"Poverty gap index at {povlines_rel.text[pct]} {survey_type.text[survey]}"
        df_graphers.loc[j, "ySlugs"] = f"poverty_gap_index_{povlines_rel.slug_suffix[pct]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel.dropdown[pct]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line)."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

df_graphers["Show breaks between less comparable surveys Checkbox"] = "false"
# %% [markdown]
# ### Grapher views to show breaks in the curves

# %%
df_graphers_spells = pd.DataFrame()
j = 0

# Create ySlugs dynamically
c_spell_list = []
i_spell_list = []
for c_spell in range(1, CONSUMPTION_SPELLS_PIP + 1):
    c_spell_list.append(f"consumption_spell_{c_spell}")

for i_spell in range(1, INCOME_SPELLS_PIP + 1):
    i_spell_list.append(f"income_spell_{i_spell}")

# Merge the items in the list, separated by a space
spell_list = c_spell_list + i_spell_list

ySlugs_spells = " ".join(spell_list)
ySlugs_spells_year = " ".join([x + "_year" for x in spell_list])

for i in range(len(df_graphers)):
    df_graphers_spells.loc[j, "title"] = df_graphers["title"][i]
    df_graphers_spells.loc[j, "ySlugs"] = ySlugs_spells
    df_graphers_spells.loc[j, "Indicator Dropdown"] = df_graphers["Indicator Dropdown"][i]
    df_graphers_spells.loc[j, "Poverty line Dropdown"] = df_graphers["Poverty line Dropdown"][i]
    df_graphers_spells.loc[j, "Household survey data type Dropdown"] = df_graphers[
        "Household survey data type Dropdown"
    ][i]
    df_graphers_spells.loc[j, "tableSlug"] = df_graphers["survey_type"][i] + "_" + df_graphers["ySlugs"][i]
    df_graphers_spells.loc[j, "subtitle"] = " ".join(
        [
            df_graphers["subtitle"][i],
            "The chart shows breaks in the comparability of the underlying household survey data over time within each country individually.",
        ]
    )

    df_graphers_spells.loc[j, "note"] = df_graphers["note"][i]
    df_graphers_spells.loc[j, "type"] = df_graphers["type"][i]
    df_graphers_spells.loc[j, "yAxisMin"] = df_graphers["yAxisMin"][i]
    df_graphers_spells.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers_spells.loc[j, "hasMapTab"] = "false"
    df_graphers_spells.loc[j, "tab"] = np.nan
    df_graphers_spells.loc[j, "Show breaks between less comparable surveys Checkbox"] = "true"
    j += 1

# Delete spells views for multiple poverty lines
df_graphers_spells = df_graphers_spells[~(df_graphers_spells["Poverty line Dropdown"] == "Multiple lines")].reset_index(
    drop=True
)

# Modify views to be able to see spells for aggregated data

# Add suffix to ySlugs
df_graphers_spells.loc[df_graphers_spells["tableSlug"].str.contains("_year"), ["ySlugs"]] = ySlugs_spells_year

# Remove suffix from tableSlug
df_graphers_spells["tableSlug"] = df_graphers_spells["tableSlug"].str.removesuffix("_year")

df_graphers = pd.concat([df_graphers, df_graphers_spells], ignore_index=True)

# %% [markdown]
# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# %%
# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# When the "Depending on" footnote is introduced, it generates unwanted texts as:
# "Depending on the country and year, the data relates to income measured after taxes and benefits [per capita](#dod:per-capita)."
# "Depending on the country and year, the data relates to consumption [per capita](#dod:per-capita)."

# When int-$ are not included
df_graphers["note"] = df_graphers["note"].str.replace(
    "Depending on the country and year, the data relates to income measured after taxes and benefits [per capita](#dod:per-capita).",
    "The data relates to income measured after taxes and benefits [per capita](#dod:per-capita).",
    regex=False,
)
df_graphers["note"] = df_graphers["note"].str.replace(
    "Depending on the country and year, the data relates to consumption [per capita](#dod:per-capita).",
    "The data relates to consumption [per capita](#dod:per-capita).",
    regex=False,
)

# When int-$ are included
df_graphers["note"] = df_graphers["note"].str.replace(
    "Depending on the country and year, it relates to income measured after taxes and benefits [per capita](#dod:per-capita).",
    "It relates to income measured after taxes and benefits [per capita](#dod:per-capita).",
    regex=False,
)
df_graphers["note"] = df_graphers["note"].str.replace(
    "Depending on the country and year, it relates to consumption [per capita](#dod:per-capita).",
    "It relates to consumption [per capita](#dod:per-capita).",
    regex=False,
)

# Select one default view
df_graphers.loc[
    (df_graphers["ySlugs"] == "headcount_ratio_215")
    & (df_graphers["tableSlug"] == "income_consumption_2017")
    & (df_graphers["Show breaks between less comparable surveys Checkbox"] == "false"),
    ["defaultView"],
] = "true"

# %% [markdown]
# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

# %%
# Define list of variables to iterate: survey types and the list of variables (the latter for spell tables)
survey_list = list(survey_type["table_name"].unique())
var_list = list(df_spells["master_var"].unique())

# Header is converted into a tab-separated text
header_tsv = df_header.to_csv(sep="\t", header=False)

# Auxiliar variable `survey_type` is dropped and graphers table is converted into a tab-separated text
graphers_tsv = df_graphers.drop(columns=["survey_type"])
graphers_tsv = graphers_tsv.to_csv(sep="\t", index=False)

# This table is indented to follow explorers' format
graphers_tsv_indented = textwrap.indent(graphers_tsv, "\t")

# Instead of saving to file, we create the content string
content = header_tsv + "\n" + "graphers\n" + graphers_tsv_indented

# Combine the dataframes, including tables filtered by survey type and variable
for i in survey_list:
    table_tsv = df_tables[df_tables["survey_type"] == i].copy().reset_index(drop=True)
    table_tsv = table_tsv.drop(columns=["survey_type"])
    table_tsv = table_tsv.to_csv(sep="\t", index=False)
    table_tsv_indented = textwrap.indent(table_tsv, "\t")
    content += "\ntable\t" + "https://catalog.ourworldindata.org/explorers/wb/latest/world_bank_pip/" + i + ".csv\t" + i
    content += "\ncolumns\t" + i + "\n" + table_tsv_indented

for var in var_list:
    for i in survey_list:
        table_tsv = (
            df_spells[(df_spells["master_var"] == var) & (df_spells["survey_type"] == i)].copy().reset_index(drop=True)
        )
        table_tsv = table_tsv.drop(columns=["master_var", "survey_type"])
        table_tsv = table_tsv.to_csv(sep="\t", index=False)
        table_tsv_indented = textwrap.indent(table_tsv, "\t")
        content += (
            "\ntable\t"
            + "https://catalog.ourworldindata.org/explorers/wb/latest/world_bank_pip/"
            + i
            + "_"
            + var
            + ".csv\t"
            + i
            + "_"
            + var
        )
        content += "\ncolumns\t" + i + "_" + var + "\n" + table_tsv_indented

# %%
upsert_to_db("poverty-wb", content)
