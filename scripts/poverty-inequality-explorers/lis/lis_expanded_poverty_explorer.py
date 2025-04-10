# %% [markdown]
# # Expanded poverty explorer of the Luxembourg Income Study
# This code creates the tsv file for the expanded poverty explorer from the LIS data, available [here](https://owid.cloud/admin/explorers/preview/lis-expanded-poverty)


import numpy as np
import pandas as pd

from ..common_parameters import *

# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each type of welfare measure or tables considered.

# Read Google sheets
sheet_id = "1UFdwB1iBpP2tEP6GtxCHvW1GGhjsFflh42FWR80rYIg"

# Welfare type sheet
sheet_name = "welfare"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
welfare = pd.read_csv(url, keep_default_na=False)

# Equivalence scales
sheet_name = "equivalence_scales"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
equivalence_scales = pd.read_csv(url, keep_default_na=False, dtype={"checkbox": "str"})

# Absolute povlines
sheet_name = "povlines_abs"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_abs = pd.read_csv(url, keep_default_na=False, dtype={"dollars_text": "str"})

# Relative povlines
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_rel = pd.read_csv(url, keep_default_na=False)

# Tables sheet
sheet_name = "tables"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
tables = pd.read_csv(url, keep_default_na=False)

# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Poverty - Luxembourg Income Study",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Explore Luxembourg Income Study data on poverty.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57755",
    "entityType": "country or region",
    "pickerColumnSlugs": "headcount_ratio_mi_pc_3000 headcount_ratio_dhi_pc_3000 headcount_mi_pc_3000 headcount_dhi_pc_3000 total_shortfall_mi_pc_3000 total_shortfall_dhi_pc_3000 avg_shortfall_mi_pc_3000 avg_shortfall_dhi_pc_3000 income_gap_ratio_mi_pc_3000 income_gap_ratio_dhi_pc_3000 poverty_gap_index_mi_pc_3000 poverty_gap_index_dhi_pc_3000 headcount_ratio_50_median_mi_pc headcount_50_median_mi_pc total_shortfall_50_median_mi_pc avg_shortfall_50_median_mi_pc income_gap_ratio_50_median_mi_pc poverty_gap_index_50_median_mi_pc",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# ## Tables
# Variables are grouped by type of welfare to iterate by different survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.
# ### Tables for variables not showing breaks between surveys
# These variables consider a continous series, without breaks due to changes in surveys' methodology

# Table generation

sourceName = SOURCE_NAME_LIS
dataPublishedBy = DATA_PUBLISHED_BY_LIS
sourceLink = SOURCE_LINK_LIS
colorScaleNumericMinValue = COLOR_SCALE_NUMERIC_MIN_VALUE
tolerance = TOLERANCE
colorScaleEqualSizeBins = COLOR_SCALE_EQUAL_SIZEBINS
new_line = NEW_LINE

yAxisMin = Y_AXIS_MIN

notes_title = NOTES_TITLE_LIS

processing_description = PROCESSING_DESCRIPTION_LIS

processing_poverty = PROCESSING_POVERTY_LIS
processing_gini_mean_median = PROCESSING_GINI_MEAN_MEDIAN_LIS
processing_distribution = PROCESSING_DISTRIBUTION_LIS

ppp_description = PPP_DESCRIPTION_LIS
relative_poverty_description = RELATIVE_POVERTY_DESCRIPTION_LIS

df_tables = pd.DataFrame()
j = 0

for tab in range(len(tables)):
    # Define country as entityName
    df_tables.loc[j, "name"] = "Country"
    df_tables.loc[j, "slug"] = "country"
    df_tables.loc[j, "type"] = "EntityName"
    j += 1

    # Define year as Year
    df_tables.loc[j, "name"] = "Year"
    df_tables.loc[j, "slug"] = "year"
    df_tables.loc[j, "type"] = "Year"
    j += 1

    for wel in range(len(welfare)):
        for eq in range(len(equivalence_scales)):
            # Headcount ratio (abs)
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Share below ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"% of population living in households with {welfare['welfare_type'][wel]} below ${povlines_abs['dollars_text'][p]} a day.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "3;10;20;30;40;50;60;70;80;90;100"
                df_tables.loc[j, "colorScaleScheme"] = "OrRd"
                j += 1

            # Headcount (abs)
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Number below ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"Number of people living in households with {welfare['welfare_type'][wel]} below ${povlines_abs['dollars_text'][p]} a day.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = np.nan
                df_tables.loc[j, "shortUnit"] = np.nan
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = (
                    "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000"
                )
                df_tables.loc[j, "colorScaleScheme"] = "Reds"
                j += 1

            # Total shortfall (abs)
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Total shortfall - ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs.cents[p]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The total shortfall from a poverty line of ${povlines_abs['dollars_text'][p]} a day. This is the amount of money that would be theoretically needed to lift the {welfare['welfare_type'][wel]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs["scale_total_shortfall"][p]
                df_tables.loc[j, "colorScaleScheme"] = "Oranges"
                j += 1

            # Average shortfall ($)
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Average shortfall - ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of ${povlines_abs['dollars_text'][p]} (averaged across the population in poverty).",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs["scale_avg_shortfall"][p]
                df_tables.loc[j, "colorScaleScheme"] = "Purples"
                j += 1

            # Average shortfall ($): Daily value
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Average shortfall - ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}_day"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of ${povlines_abs['dollars_text'][p]} (averaged across the population in poverty).",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs["scale_avg_shortfall"][p]
                df_tables.loc[j, "colorScaleScheme"] = "Purples"
                df_tables.loc[j, "transform"] = (
                    f"multiplyBy avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]} 0.00274"
                )
                j += 1

            # Average shortfall (% of poverty line) [this is the income gap ratio]
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Income gap ratio - ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f'The average shortfall from a poverty line of ${povlines_abs.dollars_text[p]} a day (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "10;20;30;40;50;60;70;80;90;100"
                df_tables.loc[j, "colorScaleScheme"] = "YlOrRd"
                j += 1

            # Poverty gap index
            for p in range(len(povlines_abs)):
                df_tables.loc[j, "name"] = (
                    f"Poverty gap index - ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The poverty gap index calculated at a poverty line of ${povlines_abs['dollars_text'][p]} a day. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = povlines_abs[
                    f"scale_poverty_gap_index_{welfare['slug'][wel]}"
                ][p]
                df_tables.loc[j, "colorScaleScheme"] = "RdPu"
                j += 1

            # Headcount ratio (rel)
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Share below {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"headcount_ratio_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"% of population living in households with {welfare['welfare_type'][wel]} below {povlines_rel['percent'][pct]} of the median {welfare['welfare_type'][wel]}.",
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30"
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                j += 1

            # Headcount (rel)
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Number below {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"headcount_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"Number of people living in households with {welfare['welfare_type'][wel]} below {povlines_rel['percent'][pct]} of the median {welfare['welfare_type'][wel]}.",
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = np.nan
                df_tables.loc[j, "shortUnit"] = np.nan
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = (
                    "100000;300000;1000000;3000000;10000000;30000000;100000000;300000000;1000000000"
                )
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                j += 1

            # Total shortfall (rel)
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Total shortfall - {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"total_shortfall_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The total shortfall from a poverty line of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]}. This is the amount of money that would be theoretically needed to lift the {welfare['welfare_type'][wel]} of all people in poverty up to the poverty line. However this is not a measure of the actual cost of eliminating poverty, since it does not take into account the costs involved in making the necessary transfers nor any changes in behaviour they would bring about.",
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = np.nan
                df_tables.loc[j, "shortUnit"] = np.nan
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = povlines_rel["scale_total_shortfall"][pct]
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                j += 1

            # Average shortfall ($)
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Average shortfall - {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"avg_shortfall_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]} (averaged across the population in poverty).",
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "1000;2000;3000;4000;5000"
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                j += 1

            # Average shortfall ($): Daily value
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Average shortfall - {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"avg_shortfall_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_day"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The average shortfall from a poverty line of of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]} (averaged across the population in poverty).",
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;20.0001"
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                df_tables.loc[j, "transform"] = (
                    f"multiplyBy avg_shortfall_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} 0.00274"
                )
                j += 1

            # Average shortfall (% of poverty line) [this is the income gap ratio]
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Income gap ratio - {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"income_gap_ratio_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f'The average shortfall from a poverty line of of {povlines_rel.text[pct]} {welfare.welfare_type[wel]} (averaged across the population in poverty) expressed as a share of the poverty line. This metric is sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than the poverty line.',
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30;35;40"
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                j += 1

            # Poverty gap index
            for pct in range(len(povlines_rel)):
                df_tables.loc[j, "name"] = (
                    f"Poverty gap index - {povlines_rel['percent'][pct]} of median ({welfare['title'][wel]})"
                )
                df_tables.loc[j, "slug"] = (
                    f"poverty_gap_index_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The poverty gap index calculated at a poverty line of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]}. The poverty gap index is a measure that reflects both the depth and prevalence of poverty. It is defined as the mean shortfall of the total population from the poverty line counting the non-poor as having zero shortfall and expressed as a percentage of the poverty line. It is worth unpacking that definition a little. For those below the poverty line, the shortfall corresponds to the amount of money required in order to reach the poverty line. For those at or above the poverty line, the shortfall is counted as zero. The average shortfall is then calculated across the total population – both poor and non-poor – and then expressed as a share of the poverty line. Unlike the more commonly-used metric of the headcount ratio, the poverty gap index is thus sensitive not only to whether a person’s income falls below the poverty line or not, but also by how much – i.e. to the depth of poverty they experience.",
                        relative_poverty_description,
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_poverty,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = "2;4;6;8;10;12"
                df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
                j += 1

    df_tables["tableSlug"] = tables["name"][tab]

df_tables["sourceName"] = sourceName
df_tables["dataPublishedBy"] = dataPublishedBy
df_tables["sourceLink"] = sourceLink
df_tables["colorScaleNumericMinValue"] = colorScaleNumericMinValue
df_tables["tolerance"] = tolerance
df_tables["colorScaleEqualSizeBins"] = colorScaleEqualSizeBins

# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# ### Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by welfare type.

# Grapher table generation

df_graphers = pd.DataFrame()

j = 0

for tab in range(len(tables)):
    for eq in range(len(equivalence_scales)):
        for wel in range(len(welfare)):
            # Headcount ratio (abs)
            for p in range(len(povlines_abs)):
                df_graphers.loc[j, "title"] = f"{povlines_abs['title_share'][p]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"{povlines_abs['subtitle'][p]} {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Headcount (abs)
            for p in range(len(povlines_abs)):
                df_graphers.loc[j, "title"] = f"{povlines_abs.title_number[p]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"{povlines_abs['subtitle'][p]} {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Total shortfall (abs)
            for p in range(len(povlines_abs)):
                df_graphers.loc[j, "title"] = f"{povlines_abs['title_total_shortfall'][p]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs.cents[p]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"{povlines_abs['subtitle_total_shortfall'][p]} {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. The cost of closing the poverty gap does not take into account costs and inefficiencies from making the necessary transfers."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Average shortfall ($)
            for p in range(len(povlines_abs)):
                df_graphers.loc[j, "title"] = f"{povlines_abs['title_avg_shortfall'][p]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}_day"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"{povlines_abs['subtitle_avg_shortfall'][p]} {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Average shortfall (% of poverty line)
            for p in range(len(povlines_abs)):
                df_graphers.loc[j, "title"] = f"{povlines_abs['title_income_gap_ratio'][p]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"{povlines_abs['subtitle_income_gap_ratio'][p]} {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Poverty gap index
            for p in range(len(povlines_abs)):
                df_graphers.loc[j, "title"] = (
                    f"Poverty gap index at ${povlines_abs['dollars_text'][p]} a day ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line). {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # MULTIPLE LINES
            # Headcount ratio (abs) - Multiple lines
            df_graphers.loc[j, "title"] = (
                f"Share of population living below a range of poverty lines ({welfare['title'][wel]})"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_100 headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_200 headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_500 headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_1000 headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_2000 headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_3000 headcount_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_4000"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This data is adjusted for inflation and for differences in living costs between countries. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

            # Headcount (abs) - Multiple lines
            df_graphers.loc[j, "title"] = (
                f"Number of people living below a range of poverty lines ({welfare['title'][wel]})"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_100 headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_200 headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_500 headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_1000 headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_2000 headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_3000 headcount_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_4000"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This data is adjusted for inflation and for differences in living costs between countries. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

            # Total shortfall (abs) - Multiple lines

            df_graphers.loc[j, "title"] = f"Total shortfall from a range of poverty lines ({welfare['title'][wel]})"
            df_graphers.loc[j, "ySlugs"] = (
                f"total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_100 total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_200 total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_500 total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_1000 total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_2000 total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_3000 total_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_4000"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
            df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This data is adjusted for inflation and for differences in living costs between countries. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. The cost of closing the poverty gap does not take into account costs and inefficiencies from making the necessary transfers."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

            # Average shortfall ($) - Multiple lines
            df_graphers.loc[j, "title"] = f"Average shortfall from a range of poverty lines ({welfare['title'][wel]})"
            df_graphers.loc[j, "ySlugs"] = (
                f"avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_100_day avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_200_day avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_500_day avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_1000_day avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_2000_day avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_3000_day avg_shortfall_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_4000_day"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
            df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This data is adjusted for inflation and for differences in living costs between countries. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

            # Average shortfall (% of poverty line) - Multiple lines
            df_graphers.loc[j, "title"] = (
                f"Average shortfall from a range of poverty lines (as a share of the poverty line) ({welfare['title'][wel]})"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_100 income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_200 income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_500 income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_1000 income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_2000 income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_3000 income_gap_ratio_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_4000"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
            df_graphers.loc[j, "Poverty line Dropdown"] = "Multiple lines"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = f"{welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

            # Poverty gap index - Multiple lines
            df_graphers.loc[j, "title"] = f"Poverty gap index at a range of poverty lines ({welfare['title'][wel]})"
            df_graphers.loc[j, "ySlugs"] = (
                f"poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_100 poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_200 poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_500 poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_1000 poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_2000 poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_3000 poverty_gap_index_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_4000"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"Multiple lines"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = f"{welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

            # RELATIVE POVERTY
            # Headcount ratio (rel)
            for pct in range(len(povlines_rel)):
                df_graphers.loc[j, "title"] = f"{povlines_rel['title_share'][pct]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"headcount_ratio_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]}. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = ""
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Headcount (rel)
            for pct in range(len(povlines_rel)):
                df_graphers.loc[j, "title"] = f"{povlines_rel['title_number'][pct]} ({welfare['title'][wel]})"
                df_graphers.loc[j, "ySlugs"] = (
                    f"headcount_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]}. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = ""
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Total shortfall (rel)
            for pct in range(len(povlines_rel)):
                df_graphers.loc[j, "title"] = (
                    f"Total shortfall from a poverty line of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"total_shortfall_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povlines_rel.text[pct]} {welfare['welfare_type'][wel]}. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Average shortfall ($) (rel)
            for pct in range(len(povlines_rel)):
                df_graphers.loc[j, "title"] = (
                    f"Average shortfall from a poverty line of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"avg_shortfall_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}_day"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]}, averaged across the population in poverty. {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Average shortfall (% of poverty line) (rel)
            for pct in range(len(povlines_rel)):
                df_graphers.loc[j, "title"] = (
                    f"Average shortfall from a poverty line of {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]} (as a share of the poverty line) ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"income_gap_ratio_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f'This is the average shortfall expressed as a share of the poverty line, sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than {povlines_rel.text[pct]} {welfare.welfare_type[wel]}. {welfare.subtitle[wel]} {equivalence_scales.note[eq]}'
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Poverty gap index (rel)
            for pct in range(len(povlines_rel)):
                df_graphers.loc[j, "title"] = (
                    f"Poverty gap index at {povlines_rel['text'][pct]} {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"poverty_gap_index_{povlines_rel['slug_suffix'][pct]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
                df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line). {welfare['subtitle'][wel]} {equivalence_scales['subtitle'][eq]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
                )
                df_graphers.loc[j, "type"] = np.nan
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

        # BEFORE VS. AFTER TAX
        # Headcount ratio (abs)
        for p in range(len(povlines_abs)):
            df_graphers.loc[j, "title"] = f"{povlines_abs['title_share'][p]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"headcount_ratio_mi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]} headcount_ratio_dhi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = f"{povlines_abs['subtitle'][p]} {equivalence_scales['subtitle'][eq]}"
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Headcount (abs)
        for p in range(len(povlines_abs)):
            df_graphers.loc[j, "title"] = f"{povlines_abs.title_number[p]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"headcount_mi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]} headcount_dhi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = f"{povlines_abs['subtitle'][p]} {equivalence_scales['subtitle'][eq]}"
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Total shortfall (abs)
        for p in range(len(povlines_abs)):
            df_graphers.loc[j, "title"] = f"{povlines_abs['title_total_shortfall'][p]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"total_shortfall_mi_{equivalence_scales['slug'][eq]}_{povlines_abs.cents[p]} total_shortfall_dhi_{equivalence_scales['slug'][eq]}_{povlines_abs.cents[p]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"{povlines_abs['subtitle_total_shortfall'][p]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                "This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. The cost of closing the poverty gap does not take into account costs and inefficiencies from making the necessary transfers."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Average shortfall ($)
        for p in range(len(povlines_abs)):
            df_graphers.loc[j, "title"] = f"{povlines_abs['title_avg_shortfall'][p]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"avg_shortfall_mi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}_day avg_shortfall_dhi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}_day"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"{povlines_abs['subtitle_avg_shortfall'][p]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Average shortfall (% of poverty line)
        for p in range(len(povlines_abs)):
            df_graphers.loc[j, "title"] = f"{povlines_abs['title_income_gap_ratio'][p]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"income_gap_ratio_mi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]} income_gap_ratio_dhi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs.povline_dropdown[p]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"{povlines_abs['subtitle_income_gap_ratio'][p]} {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Poverty gap index
        for p in range(len(povlines_abs)):
            df_graphers.loc[j, "title"] = (
                f"Poverty gap index at ${povlines_abs['dollars_text'][p]} a day (After vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"poverty_gap_index_mi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]} poverty_gap_index_dhi_{equivalence_scales['slug'][eq]}_{povlines_abs['cents'][p]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_abs['povline_dropdown'][p]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line). {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Headcount ratio (rel)
        for pct in range(len(povlines_rel)):
            df_graphers.loc[j, "title"] = f"{povlines_rel['title_share'][pct]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"headcount_ratio_{povlines_rel['slug_suffix'][pct]}_mi_{equivalence_scales['slug'][eq]} headcount_ratio_{povlines_rel['slug_suffix'][pct]}_dhi_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {povlines_rel['text'][pct]} income. {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = ""
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Headcount (rel)
        for pct in range(len(povlines_rel)):
            df_graphers.loc[j, "title"] = f"{povlines_rel['title_number'][pct]} (After vs. before tax)"
            df_graphers.loc[j, "ySlugs"] = (
                f"headcount_{povlines_rel['slug_suffix'][pct]}_mi_{equivalence_scales['slug'][eq]} headcount_{povlines_rel['slug_suffix'][pct]}_dhi_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at {povlines_rel['text'][pct]} income. {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = ""
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Total shortfall (rel)
        for pct in range(len(povlines_rel)):
            df_graphers.loc[j, "title"] = (
                f"Total shortfall from a poverty line of {povlines_rel['text'][pct]} income (After vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"total_shortfall_{povlines_rel['slug_suffix'][pct]}_mi_{equivalence_scales['slug'][eq]} total_shortfall_{povlines_rel['slug_suffix'][pct]}_dhi_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Total shortfall from poverty line"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povlines_rel.text[pct]} income. {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Average shortfall ($) (rel)
        for pct in range(len(povlines_rel)):
            df_graphers.loc[j, "title"] = (
                f"Average shortfall from a poverty line of {povlines_rel['text'][pct]} income (After vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"avg_shortfall_{povlines_rel['slug_suffix'][pct]}_mi_{equivalence_scales['slug'][eq]}_day avg_shortfall_{povlines_rel['slug_suffix'][pct]}_dhi_{equivalence_scales['slug'][eq]}_day"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall ($)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This is the amount of money that would be theoretically needed to lift the incomes of all people in poverty up to {povlines_rel['text'][pct]} income, averaged across the population in poverty. {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Average shortfall (% of poverty line) (rel)
        for pct in range(len(povlines_rel)):
            df_graphers.loc[j, "title"] = (
                f"Average shortfall from a poverty line of {povlines_rel['text'][pct]} income (as a share of the poverty line) (After vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"income_gap_ratio_{povlines_rel['slug_suffix'][pct]}_mi_{equivalence_scales['slug'][eq]} income_gap_ratio_{povlines_rel['slug_suffix'][pct]}_dhi_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Average shortfall (% of poverty line)"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f'This is the average shortfall expressed as a share of the poverty line, sometimes called the "income gap ratio". It captures the depth of poverty of those living on less than {povlines_rel.text[pct]} income. {equivalence_scales.note[eq]}'
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # Poverty gap index (rel)
        for pct in range(len(povlines_rel)):
            df_graphers.loc[j, "title"] = (
                f"Poverty gap index at {povlines_rel['text'][pct]} income (After vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"poverty_gap_index_{povlines_rel['slug_suffix'][pct]}_mi_{equivalence_scales['slug'][eq]} poverty_gap_index_{povlines_rel['slug_suffix'][pct]}_dhi_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Poverty gap index"
            df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_rel['dropdown'][pct]}"
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"The poverty gap index is a poverty measure that reflects both the prevalence and the depth of poverty. It is calculated as the share of population in poverty multiplied by the average shortfall from the poverty line (expressed as a % of the poverty line). {equivalence_scales['subtitle'][eq]}"
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

    df_graphers["tableSlug"] = tables["name"][tab]

# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# Add yAxisMin
df_graphers["yAxisMin"] = yAxisMin

# Select one default view
df_graphers.loc[
    (df_graphers["Indicator Dropdown"] == "Share in poverty")
    & (df_graphers["Poverty line Dropdown"] == "$30 per day")
    & (df_graphers["Income measure Dropdown"] == "After tax")
    & (df_graphers["Adjust for cost sharing within households (equivalized income) Checkbox"] == "false"),
    ["defaultView"],
] = "true"


# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

save("poverty-lis", tables, df_header, df_graphers, df_tables)  # type: ignore
