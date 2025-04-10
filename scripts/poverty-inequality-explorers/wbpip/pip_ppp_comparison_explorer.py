# TODO: Change the slug of this explorer
# # Poverty Data Explorer of World Bank data: 2017 vs 2021 prices
# This code creates the tsv file for the PPP comparison explorer from the World Bank PIP data, available [here](https://ourworldindata.org/explorers/poverty-explorer-ppp-comparison)

import textwrap

import numpy as np
import pandas as pd

from ..common_parameters import *

# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each poverty line, both prices together, relative poverty or survey type.

# Define PPP versions
ppp_year_old = PPP_VERSION_OLD_PIP
ppp_year_current = PPP_VERSION_CURRENT_PIP

# Read Google sheets
sheet_id = "1mR0LPEGlY-wCp1q9lNTlDbVIG65JazKvHL16my9tH8Y"

# Poverty lines in old prices sheet
sheet_name = f"povlines_ppp{ppp_year_old}"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_ppp_old = pd.read_csv(url, dtype={"dollars_text": "str"})

# Poverty lines in current prices sheet
sheet_name = f"povlines_ppp{ppp_year_current}"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_ppp_current = pd.read_csv(url, dtype={"dollars_text": "str"})

# Poverty lines in both prices sheet
sheet_name = "povlines_both"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_both = pd.read_csv(
    url, dtype={f"dollars_{ppp_year_old}_text": "str", f"dollars_{ppp_year_current}_text": "str"}
)

# Relative poverty lines sheet
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
povlines_rel = pd.read_csv(url)

# Survey type sheet
sheet_name = "survey_type"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
survey_type = pd.read_csv(url)

# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": f"Poverty - World Bank {ppp_year_old} vs. {ppp_year_current} prices",
    "selection": ["Mozambique", "Nigeria", "Kenya", "Bangladesh", "Bolivia", "World"],
    "explorerSubtitle": f"Compare key poverty indicators from World Bank data in {ppp_year_old} and {ppp_year_current} prices.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57756",
    "entityType": "country or region",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# ## Tables
# Variables are grouped by type to iterate by different poverty lines and survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.

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

processing_description = PROCESSING_DESCRIPTION_PIP_PPP_COMPARISON
ppp_description_current = PPP_DESCRIPTION_PIP_CURRENT
ppp_description_old = PPP_DESCRIPTION_PIP_OLD
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

    # Shares (ppp_year_old)
    for p_old in range(len(povlines_ppp_old)):
        df_tables.loc[j, "name"] = (
            f"Share of population below ${povlines_ppp_old.dollars_text[p_old]} a day ({ppp_year_old} prices)"
        )

        df_tables.loc[j, "slug"] = f"headcount_ratio_{povlines_ppp_old.cents[p_old]}_ppp{ppp_year_old}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {survey_type.text[survey]} per person below ${povlines_ppp_old.dollars_text[p_old]} a day ({ppp_year_old} prices).",
                ppp_description_old,
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

    # Shares ({ppp_year_current})
    for p_current in range(len(povlines_ppp_current)):
        df_tables.loc[j, "name"] = (
            f"Share of population below ${povlines_ppp_current.dollars_text[p_current]} a day ({ppp_year_current} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_ratio_{povlines_ppp_current.cents[p_current]}_ppp{ppp_year_current}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {survey_type.text[survey]} per person below ${povlines_ppp_current.dollars_text[p_current]} a day ({ppp_year_current} prices).",
                ppp_description_current,
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

    # Number ({ppp_year_old})
    for p_old in range(len(povlines_ppp_old)):
        df_tables.loc[j, "name"] = (
            f"Number of people below ${povlines_ppp_old.dollars_text[p_old]} a day ({ppp_year_old} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_{povlines_ppp_old.cents[p_old]}_ppp{ppp_year_old}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {survey_type.text[survey]} per person below ${povlines_ppp_old.dollars_text[p_old]} a day ({ppp_year_old} prices).",
                ppp_description_old,
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
        df_tables.loc[j, "colorScaleScheme"] = "Reds"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Number ({ppp_year_current})
    for p_current in range(len(povlines_ppp_current)):
        df_tables.loc[j, "name"] = (
            f"Number of people below ${povlines_ppp_current.dollars_text[p_current]} a day ({ppp_year_current} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_{povlines_ppp_current.cents[p_current]}_ppp{ppp_year_current}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {survey_type.text[survey]} per person below ${povlines_ppp_current.dollars_text[p_current]} a day ({ppp_year_current} prices).",
                ppp_description_current,
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
        df_tables.loc[j, "colorScaleScheme"] = "Reds"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Share (relative, {ppp_year_old})
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = (
            f"{povlines_rel.percent[pct]} of median - share of population below poverty line ({ppp_year_old} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_ratio_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_old}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {survey_type.text[survey]} per person below {povlines_rel.percent[pct]} of the median ({ppp_year_old} prices).",
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
        df_tables.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30;30.0001"
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Share (relative, {ppp_year_current})
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = (
            f"{povlines_rel.percent[pct]} of median - share of population below poverty line ({ppp_year_current} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_ratio_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_current}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"% of population living in households with an {survey_type.text[survey]} per person below {povlines_rel.percent[pct]} of the median ({ppp_year_current} prices).",
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
        df_tables.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30;30.0001"
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Number (relative, {ppp_year_old})
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = (
            f"{povlines_rel.percent[pct]} of median - total number of people below poverty line ({ppp_year_old} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_old}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {survey_type.text[survey]} per person below {povlines_rel.percent[pct]} of the median ({ppp_year_old} prices).",
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
        df_tables.loc[j, "colorScaleScheme"] = "YlOrRd"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Number (relative, {ppp_year_current})
    for pct in range(len(povlines_rel)):
        df_tables.loc[j, "name"] = (
            f"{povlines_rel.percent[pct]} of median - total number of people below poverty line ({ppp_year_current} prices)"
        )
        df_tables.loc[j, "slug"] = f"headcount_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_current}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"Number of people living in households with an {survey_type.text[survey]} per person below {povlines_rel.percent[pct]} of the median ({ppp_year_current} prices).",
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
        df_tables.loc[j, "colorScaleScheme"] = "YlOrRd"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Mean ({ppp_year_old})
    df_tables.loc[j, "name"] = f"Mean {survey_type.text[survey]} per day ({ppp_year_old} prices)"
    df_tables.loc[j, "slug"] = f"mean_ppp{ppp_year_old}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The mean level of {survey_type.text[survey]} per day ({ppp_year_old} prices).",
            ppp_description_old,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_old} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;50.0001"
    df_tables.loc[j, "colorScaleScheme"] = "BuGn"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Mean ({ppp_year_current})
    df_tables.loc[j, "name"] = f"Mean {survey_type.text[survey]} per day ({ppp_year_current} prices)"
    df_tables.loc[j, "slug"] = f"mean_ppp{ppp_year_current}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The mean level of {survey_type.text[survey]} per day ({ppp_year_current} prices).",
            ppp_description_current,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_current} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;50.0001"
    df_tables.loc[j, "colorScaleScheme"] = "BuGn"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Median ({ppp_year_old})
    df_tables.loc[j, "name"] = f"Median {survey_type.text[survey]} per day ({ppp_year_old} prices)"
    df_tables.loc[j, "slug"] = f"median_ppp{ppp_year_old}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per day below which half of the population live ({ppp_year_old} prices).",
            ppp_description_old,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_old} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;50.0001"
    df_tables.loc[j, "colorScaleScheme"] = "BuGn"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Median ({ppp_year_current})
    df_tables.loc[j, "name"] = f"Median {survey_type.text[survey]} per day ({ppp_year_current} prices)"
    df_tables.loc[j, "slug"] = f"median_ppp{ppp_year_current}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per day below which half of the population live ({ppp_year_current} prices).",
            ppp_description_current,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_current} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;50.0001"
    df_tables.loc[j, "colorScaleScheme"] = "BuGn"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P10 ({ppp_year_old})
    df_tables.loc[j, "name"] = f"P10 ({ppp_year_old} prices)"
    df_tables.loc[j, "slug"] = f"decile1_thr_ppp{ppp_year_old}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per day below which 10% of the population falls ({ppp_year_old} prices).",
            ppp_description_old,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_old} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;20.0001"
    df_tables.loc[j, "colorScaleScheme"] = "Greens"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P10 ({ppp_year_current})
    df_tables.loc[j, "name"] = f"P10 ({ppp_year_current} prices)"
    df_tables.loc[j, "slug"] = f"decile1_thr_ppp{ppp_year_current}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per day below which 10% of the population falls ({ppp_year_current} prices).",
            ppp_description_current,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = "international-$ in {ppp_year_current} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;20.0001"
    df_tables.loc[j, "colorScaleScheme"] = "Greens"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P90 ({ppp_year_old})
    df_tables.loc[j, "name"] = f"P90 ({ppp_year_old} prices)"
    df_tables.loc[j, "slug"] = f"decile9_thr_ppp{ppp_year_old}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per day below which 90% of the population falls ({ppp_year_old} prices).",
            ppp_description_old,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_old} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "5;10;20;50;100;100.0001"
    df_tables.loc[j, "colorScaleScheme"] = "Blues"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P90 ({ppp_year_current})
    df_tables.loc[j, "name"] = f"P90 ({ppp_year_current} prices)"
    df_tables.loc[j, "slug"] = f"decile9_thr_ppp{ppp_year_current}"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per day below which 90% of the population falls ({ppp_year_current} prices).",
            ppp_description_current,
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = f"international-$ in {ppp_year_current} prices"
    df_tables.loc[j, "shortUnit"] = "$"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericBins"] = "5;10;20;50;100;100.0001"
    df_tables.loc[j, "colorScaleScheme"] = "Blues"
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

# ## Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by survey type and poverty lines.

# Grapher table generation

df_graphers = pd.DataFrame()

j = 0

for survey in range(len(survey_type)):
    # Share ({ppp_year_old})
    for p_old in range(len(povlines_ppp_old)):
        df_graphers.loc[j, "title"] = f"{povlines_ppp_old.title_share[p_old]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_{povlines_ppp_old.cents[p_old]}_ppp{ppp_year_old}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_ppp_old.povline_dropdown[p_old]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_ppp_old.subtitle[p_old]}"
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_old} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Share ({ppp_year_current})
    for p_current in range(len(povlines_ppp_current)):
        df_graphers.loc[j, "title"] = f"{povlines_ppp_current.title_share[p_current]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_{povlines_ppp_current.cents[p_current]}_ppp{ppp_year_current}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_ppp_current.povline_dropdown[p_current]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_ppp_current.subtitle[p_current]}"
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_current} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Number ({ppp_year_old})
    for p_old in range(len(povlines_ppp_old)):
        df_graphers.loc[j, "title"] = f"{povlines_ppp_old.title_number[p_old]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_{povlines_ppp_old.cents[p_old]}_ppp{ppp_year_old}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_ppp_old.povline_dropdown[p_old]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_ppp_old.subtitle[p_old]}"
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_old} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Number ({ppp_year_current})
    for p_current in range(len(povlines_ppp_current)):
        df_graphers.loc[j, "title"] = f"{povlines_ppp_current.title_number[p_current]}"
        df_graphers.loc[j, "ySlugs"] = f"headcount_{povlines_ppp_current.cents[p_current]}_ppp{ppp_year_current}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_ppp_current.povline_dropdown[p_current]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_ppp_current.subtitle[p_current]}"
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_current} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Share (both)
    for p in range(len(povlines_both)):
        df_graphers.loc[j, "title"] = f"{povlines_both.title_share[p]}"
        df_graphers.loc[j, "ySlugs"] = (
            f"headcount_ratio_{povlines_both.cents_old[p]}_ppp{ppp_year_old} headcount_ratio_{povlines_both.cents_current[p]}_ppp{ppp_year_current}"
        )
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_both.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_both.subtitle[p]}"
        df_graphers.loc[j, "note"] = (
            f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
        df_graphers.loc[j, "hasMapTab"] = np.nan
        df_graphers.loc[j, "tab"] = np.nan
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Number (both)
    for p in range(len(povlines_both)):
        df_graphers.loc[j, "title"] = f"{povlines_both.title_number[p]}"
        df_graphers.loc[j, "ySlugs"] = (
            f"headcount_{povlines_both.cents_old[p]}_ppp{ppp_year_old} headcount_{povlines_both.cents_current[p]}_ppp{ppp_year_current}"
        )
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
        df_graphers.loc[j, "Poverty line Dropdown"] = f"{povlines_both.povline_dropdown[p]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = f"{povlines_both.subtitle[p]}"
        df_graphers.loc[j, "note"] = (
            f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
        df_graphers.loc[j, "hasMapTab"] = np.nan
        df_graphers.loc[j, "tab"] = np.nan
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Share (relative, {ppp_year_old})
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"{povlines_rel.title_share[pct]} ({ppp_year_old} prices)"
        df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_old}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
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

    # Share (relative, {ppp_year_current})
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"{povlines_rel.title_share[pct]} ({ppp_year_current} prices)"
        df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_current}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
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

    # Number (relative, {ppp_year_old})
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"{povlines_rel.title_number[pct]} ({ppp_year_old} prices)"
        df_graphers.loc[j, "ySlugs"] = f"headcount_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_old}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
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

    # Number (relative, {ppp_year_current})
    for pct in range(len(povlines_rel)):
        df_graphers.loc[j, "title"] = f"{povlines_rel.title_number[pct]} ({ppp_year_current} prices)"
        df_graphers.loc[j, "ySlugs"] = f"headcount_{povlines_rel.slug_suffix[pct]}_ppp{ppp_year_current}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
        df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
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

    # Share (relative, both)
    df_graphers.loc[j, "title"] = (
        f"Relative poverty: Share of people below 60% of the median ({ppp_year_old} vs. {ppp_year_current} prices)"
    )
    df_graphers.loc[j, "ySlugs"] = (
        f"headcount_ratio_60_median_ppp{ppp_year_old} headcount_ratio_60_median_ppp{ppp_year_current}"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Share in poverty"
    df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = f"Relative poverty: 60% of median"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at 60% of the median {survey_type.text[survey]}."
    )
    df_graphers.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = np.nan
    df_graphers.loc[j, "tab"] = np.nan
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Number (relative, both)
    df_graphers.loc[j, "title"] = (
        f"Relative poverty: Number of people below 60% of the median ({ppp_year_old} vs. {ppp_year_current} prices)"
    )
    df_graphers.loc[j, "ySlugs"] = f"headcount_60_median_ppp{ppp_year_old} headcount_60_median_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Number in poverty"
    df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = f"Relative poverty: 60% of median"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"Relative poverty is measured in terms of a poverty line that rises and falls over time with average incomes – in this case set at 60% of the median {survey_type.text[survey]}."
    )
    df_graphers.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = np.nan
    df_graphers.loc[j, "tab"] = np.nan
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Mean ({ppp_year_old})
    df_graphers.loc[j, "title"] = f"Mean {survey_type.text[survey]} per day ({ppp_year_old} prices)"
    df_graphers.loc[j, "ySlugs"] = f"mean_ppp{ppp_year_old}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_old} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Mean ({ppp_year_current})
    df_graphers.loc[j, "title"] = f"Mean {survey_type.text[survey]} per day ({ppp_year_current} prices)"
    df_graphers.loc[j, "ySlugs"] = f"mean_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_current} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Mean (both)
    df_graphers.loc[j, "title"] = (
        f"Mean {survey_type.text[survey]} per day: {ppp_year_old} vs. {ppp_year_current} prices"
    )
    df_graphers.loc[j, "ySlugs"] = f"mean_ppp{ppp_year_old} mean_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption"
    df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = np.nan
    df_graphers.loc[j, "tab"] = np.nan
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Median ({ppp_year_old})
    df_graphers.loc[j, "title"] = f"Median {survey_type.text[survey]} per day ({ppp_year_old} prices)"
    df_graphers.loc[j, "ySlugs"] = f"median_ppp{ppp_year_old}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Median income or consumption"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_old} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Median ({ppp_year_current})
    df_graphers.loc[j, "title"] = f"Median {survey_type.text[survey]} per day ({ppp_year_current} prices)"
    df_graphers.loc[j, "ySlugs"] = f"median_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Median income or consumption"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_current} prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Median (both)
    df_graphers.loc[j, "title"] = (
        f"Median {survey_type.text[survey]} per day: {ppp_year_old} vs. {ppp_year_current} prices"
    )
    df_graphers.loc[j, "ySlugs"] = f"median_ppp{ppp_year_old} median_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "Median income or consumption"
    df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        "This data is adjusted for inflation and for differences in living costs between countries."
    )
    df_graphers.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = np.nan
    df_graphers.loc[j, "tab"] = np.nan
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P10 ({ppp_year_old})
    df_graphers.loc[j, "title"] = f"P10: The {survey_type.text[survey]} of the poorest tenth ({ppp_year_old} prices)"
    df_graphers.loc[j, "ySlugs"] = f"decile1_thr_ppp{ppp_year_old}"
    df_graphers.loc[j, "Indicator Dropdown"] = "P10 (poorest tenth)"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"P10 is the level of {survey_type.text[survey]} per day below which 10% of the population falls."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_old} prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P10 ({ppp_year_current})
    df_graphers.loc[j, "title"] = (
        f"P10: The {survey_type.text[survey]} of the poorest tenth ({ppp_year_current} prices)"
    )
    df_graphers.loc[j, "ySlugs"] = f"decile1_thr_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "P10 (poorest tenth)"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"P10 is the level of {survey_type.text[survey]} per day below which 10% of the population falls."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_current} prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P10 (both)
    df_graphers.loc[j, "title"] = (
        f"P10: The {survey_type.text[survey]} of the poorest tenth ({ppp_year_old} vs. {ppp_year_current} prices)"
    )
    df_graphers.loc[j, "ySlugs"] = f"decile1_thr_ppp{ppp_year_old} decile1_thr_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "P10 (poorest tenth)"
    df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"P10 is the level of {survey_type.text[survey]} per day below which 10% of the population falls."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is adjusted for inflation and for differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = np.nan
    df_graphers.loc[j, "tab"] = np.nan
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P90 ({ppp_year_old})
    df_graphers.loc[j, "title"] = f"P90: The {survey_type.text[survey]} of the richest tenth ({ppp_year_old} prices)"
    df_graphers.loc[j, "ySlugs"] = f"decile9_thr_ppp{ppp_year_old}"
    df_graphers.loc[j, "Indicator Dropdown"] = "P90 (richest tenth)"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"P90 is the level of {survey_type.text[survey]} per day above which 10% of the population falls."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_old} prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P90 ({ppp_year_current})
    df_graphers.loc[j, "title"] = (
        f"P90: The {survey_type.text[survey]} of the richest tenth ({ppp_year_current} prices)"
    )
    df_graphers.loc[j, "ySlugs"] = f"decile9_thr_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "P90 (richest tenth)"
    df_graphers.loc[j, "International-$ Dropdown"] = f"{ppp_year_current} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"P90 is the level of {survey_type.text[survey]} per day above which 10% of the population falls."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at {ppp_year_current} prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # P90 (both)
    df_graphers.loc[j, "title"] = (
        f"P90: The {survey_type.text[survey]} of the richest tenth ({ppp_year_old} vs. {ppp_year_current} prices)"
    )
    df_graphers.loc[j, "ySlugs"] = f"decile9_thr_ppp{ppp_year_old} decile9_thr_ppp{ppp_year_current}"
    df_graphers.loc[j, "Indicator Dropdown"] = "P90 (richest tenth)"
    df_graphers.loc[j, "International-$ Dropdown"] = f"Compare {ppp_year_current} and {ppp_year_old} prices"
    df_graphers.loc[j, "Poverty line Dropdown"] = np.nan
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"P90 is the level of {survey_type.text[survey]} per day above which 10% of the population falls."
    )
    df_graphers.loc[j, "note"] = (
        f"This data is adjusted for inflation and for differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = np.nan
    df_graphers.loc[j, "tab"] = np.nan
    df_graphers.loc[j, "yScaleToggle"] = "true"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

# Final adjustments to the graphers table:

# Add PPP comparison article as related question link
df_graphers["relatedQuestionText"] = "From $1.90 to $2.15 a day: the updated International Poverty Line"
df_graphers["relatedQuestionUrl"] = (
    "https://ourworldindata.org/from-1-90-to-2-15-a-day-the-updated-international-poverty-line"
)

# Select one default view
df_graphers.loc[
    (df_graphers["ySlugs"] == f"headcount_ratio_190_ppp{ppp_year_old} headcount_ratio_215_ppp{ppp_year_current}")
    & (df_graphers["tableSlug"] == f"income_consumption_{ppp_year_old}_{ppp_year_current}"),
    ["defaultView"],
] = "true"

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

# Reorder dropdown menus
povline_dropdown_list = [
    "$1.90 per day: International Poverty Line",
    "$2.15 per day: International Poverty Line",
    "$3.20 per day: Lower-middle income poverty line",
    "$3.65 per day: Lower-middle income poverty line",
    "$5.50 per day: Upper-middle income poverty line",
    "$6.85 per day: Upper-middle income poverty line",
    "$1 per day",
    "$10 per day",
    "$20 per day",
    "$30 per day",
    "$40 per day",
    "International Poverty Line",
    "Lower-middle income poverty line",
    "Upper-middle income poverty line",
    "Relative poverty: 40% of median",
    "Relative poverty: 50% of median",
    "Relative poverty: 60% of median",
]


df_graphers_mapping = pd.DataFrame(
    {
        "povline_dropdown": povline_dropdown_list,
    }
)
df_graphers_mapping = df_graphers_mapping.reset_index().set_index("povline_dropdown")

df_graphers["povline_dropdown_aux"] = df_graphers["Poverty line Dropdown"].map(df_graphers_mapping["index"])
df_graphers = df_graphers.sort_values("povline_dropdown_aux", ignore_index=True)
df_graphers = df_graphers.drop(columns=["povline_dropdown_aux"])

# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

# Define list of variables to iterate: survey types
survey_list = list(survey_type["table_name"].unique())

# Header is converted into a tab-separated text
header_tsv = df_header.to_csv(sep="\t", header=False)

# Auxiliar variable `survey_type` is dropped and graphers table is converted into a tab-separated text
graphers_tsv = df_graphers.drop(columns=["survey_type"])
graphers_tsv = graphers_tsv.to_csv(sep="\t", index=False)

# This table is indented, to follow explorers' format
graphers_tsv_indented = textwrap.indent(graphers_tsv, "\t")

# The dataframes are combined, including tables which are filtered by survey type and variable
content = header_tsv + "\ngraphers\n" + graphers_tsv_indented

for i in survey_list:
    table_tsv = df_tables[df_tables["survey_type"] == i].copy().reset_index(drop=True)
    table_tsv = table_tsv.drop(columns=["survey_type"])
    table_tsv = table_tsv.to_csv(sep="\t", index=False)
    table_tsv_indented = textwrap.indent(table_tsv, "\t")
    content += (
        "\ntable\t"
        + "https://catalog.ourworldindata.org/explorers/wb/latest/world_bank_pip/"
        + i
        + ".csv\t"
        + i
        + "\ncolumns\t"
        + i
        + "\n"
        + table_tsv_indented
    )

upsert_to_db("poverty-explorer-2011-vs-2017-ppp", content)
