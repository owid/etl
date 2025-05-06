# # Incomes across the distribution explorer
# This code creates the tsv file for the incomes across the distribution explorer from the World Bank PIP data, available [here](https://owid.cloud/admin/explorers/preview/incomes-across-distribution-ppp2017)

import textwrap

import numpy as np
import pandas as pd

from ..common_parameters import *

# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each poverty line or survey type.

# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Settings for 10 deciles variables (share, avg) sheet
sheet_name = "deciles10"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
deciles10 = pd.read_csv(url, dtype={"dropdown": "str", "decile": "str"})

# Settings for 9 deciles variables (thr) sheet
sheet_name = "deciles9"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
deciles9 = pd.read_csv(url, dtype={"dropdown": "str", "decile": "str"})

# Income aggregation sheet (day, month, year)
sheet_name = "income_aggregation"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
income_aggregation = pd.read_csv(url, keep_default_na=False, dtype={"multiplier": "str"})

# Survey type sheet
sheet_name = "survey_type"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
survey_type = pd.read_csv(url)

# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Incomes Across the Distribution - World Bank",
    "selection": ["Mozambique", "Nigeria", "Kenya", "Bangladesh", "Bolivia", "World"],
    "explorerSubtitle": "Explore World Bank data on the distribution of incomes.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57756",
    "entityType": "country or region",
    "pickerColumnSlugs": "mean_year median_year",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# ## Tables
# Variables are grouped by type to iterate by different poverty lines and survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.
# ### Tables for variables not showing breaks between surveys
# These variables consider a continous series, without breaks due to changes in surveys' methodology

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

    # Raw variables to not break aggregations
    # mean
    df_tables.loc[j, "name"] = f"Mean {survey_type.text[survey]} per day"
    df_tables.loc[j, "slug"] = f"mean"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The mean level of {survey_type.text[survey]} per person per day.",
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
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100"
    df_tables.loc[j, "colorScaleScheme"] = "BuGn"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # median
    df_tables.loc[j, "name"] = f"Median {survey_type.text[survey]} per day"
    df_tables.loc[j, "slug"] = f"median"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The level of {survey_type.text[survey]} per person per day below which half of the population falls.",
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
    df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100"
    df_tables.loc[j, "colorScaleScheme"] = "Blues"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    for dec9 in range(len(deciles9)):
        # thresholds
        df_tables.loc[j, "name"] = deciles9.ordinal[dec9].capitalize()
        df_tables.loc[j, "slug"] = f"decile{deciles9.decile[dec9]}_thr"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The level of {survey_type.text[survey]} per person per day below which {deciles9.decile[dec9]}0% of the population falls.",
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
        df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100"
        df_tables.loc[j, "colorScaleScheme"] = "Purples"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    for dec10 in range(len(deciles10)):
        # averages
        df_tables.loc[j, "name"] = deciles10.ordinal[dec10].capitalize()
        df_tables.loc[j, "slug"] = f"decile{deciles10.decile[dec10]}_avg"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The mean {survey_type.text[survey]} per person per day within the {deciles10.ordinal[dec10]} (tenth of the population).",
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
        df_tables.loc[j, "colorScaleNumericBins"] = "1;2;5;10;20;50;100"
        df_tables.loc[j, "colorScaleScheme"] = "Greens"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    for dec10 in range(len(deciles10)):
        # shares
        df_tables.loc[j, "name"] = deciles10.ordinal[dec10].capitalize()
        df_tables.loc[j, "slug"] = f"decile{deciles10.decile[dec10]}_share"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The {survey_type.text[survey]} of the {deciles10.ordinal[dec10]} (tenth of the population) as a share of total {survey_type.text[survey]}.",
                survey_type.description[survey],
                additional_description,
                notes_title,
                processing_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = deciles10.scale_share[dec10]
        df_tables.loc[j, "colorScaleScheme"] = "OrRd"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Aggregations
    for agg in range(len(income_aggregation)):
        # mean
        df_tables.loc[j, "name"] = f"Mean {survey_type.text[survey]} per {income_aggregation.aggregation[agg]}"
        df_tables.loc[j, "slug"] = f"mean{income_aggregation.slug_suffix[agg]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The mean level of {survey_type.text[survey]} per person per {income_aggregation.aggregation[agg]}.",
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
        df_tables.loc[j, "colorScaleNumericBins"] = income_aggregation.scale[agg]
        df_tables.loc[j, "colorScaleScheme"] = "BuGn"
        df_tables.loc[j, "transform"] = f"multiplyBy mean {income_aggregation.multiplier[agg]}"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

        # median
        df_tables.loc[j, "name"] = f"Median {survey_type.text[survey]} per {income_aggregation.aggregation[agg]}"
        df_tables.loc[j, "slug"] = f"median{income_aggregation.slug_suffix[agg]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The level of {survey_type.text[survey]} per person per {income_aggregation.aggregation[agg]} below which half of the population falls.",
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
        df_tables.loc[j, "colorScaleNumericBins"] = income_aggregation.scale[agg]
        df_tables.loc[j, "colorScaleScheme"] = "Blues"
        df_tables.loc[j, "transform"] = f"multiplyBy median {income_aggregation.multiplier[agg]}"
        df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

        for dec9 in range(len(deciles9)):
            # thresholds
            df_tables.loc[j, "name"] = deciles9.ordinal[dec9].capitalize()
            df_tables.loc[j, "slug"] = f"decile{deciles9.decile[dec9]}_thr{income_aggregation.slug_suffix[agg]}"
            df_tables.loc[j, "description"] = new_line.join(
                [
                    f"The level of {survey_type.text[survey]} per person per {income_aggregation.aggregation[agg]} below which {deciles9.decile[dec9]}0% of the population falls.",
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
            df_tables.loc[j, "colorScaleNumericBins"] = deciles9[f"scale_thr_{income_aggregation.aggregation[agg]}"][
                dec9
            ]
            df_tables.loc[j, "colorScaleScheme"] = "Purples"
            df_tables.loc[j, "transform"] = (
                f"multiplyBy decile{deciles9.decile[dec9]}_thr {income_aggregation.multiplier[agg]}"
            )
            df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
            j += 1

        for dec10 in range(len(deciles10)):
            # averages
            df_tables.loc[j, "name"] = deciles10.ordinal[dec10].capitalize()
            df_tables.loc[j, "slug"] = f"decile{deciles10.decile[dec10]}_avg{income_aggregation.slug_suffix[agg]}"
            df_tables.loc[j, "description"] = new_line.join(
                [
                    f"The mean {survey_type.text[survey]} per person per {income_aggregation.aggregation[agg]} within the {deciles10.ordinal[dec10]} (tenth of the population).",
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
            df_tables.loc[j, "colorScaleNumericBins"] = deciles10[f"scale_avg_{income_aggregation.aggregation[agg]}"][
                dec10
            ]
            df_tables.loc[j, "colorScaleScheme"] = "Greens"
            df_tables.loc[j, "transform"] = (
                f"multiplyBy decile{deciles10.decile[dec10]}_avg {income_aggregation.multiplier[agg]}"
            )
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

# ### Tables for variables showing breaks between surveys
# These variables consider a breaks in the series due to changes in surveys' methodology. Special modifications have to be included to graph monthly and yearly variables properly.

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

# Delete monthly and yearly variables, because there are not spells files for them
df_spells = df_spells[~df_spells["master_var"].str.contains("_month")].reset_index(drop=True)
df_spells = df_spells[~df_spells["master_var"].str.contains("_year")].reset_index(drop=True)

# Delete rows for country and year
df_spells = df_spells[(df_spells["master_var"] != "country") & (df_spells["master_var"] != "year")].reset_index(
    drop=True
)

# Create new rows for daily, monthly and yearly aggregations
# Drop shares, because they are not aggregated
df_spells_agg = df_spells[~df_spells["master_var"].str.contains("_share")].copy().reset_index(drop=True)

# Remove country and year slugs
df_spells_agg = df_spells_agg[(df_spells_agg["slug"] != "country") & (df_spells_agg["slug"] != "year")].reset_index(
    drop=True
)

# Create columns for each aggregation
df_spells_consolidated = pd.DataFrame()
for agg in range(len(income_aggregation)):
    df_spells_period = df_spells_agg.copy()
    df_spells_period["transform"] = (
        "multiplyBy " + df_spells_period["slug"] + " " + income_aggregation["multiplier"][agg]
    )
    df_spells_period["slug"] = df_spells_period["slug"] + income_aggregation["slug_suffix"][agg]
    df_spells_period["description"] = df_spells_period["description"].str.replace(
        "day", income_aggregation["aggregation"][agg]
    )
    df_spells_consolidated = pd.concat([df_spells_consolidated, df_spells_period], ignore_index=True)

# Concatenate all the spells tables
df_spells = pd.concat([df_spells, df_spells_consolidated], ignore_index=True)

# Make tolerance integer (to not break the parameter in the platform)
df_spells["tolerance"] = df_spells["tolerance"].astype("Int64")

# ## Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by survey type and poverty lines.

# Grapher table generation

df_graphers = pd.DataFrame()

j = 0

for survey in range(len(survey_type)):
    for agg in range(len(income_aggregation)):
        # mean
        df_graphers.loc[j, "title"] = f"Mean {survey_type.text[survey]} per {income_aggregation.aggregation[agg]}"
        df_graphers.loc[j, "ySlugs"] = f"mean{income_aggregation.slug_suffix[agg]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption"
        df_graphers.loc[j, "Decile Dropdown"] = np.nan
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "Period Radio"] = f"{income_aggregation.aggregation[agg].title()}"
        df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            "This data is adjusted for inflation and for differences in living costs between countries."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "yScaleToggle"] = "true"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

        # median
        df_graphers.loc[j, "title"] = f"Median {survey_type.text[survey]} per {income_aggregation.aggregation[agg]}"
        df_graphers.loc[j, "ySlugs"] = f"median{income_aggregation.slug_suffix[agg]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Median income or consumption"
        df_graphers.loc[j, "Decile Dropdown"] = np.nan
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "Period Radio"] = f"{income_aggregation.aggregation[agg].title()}"
        df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            "This data is adjusted for inflation and for differences in living costs between countries."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "yScaleToggle"] = "true"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

        for dec9 in range(len(deciles9)):
            # thresholds
            df_graphers.loc[j, "title"] = (
                f"Threshold {survey_type.text[survey]} per {income_aggregation.aggregation[agg]} marking the {deciles9.ordinal[dec9]}"
            )
            df_graphers.loc[j, "ySlugs"] = f"decile{deciles9.decile[dec9]}_thr{income_aggregation.slug_suffix[agg]}"
            df_graphers.loc[j, "Indicator Dropdown"] = "Decile thresholds"
            df_graphers.loc[j, "Decile Dropdown"] = f"{deciles9.dropdown[dec9]}"
            df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
            df_graphers.loc[j, "Period Radio"] = f"{income_aggregation.aggregation[agg].title()}"
            df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
            df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
            df_graphers.loc[j, "subtitle"] = (
                f"The level of {survey_type.text_ineq[survey]} per person per {income_aggregation.aggregation[agg]} below which {deciles9.decile[dec9]}0% of the population falls."
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "yAxisMin"] = yAxisMin
            df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
            df_graphers.loc[j, "hasMapTab"] = "true"
            df_graphers.loc[j, "tab"] = "map"
            df_graphers.loc[j, "yScaleToggle"] = "true"
            df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
            j += 1

        for dec10 in range(len(deciles10)):
            # averages
            df_graphers.loc[j, "title"] = (
                f"Mean {survey_type.text[survey]} per {income_aggregation.aggregation[agg]} within the {deciles10.ordinal[dec10]}"
            )
            df_graphers.loc[j, "ySlugs"] = f"decile{deciles10.decile[dec10]}_avg{income_aggregation.slug_suffix[agg]}"
            df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption, by decile"
            df_graphers.loc[j, "Decile Dropdown"] = f"{deciles10.dropdown[dec10]}"
            df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
            df_graphers.loc[j, "Period Radio"] = f"{income_aggregation.aggregation[agg].title()}"
            df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
            df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
            df_graphers.loc[j, "subtitle"] = (
                f"The mean {survey_type.text_ineq[survey]} per person per {income_aggregation.aggregation[agg]} within the {deciles10.ordinal[dec10]} (tenth of the population)."
            )
            df_graphers.loc[j, "note"] = (
                f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
            )
            df_graphers.loc[j, "type"] = np.nan
            df_graphers.loc[j, "yAxisMin"] = yAxisMin
            df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
            df_graphers.loc[j, "hasMapTab"] = "true"
            df_graphers.loc[j, "tab"] = "map"
            df_graphers.loc[j, "yScaleToggle"] = "true"
            df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
            j += 1

        # Only add relative toggle to these
        # thresholds - multiple deciles
        df_graphers.loc[j, "title"] = (
            f"Threshold {survey_type.text[survey]} per {income_aggregation.aggregation[agg]} for each decile"
        )
        df_graphers.loc[j, "ySlugs"] = (
            f"decile1_thr{income_aggregation.slug_suffix[agg]} decile2_thr{income_aggregation.slug_suffix[agg]} decile3_thr{income_aggregation.slug_suffix[agg]} decile4_thr{income_aggregation.slug_suffix[agg]} decile5_thr{income_aggregation.slug_suffix[agg]} decile6_thr{income_aggregation.slug_suffix[agg]} decile7_thr{income_aggregation.slug_suffix[agg]} decile8_thr{income_aggregation.slug_suffix[agg]} decile9_thr{income_aggregation.slug_suffix[agg]}"
        )
        df_graphers.loc[j, "Indicator Dropdown"] = "Decile thresholds"
        df_graphers.loc[j, "Decile Dropdown"] = "All deciles"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "Period Radio"] = f"{income_aggregation.aggregation[agg].title()}"
        df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
        df_graphers.loc[j, "hideRelativeToggle"] = "false"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The level of {survey_type.text_ineq[survey]} per person per {income_aggregation.aggregation[agg]} below which 10%, 20%, 30%, etc. of the population falls."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
        df_graphers.loc[j, "hasMapTab"] = np.nan
        df_graphers.loc[j, "tab"] = np.nan
        df_graphers.loc[j, "yScaleToggle"] = "true"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

        # averages - multiple deciles
        df_graphers.loc[j, "title"] = (
            f"Mean {survey_type.text[survey]} per {income_aggregation.aggregation[agg]} within each decile"
        )
        df_graphers.loc[j, "ySlugs"] = (
            f"decile1_avg{income_aggregation.slug_suffix[agg]} decile2_avg{income_aggregation.slug_suffix[agg]} decile3_avg{income_aggregation.slug_suffix[agg]} decile4_avg{income_aggregation.slug_suffix[agg]} decile5_avg{income_aggregation.slug_suffix[agg]} decile6_avg{income_aggregation.slug_suffix[agg]} decile7_avg{income_aggregation.slug_suffix[agg]} decile8_avg{income_aggregation.slug_suffix[agg]} decile9_avg{income_aggregation.slug_suffix[agg]} decile10_avg{income_aggregation.slug_suffix[agg]}"
        )
        df_graphers.loc[j, "Indicator Dropdown"] = "Mean income or consumption, by decile"
        df_graphers.loc[j, "Decile Dropdown"] = "All deciles"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "Period Radio"] = f"{income_aggregation.aggregation[agg].title()}"
        df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
        df_graphers.loc[j, "hideRelativeToggle"] = "false"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The mean {survey_type.text_ineq[survey]} per person per {income_aggregation.aggregation[agg]} within each decile (tenth of the population)."
        )
        df_graphers.loc[j, "note"] = (
            f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. Depending on the country and year, it relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
        df_graphers.loc[j, "hasMapTab"] = np.nan
        df_graphers.loc[j, "tab"] = np.nan
        df_graphers.loc[j, "yScaleToggle"] = "true"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # Shares do not have aggregation nor relative change
    for dec10 in range(len(deciles10)):
        # shares
        df_graphers.loc[j, "title"] = f"{survey_type.text[survey].capitalize()} share of the {deciles10.ordinal[dec10]}"
        df_graphers.loc[j, "ySlugs"] = f"decile{deciles10.decile[dec10]}_share"
        df_graphers.loc[j, "Indicator Dropdown"] = "Decile shares"
        df_graphers.loc[j, "Decile Dropdown"] = f"{deciles10.dropdown[dec10]}"
        df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
        df_graphers.loc[j, "Period Radio"] = np.nan
        df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
        df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The share of {survey_type.text_ineq[survey]} received by the {deciles10.ordinal[dec10]} (tenth of the population)."
        )
        df_graphers.loc[j, "note"] = (
            f"Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
        )
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "yAxisMin"] = yAxisMin
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        df_graphers.loc[j, "yScaleToggle"] = "false"
        df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
        j += 1

    # shares - multiple deciles
    df_graphers.loc[j, "title"] = f"{survey_type.text[survey].capitalize()} share for each decile"
    df_graphers.loc[j, "ySlugs"] = (
        f"decile1_share decile2_share decile3_share decile4_share decile5_share decile6_share decile7_share decile8_share decile9_share decile10_share"
    )
    df_graphers.loc[j, "Indicator Dropdown"] = "Decile shares"
    df_graphers.loc[j, "Decile Dropdown"] = "All deciles"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "Period Radio"] = np.nan
    df_graphers.loc[j, "Show breaks between less comparable surveys Checkbox"] = "false"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"The share of {survey_type.text_ineq[survey]} received by each decile (tenth of the population)."
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

df_graphers["Show breaks between less comparable surveys Checkbox"] = "false"

# ### Grapher views to show breaks in the curves
# Similar to the tables, additional modifications have to be done to process monthly and yearly data properly.

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

for i in range(len(df_graphers)):
    df_graphers_spells.loc[j, "title"] = df_graphers["title"][i]
    df_graphers_spells.loc[j, "ySlugs"] = ySlugs_spells
    df_graphers_spells.loc[j, "Indicator Dropdown"] = df_graphers["Indicator Dropdown"][i]
    df_graphers_spells.loc[j, "Decile Dropdown"] = df_graphers["Decile Dropdown"][i]
    df_graphers_spells.loc[j, "Household survey data type Dropdown"] = df_graphers[
        "Household survey data type Dropdown"
    ][i]
    df_graphers_spells.loc[j, "Period Radio"] = df_graphers["Period Radio"][i]
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

# Delete spells views for multiple deciles
df_graphers_spells = df_graphers_spells[~(df_graphers_spells["Decile Dropdown"] == "All deciles")].reset_index(
    drop=True
)

# Modify views to be able to see spells for aggregated data
for agg in range(len(income_aggregation)):
    df_graphers_spells.loc[
        df_graphers_spells["tableSlug"].str.contains(income_aggregation["slug_suffix"][agg]),
        ["ySlugs"],
    ] = " ".join([x + income_aggregation["slug_suffix"][agg] for x in spell_list])
    # Modify tableSlug to redirect aggregation views to original tables
    df_graphers_spells["tableSlug"] = df_graphers_spells["tableSlug"].str.removesuffix(
        income_aggregation["slug_suffix"][agg]
    )

df_graphers = pd.concat([df_graphers, df_graphers_spells], ignore_index=True)

# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`, and also order decile and metric dropdowns properly

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
    (df_graphers["Decile Dropdown"] == "9 (richest)")
    & (df_graphers["Indicator Dropdown"] == "Decile thresholds")
    & (df_graphers["Period Radio"] == "Day")
    & (df_graphers["Show breaks between less comparable surveys Checkbox"] == "false")
    & (df_graphers["tableSlug"] == "income_consumption_2017"),
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

# Define list of variables to iterate: survey types and the list of variables (the latter for spell tables)
survey_list = list(survey_type["table_name"].unique())
var_list = list(df_spells["master_var"].unique())

# Header is converted into a tab-separated text
header_tsv = df_header.to_csv(sep="\t", header=False)

# Auxiliar variable `survey_type` is dropped and graphers table is converted into a tab-separated text
graphers_tsv = df_graphers.drop(columns=["survey_type"])
graphers_tsv = graphers_tsv.to_csv(sep="\t", index=False)

# This table is indented to follow the explorers' format
graphers_tsv_indented = textwrap.indent(graphers_tsv, "\t")

# Combine all parts into a content string instead of writing to a file
content = header_tsv
content += "\ngraphers\n" + graphers_tsv_indented

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

upsert_to_db("incomes-across-distribution-wb", content)
