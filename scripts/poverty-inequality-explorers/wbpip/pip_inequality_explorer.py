# # Inequality Data Explorer of World Bank data
# This code creates the tsv file for the inequality explorer from the World Bank PIP data, available [here](https://owid.cloud/admin/explorers/preview/pip-inequality-explorer)

import textwrap

import numpy as np
import pandas as pd

from ..common_parameters import *

# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each relative poverty line or survey type.

# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Relative poverty sheet
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
    "explorerTitle": "Inequality - World Bank",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Explore World Bank data on inequality.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57756",
    "entityType": "country or region",
    "pickerColumnSlugs": "gini decile10_share palma_ratio headcount_ratio_50_median",
}

# Index-oriented dataframe
df_header = pd.DataFrame.from_dict(header_dict, orient="index", columns=None)
# Assigns a cell for each entity separated by comma (like in `selection`)
df_header = df_header[0].apply(pd.Series)

# ## Tables
# Variables are grouped by type to iterate by different survey types at the same time. The output is the list of all the variables being used in the explorer, with metadata.
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

    # Gini coefficient
    df_tables.loc[j, "name"] = f"Gini coefficient"
    df_tables.loc[j, "slug"] = f"gini"
    df_tables.loc[j, "description"] = new_line.join(
        [
            "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = np.nan
    df_tables.loc[j, "shortUnit"] = np.nan
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericMinValue"] = 1
    df_tables.loc[j, "colorScaleNumericBins"] = "0.25;0.3;0.35;0.4;0.45;0.5;0.55;0.6"
    df_tables.loc[j, "colorScaleScheme"] = "Oranges"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Share of the top 10%
    df_tables.loc[j, "name"] = f"{survey_type.text[survey].capitalize()} share of the richest 10%"
    df_tables.loc[j, "slug"] = f"decile10_share"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The {survey_type.text[survey]} of the richest decile (tenth of the population) as a share of total {survey_type.text[survey]}.",
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = "%"
    df_tables.loc[j, "shortUnit"] = "%"
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericMinValue"] = 100
    df_tables.loc[j, "colorScaleNumericBins"] = "20;25;30;35;40;45;50"
    df_tables.loc[j, "colorScaleScheme"] = "OrRd"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Palma ratio
    df_tables.loc[j, "name"] = f"Palma ratio"
    df_tables.loc[j, "slug"] = f"palma_ratio"
    df_tables.loc[j, "description"] = new_line.join(
        [
            "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = np.nan
    df_tables.loc[j, "shortUnit"] = np.nan
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericMinValue"] = 0
    df_tables.loc[j, "colorScaleNumericBins"] = "0.5;1;1.5;2;2.5;3;3.5;4;4.5;5;5.5"
    df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Headcount ratio (rel)
    df_tables.loc[j, "name"] = f"Share in relative poverty"
    df_tables.loc[j, "slug"] = f"headcount_ratio_50_median"
    df_tables.loc[j, "description"] = new_line.join(
        [
            f"The share of population with {survey_type.text_ineq[survey]} below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution.",
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
    df_tables.loc[j, "colorScaleNumericMinValue"] = 0
    df_tables.loc[j, "colorScaleNumericBins"] = "3;6;9;12;15;18;21;24;27"
    df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # MLD
    df_tables.loc[j, "name"] = f"Mean Log Deviation"
    df_tables.loc[j, "slug"] = f"mld"
    df_tables.loc[j, "description"] = new_line.join(
        [
            "The mean log deviation (MLD) is a measure of inequality. An MLD of zero indicates perfect equality and it takes on larger positive values as incomes become more unequal. The measure is also referred to as 'Theil L' or 'GE(0)', in reference to the wider families of inequality measures to which the MLD belongs.",
            survey_type.description[survey],
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables.loc[j, "unit"] = np.nan
    df_tables.loc[j, "shortUnit"] = np.nan
    df_tables.loc[j, "type"] = "Numeric"
    df_tables.loc[j, "colorScaleNumericMinValue"] = 0
    df_tables.loc[j, "colorScaleNumericBins"] = "0.1;0.2;0.3;0.4;0.5;0.6;0.7;0.8;0.9;1"
    df_tables.loc[j, "colorScaleScheme"] = "RdPu"
    df_tables.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

df_tables["sourceName"] = sourceName
df_tables["dataPublishedBy"] = dataPublishedBy
df_tables["sourceLink"] = sourceLink
df_tables["tolerance"] = tolerance
df_tables["colorScaleEqualSizeBins"] = colorScaleEqualSizeBins

# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# ### Tables for variables showing breaks between surveys
# These variables consider a breaks in the series due to changes in surveys' methodology.

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

# Make tolerance integer (to not break the parameter in the platform)
df_spells["tolerance"] = df_spells["tolerance"].astype("Int64")

# ### Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by survey type.

# Grapher table generation

df_graphers = pd.DataFrame()

j = 0

for survey in range(len(survey_type)):
    # Gini coefficient
    df_graphers.loc[j, "title"] = f"Gini coefficient"
    df_graphers.loc[j, "ySlugs"] = f"gini"
    df_graphers.loc[j, "Indicator Dropdown"] = "Gini coefficient"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality. Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Share of the top 10%
    df_graphers.loc[j, "title"] = f"{survey_type.text[survey].capitalize()} share of the richest 10%"
    df_graphers.loc[j, "ySlugs"] = f"decile10_share"
    df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 10%"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"The share of {survey_type.text_ineq[survey]} received by the richest 10% of the population."
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

    # Palma ratio
    df_graphers.loc[j, "title"] = f"Palma ratio"
    df_graphers.loc[j, "ySlugs"] = f"palma_ratio"
    df_graphers.loc[j, "Indicator Dropdown"] = "Palma ratio"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality. Depending on the country and year, the data relates to {survey_type.detailed_text[survey]} [per capita](#dod:per-capita)."
    )
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "yAxisMin"] = yAxisMin
    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers.loc[j, "hasMapTab"] = "true"
    df_graphers.loc[j, "tab"] = "map"
    df_graphers.loc[j, "survey_type"] = survey_type["table_name"][survey]
    j += 1

    # Headcount ratio (rel)
    df_graphers.loc[j, "title"] = f"Share of people in relative poverty"
    df_graphers.loc[j, "ySlugs"] = f"headcount_ratio_50_median"
    df_graphers.loc[j, "Indicator Dropdown"] = f"Share in relative poverty"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"The share of population with {survey_type.text_ineq[survey]} below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution."
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

    # MLD
    df_graphers.loc[j, "title"] = f"Mean log deviation"
    df_graphers.loc[j, "ySlugs"] = f"mld"
    df_graphers.loc[j, "Indicator Dropdown"] = "Mean log deviation"
    df_graphers.loc[j, "Household survey data type Dropdown"] = f"{survey_type.dropdown_option[survey]}"
    df_graphers.loc[j, "tableSlug"] = f"{survey_type.table_name[survey]}"
    df_graphers.loc[j, "subtitle"] = (
        f"The mean log deviation (MLD) is a measure of inequality. An MLD of zero indicates perfect equality and it takes on larger positive values as incomes become more unequal."
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

df_graphers["Show breaks between less comparable surveys Checkbox"] = "false"
# ### Grapher views to show breaks in the curves

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

df_graphers = pd.concat([df_graphers, df_graphers_spells], ignore_index=True)

# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

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

# For Gini/Palma subtitle:
df_graphers["subtitle"] = df_graphers["subtitle"].str.replace(
    "Depending on the country and year, the data relates to income measured after taxes and benefits [per capita](#dod:per-capita).",
    "The data relates to income measured after taxes and benefits [per capita](#dod:per-capita).",
    regex=False,
)
df_graphers["subtitle"] = df_graphers["subtitle"].str.replace(
    "Depending on the country and year, the data relates to consumption [per capita](#dod:per-capita).",
    "The data relates to consumption [per capita](#dod:per-capita).",
    regex=False,
)

# Select one default view
df_graphers.loc[
    (df_graphers["ySlugs"] == "gini")
    & (df_graphers["Show breaks between less comparable surveys Checkbox"] == "false")
    & (df_graphers["tableSlug"] == "income_consumption_2017"),
    ["defaultView"],
] = "true"


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

# This table is indented, to follow explorers' format
graphers_tsv_indented = textwrap.indent(graphers_tsv, "\t")

# Build the content string instead of writing to a file
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

upsert_to_db("inequality-wb", content)
