# # Source-switching Inequality Data Explorer
# This code creates the tsv file for the main inequality explorer in the inequality topic page, available [here](https://owid.cloud/admin/explorers/preview/inequality)

import numpy as np
import pandas as pd

from ..common_parameters import *

# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each type of welfare measure or tables considered.

# MULTI-SOURCE
# Read Google sheets
sheet_id = "1wcFsNZCEn_6SJ05BFkXKLUyvCrnigfR8eeemGKgAYsI"

# All the tables sheet (this contains PIP, WID and LIS dataset information)
sheet_name = "all_the_tables"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
all_the_tables = pd.read_csv(url, keep_default_na=False)

# NOTE: We decided to drop LIS from the main inequality explorer

# WORLD INEQUALITY DATABASE
# Read Google sheets
sheet_id = "18T5IGnpyJwb8KL9USYvME6IaLEcYIo26ioHCpkDnwRQ"

# Welfare type sheet
sheet_name = "welfare"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_welfare = pd.read_csv(url, keep_default_na=False)

# Tables sheet
sheet_name = "tables"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_tables = pd.read_csv(url, keep_default_na=False)

# WORLD BANK POVERTY AND INEQUALITY PLATFORM
# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Relative poverty sheet
sheet_name = "povlines_rel"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_povlines_rel = pd.read_csv(url)

# Survey type sheet
sheet_name = "table"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_tables = pd.read_csv(url)

# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Economic Inequality",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Explore key inequality indicators from the World Inequality Database and the World Bank.",
    "isPublished": "true",
    "googleSheet": "",
    "wpBlockId": "57760",
    "entityType": "country or region",
    "pickerColumnSlugs": "gini decile10_share palma_ratio headcount_ratio_50_median p0p100_gini_pretax p90p100_share_pretax palma_ratio_pretax",
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
tolerance = TOLERANCE
colorScaleEqualSizeBins = COLOR_SCALE_EQUAL_SIZEBINS
new_line = NEW_LINE

additional_description = ADDITIONAL_DESCRIPTION_PIP_COMPARISON

notes_title = NOTES_TITLE_PIP

processing_description = PROCESSING_DESCRIPTION_PIP_INEQUALITY
ppp_description = PPP_DESCRIPTION_PIP_2017
relative_poverty_description = RELATIVE_POVERTY_DESCRIPTION_PIP

# Table generation
df_tables_pip = pd.DataFrame()
j = 0

for survey in range(len(pip_tables)):
    # Define country as entityName
    df_tables_pip.loc[j, "name"] = "Country"
    df_tables_pip.loc[j, "slug"] = "country"
    df_tables_pip.loc[j, "type"] = "EntityName"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

    # Define year as Year
    df_tables_pip.loc[j, "name"] = "Year"
    df_tables_pip.loc[j, "slug"] = "year"
    df_tables_pip.loc[j, "type"] = "Year"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

    # Gini coefficient
    df_tables_pip.loc[j, "name"] = f"Gini coefficient (World Bank PIP)"
    df_tables_pip.loc[j, "slug"] = f"gini"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = np.nan
    df_tables_pip.loc[j, "shortUnit"] = np.nan
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "0.25;0.3;0.35;0.4;0.45;0.5;0.55;0.6"
    df_tables_pip.loc[j, "colorScaleNumericMinValue"] = 1
    df_tables_pip.loc[j, "colorScaleEqualSizeBins"] = "true"
    df_tables_pip.loc[j, "colorScaleScheme"] = "Oranges"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

    # Share of the top 10%
    df_tables_pip.loc[j, "name"] = f"{pip_tables.text[survey].capitalize()} share of the richest 10% (World Bank PIP)"
    df_tables_pip.loc[j, "slug"] = f"decile10_share"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            "The share of after tax income or consumption received by the richest 10% of the population.",
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = "%"
    df_tables_pip.loc[j, "shortUnit"] = "%"
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "20;25;30;35;40;45;50"
    df_tables_pip.loc[j, "colorScaleNumericMinValue"] = 100
    df_tables_pip.loc[j, "colorScaleEqualSizeBins"] = "true"
    df_tables_pip.loc[j, "colorScaleScheme"] = "OrRd"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

    # Share of the bottom 50%
    df_tables_pip.loc[j, "name"] = f"{pip_tables.text[survey].capitalize()} share of the poorest 50% (World Bank PIP)"
    df_tables_pip.loc[j, "slug"] = f"bottom50_share"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            "The share of after tax income or consumption received by the poorest 50% of the population.",
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = "%"
    df_tables_pip.loc[j, "shortUnit"] = "%"
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "15;20;25;30;35"
    df_tables_pip.loc[j, "colorScaleNumericMinValue"] = 100
    df_tables_pip.loc[j, "colorScaleEqualSizeBins"] = "true"
    df_tables_pip.loc[j, "colorScaleScheme"] = "Blues"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

    # Palma ratio
    df_tables_pip.loc[j, "name"] = f"Palma ratio (World Bank PIP)"
    df_tables_pip.loc[j, "slug"] = f"palma_ratio"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
            additional_description,
            notes_title,
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = np.nan
    df_tables_pip.loc[j, "shortUnit"] = np.nan
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "0.5;1;1.5;2;2.5;3;3.5;4;4.5;5;5.5"
    df_tables_pip.loc[j, "colorScaleNumericMinValue"] = 0
    df_tables_pip.loc[j, "colorScaleEqualSizeBins"] = "true"
    df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

    # Headcount ratio (rel)
    df_tables_pip.loc[j, "name"] = f"Share in relative poverty (World Bank PIP)"
    df_tables_pip.loc[j, "slug"] = f"headcount_ratio_50_median"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            "The share of population with after tax income or consumption below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution.",
            relative_poverty_description,
            additional_description,
            notes_title,
            "Measures of relative poverty are not directly available in the World Bank PIP data. To calculate this metric we take the median income or consumption for the country and year, calculate a relative poverty line – in this case 50% of the median – and then run a specific query on the PIP API to return the share of population below that line.",
            processing_description,
        ]
    )
    df_tables_pip.loc[j, "unit"] = "%"
    df_tables_pip.loc[j, "shortUnit"] = "%"
    df_tables_pip.loc[j, "type"] = "Numeric"
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "3;6;9;12;15;18;21;24;27"
    df_tables_pip.loc[j, "colorScaleNumericMinValue"] = 0
    df_tables_pip.loc[j, "colorScaleEqualSizeBins"] = "true"
    df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
    df_tables_pip.loc[j, "tableSlug"] = pip_tables["table_name"][survey]
    j += 1

df_tables_pip["sourceName"] = sourceName
df_tables_pip["dataPublishedBy"] = dataPublishedBy
df_tables_pip["sourceLink"] = sourceLink
df_tables_pip["tolerance"] = tolerance

###########################################################################################
# WORLD INEQUALITY DATABASE (WID)
###########################################################################################

# Table generation

sourceName = SOURCE_NAME_WID
dataPublishedBy = DATA_PUBLISHED_BY_WID
sourceLink = SOURCE_LINK_WID
tolerance = TOLERANCE
new_line = NEW_LINE

additional_description = ADDITIONAL_DESCRIPTION_WID
ppp_description = PPP_DESCRIPTION_WID

df_tables_wid = pd.DataFrame()
j = 0

for tab in range(len(wid_tables)):
    # Define country as entityName
    df_tables_wid.loc[j, "name"] = "Country"
    df_tables_wid.loc[j, "slug"] = "country"
    df_tables_wid.loc[j, "type"] = "EntityName"
    j += 1

    # Define year as Year
    df_tables_wid.loc[j, "name"] = "Year"
    df_tables_wid.loc[j, "slug"] = "year"
    df_tables_wid.loc[j, "type"] = "Year"
    j += 1

    for wel in range(len(wid_welfare)):
        # Gini coefficient
        df_tables_wid.loc[j, "name"] = f"Gini coefficient {wid_welfare['title'][wel]} (World Inequality Database)"
        df_tables_wid.loc[j, "slug"] = f"p0p100_gini_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
                wid_welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = np.nan
        df_tables_wid.loc[j, "shortUnit"] = np.nan
        df_tables_wid.loc[j, "type"] = "Numeric"
        df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_gini"][wel]
        df_tables_wid.loc[j, "colorScaleNumericMinValue"] = 1
        df_tables_wid.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables_wid.loc[j, "colorScaleScheme"] = "Oranges"
        j += 1

        # Share of the top 10%
        df_tables_wid.loc[j, "name"] = (
            f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 10% {wid_welfare['title'][wel]} (World Inequality Database)"
        )
        df_tables_wid.loc[j, "slug"] = f"p90p100_share_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                f"The share of {wid_welfare['welfare_type'][wel]} received by the richest 10% of the population.",
                wid_welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = "%"
        df_tables_wid.loc[j, "shortUnit"] = "%"
        df_tables_wid.loc[j, "type"] = "Numeric"
        df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_top10"][wel]
        df_tables_wid.loc[j, "colorScaleNumericMinValue"] = 100
        df_tables_wid.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables_wid.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the top 1%
        df_tables_wid.loc[j, "name"] = (
            f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 1% {wid_welfare['title'][wel]} (World Inequality Database)"
        )
        df_tables_wid.loc[j, "slug"] = f"p99p100_share_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                f"The share of {wid_welfare['welfare_type'][wel]} received by the richest 1% of the population.",
                wid_welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = "%"
        df_tables_wid.loc[j, "shortUnit"] = "%"
        df_tables_wid.loc[j, "type"] = "Numeric"
        df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_top1"][wel]
        df_tables_wid.loc[j, "colorScaleNumericMinValue"] = 0
        df_tables_wid.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables_wid.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the top 0.1%
        df_tables_wid.loc[j, "name"] = (
            f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 0.1% {wid_welfare['title'][wel]} (World Inequality Database)"
        )
        df_tables_wid.loc[j, "slug"] = f"p99_9p100_share_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                f"The share of {wid_welfare['welfare_type'][wel]} received by the richest 0.1% of the population.",
                wid_welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = "%"
        df_tables_wid.loc[j, "shortUnit"] = "%"
        df_tables_wid.loc[j, "type"] = "Numeric"
        df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_top01"][wel]
        df_tables_wid.loc[j, "colorScaleNumericMinValue"] = 0
        df_tables_wid.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables_wid.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the bottom 50%
        df_tables_wid.loc[j, "name"] = (
            f"{wid_welfare['welfare_type'][wel].capitalize()} share of the poorest 50% {wid_welfare['title'][wel]} (World Inequality Database)"
        )
        df_tables_wid.loc[j, "slug"] = f"p0p50_share_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                f"The share of {wid_welfare['welfare_type'][wel]} received by the poorest 50% of the population.",
                wid_welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = "%"
        df_tables_wid.loc[j, "shortUnit"] = "%"
        df_tables_wid.loc[j, "type"] = "Numeric"
        df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_bottom50"][wel]
        df_tables_wid.loc[j, "colorScaleNumericMinValue"] = 100
        df_tables_wid.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables_wid.loc[j, "colorScaleScheme"] = "Blues"
        j += 1

        # Palma ratio
        df_tables_wid.loc[j, "name"] = f"Palma ratio {wid_welfare['title'][wel]} (World Inequality Database)"
        df_tables_wid.loc[j, "slug"] = f"palma_ratio_{wid_welfare['slug'][wel]}"
        df_tables_wid.loc[j, "description"] = new_line.join(
            [
                "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
                wid_welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables_wid.loc[j, "unit"] = np.nan
        df_tables_wid.loc[j, "shortUnit"] = np.nan
        df_tables_wid.loc[j, "type"] = "Numeric"
        df_tables_wid.loc[j, "colorScaleNumericBins"] = wid_welfare["scale_palma_ratio"][wel]
        df_tables_wid.loc[j, "colorScaleNumericMinValue"] = 0
        df_tables_wid.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables_wid.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    df_tables_wid["tableSlug"] = wid_tables["name"][tab]

df_tables_wid["sourceName"] = sourceName
df_tables_wid["dataPublishedBy"] = dataPublishedBy
df_tables_wid["sourceLink"] = sourceLink
df_tables_wid["tolerance"] = tolerance

# Keep only pretax national values for WID:
df_tables_wid = df_tables_wid[~(df_tables_wid["slug"].str.contains("posttax_nat"))].reset_index(drop=True)

# Concatenate all the tables into one
df_tables = pd.concat([df_tables_pip, df_tables_wid], ignore_index=True)
# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# ### Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by welfare type.

# Grapher table generation

###########################################################################################
# WORLD INEQUALITY DATABASE (WID)
###########################################################################################

# Grapher table generation

yAxisMin = Y_AXIS_MIN

df_graphers_wid = pd.DataFrame()

j = 0

for tab in range(len(wid_tables)):
    for wel in range(len(wid_welfare)):
        # Gini coefficient
        df_graphers_wid.loc[j, "title"] = f"Gini coefficient"
        df_graphers_wid.loc[j, "ySlugs"] = f"p0p100_gini_{wid_welfare['slug'][wel]}"
        df_graphers_wid.loc[j, "Data Radio"] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        df_graphers_wid.loc[j, "Indicator Dropdown"] = "Gini coefficient"
        df_graphers_wid.loc[j, "subtitle"] = (
            f"The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality. {wid_welfare['subtitle_ineq'][wel]}"
        )
        df_graphers_wid.loc[j, "note"] = wid_welfare["note"][wel]
        df_graphers_wid.loc[j, "type"] = np.nan
        df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers_wid.loc[j, "hasMapTab"] = "true"
        df_graphers_wid.loc[j, "tab"] = "map"
        j += 1

        # Share of the top 10%
        df_graphers_wid.loc[j, "title"] = f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 10%"
        df_graphers_wid.loc[j, "ySlugs"] = f"p90p100_share_{wid_welfare['slug'][wel]}"
        df_graphers_wid.loc[j, "Data Radio"] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        df_graphers_wid.loc[j, "Indicator Dropdown"] = "Share of the richest 10%"
        df_graphers_wid.loc[j, "subtitle"] = (
            f"The share of income received by the richest 10% of the population. {wid_welfare['subtitle'][wel]}"
        )
        df_graphers_wid.loc[j, "note"] = wid_welfare["note"][wel]
        df_graphers_wid.loc[j, "type"] = np.nan
        df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers_wid.loc[j, "hasMapTab"] = "true"
        df_graphers_wid.loc[j, "tab"] = "map"
        j += 1

        # Share of the top 1%
        df_graphers_wid.loc[j, "title"] = f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 1%"
        df_graphers_wid.loc[j, "ySlugs"] = f"p99p100_share_{wid_welfare['slug'][wel]}"
        df_graphers_wid.loc[j, "Data Radio"] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        df_graphers_wid.loc[j, "Indicator Dropdown"] = "Share of the richest 1%"
        df_graphers_wid.loc[j, "subtitle"] = (
            f"The share of income received by the richest 1% of the population. {wid_welfare['subtitle'][wel]}"
        )
        df_graphers_wid.loc[j, "note"] = wid_welfare["note"][wel]
        df_graphers_wid.loc[j, "type"] = np.nan
        df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers_wid.loc[j, "hasMapTab"] = "true"
        df_graphers_wid.loc[j, "tab"] = "map"
        j += 1

        # Share of the top 0.1%
        df_graphers_wid.loc[j, "title"] = f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 0.1%"
        df_graphers_wid.loc[j, "ySlugs"] = f"p99_9p100_share_{wid_welfare['slug'][wel]}"
        df_graphers_wid.loc[j, "Data Radio"] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        df_graphers_wid.loc[j, "Indicator Dropdown"] = "Share of the richest 0.1%"
        df_graphers_wid.loc[j, "subtitle"] = (
            f"The share of income received by the richest 0.1% of the population. {wid_welfare['subtitle'][wel]}"
        )
        df_graphers_wid.loc[j, "note"] = wid_welfare["note"][wel]
        df_graphers_wid.loc[j, "type"] = np.nan
        df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers_wid.loc[j, "hasMapTab"] = "true"
        df_graphers_wid.loc[j, "tab"] = "map"
        j += 1

        # Share of the bottom 50%
        df_graphers_wid.loc[j, "title"] = f"{wid_welfare['welfare_type'][wel].capitalize()} share of the poorest 50%"
        df_graphers_wid.loc[j, "ySlugs"] = f"p0p50_share_{wid_welfare['slug'][wel]}"
        df_graphers_wid.loc[j, "Data Radio"] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        df_graphers_wid.loc[j, "Indicator Dropdown"] = "Share of the poorest 50%"
        df_graphers_wid.loc[j, "subtitle"] = (
            f"The share of income received by the poorest 50% of the population. {wid_welfare['subtitle'][wel]}"
        )
        df_graphers_wid.loc[j, "note"] = wid_welfare["note"][wel]
        df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers_wid.loc[j, "hasMapTab"] = "true"
        df_graphers_wid.loc[j, "tab"] = "map"
        j += 1

        # # P90/P10
        # df_graphers_wid.loc[
        #     j, "title"
        # ] = f"P90/P10 ratio"
        # df_graphers_wid.loc[j, "ySlugs"] = f"p90_p10_ratio_{wid_welfare['slug'][wel]}"
        # df_graphers_wid.loc[
        #     j, "Data Radio"
        # ] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        # df_graphers_wid.loc[j, "Indicator Dropdown"] = "P90/P10"
        # df_graphers_wid.loc[
        #     j, "subtitle"
        # ] = f"P90 and P10 are the levels of {wid_welfare['welfare_type'][wel]} below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population. {wid_welfare['subtitle'][wel]}"
        # df_graphers_wid.loc[j, "note"] = f"wid_welfare['note'][wel]"
        # df_graphers_wid.loc[j, "type"] = np.nan
        # df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        # df_graphers_wid.loc[j, "hasMapTab"] = "true"
        # df_graphers_wid.loc[j, "tab"] = "map"
        # j += 1

        # # P90/P50
        # df_graphers_wid.loc[
        #     j, "title"
        # ] = f"P90/P50 ratio"
        # df_graphers_wid.loc[j, "ySlugs"] = f"p90_p50_ratio_{wid_welfare['slug'][wel]}"
        # df_graphers_wid.loc[
        #     j, "Data Radio"
        # ] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        # df_graphers_wid.loc[j, "Indicator Dropdown"] = "P90/P50"
        # df_graphers_wid.loc[
        #     j, "subtitle"
        # ] = f"The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median {wid_welfare['welfare_type'][wel]}. {wid_welfare['subtitle'][wel]}"
        # df_graphers_wid.loc[j, "note"] = f"{wid_welfare['note'][wel]}"
        # df_graphers_wid.loc[j, "type"] = np.nan
        # df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        # df_graphers_wid.loc[j, "hasMapTab"] = "true"
        # df_graphers_wid.loc[j, "tab"] = "map"
        # j += 1

        # # P50/P10
        # df_graphers_wid.loc[
        #     j, "title"
        # ] = f"P50/P10 ratio"
        # df_graphers_wid.loc[j, "ySlugs"] = f"p50_p10_ratio_{wid_welfare['slug'][wel]}"
        # df_graphers_wid.loc[
        #     j, "Data Radio"
        # ] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        # df_graphers_wid.loc[j, "Indicator Dropdown"] = "P50/P10"
        # df_graphers_wid.loc[
        #     j, "subtitle"
        # ] = f"The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median {wid_welfare['welfare_type'][wel]} is two times higher than that of someone just falling in the poorest tenth of the population. {wid_welfare['subtitle'][wel]}"
        # df_graphers_wid.loc[j, "note"] = f"{wid_welfare['note'][wel]}"
        # df_graphers_wid.loc[j, "type"] = np.nan
        # df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        # df_graphers_wid.loc[j, "hasMapTab"] = "true"
        # df_graphers_wid.loc[j, "tab"] = "map"
        # j += 1

        # # Palma ratio
        df_graphers_wid.loc[j, "title"] = f"Palma ratio"
        df_graphers_wid.loc[j, "ySlugs"] = f"palma_ratio_{wid_welfare['slug'][wel]}"
        df_graphers_wid.loc[j, "Data Radio"] = f"{wid_tables['source_name'][tab]} ({wid_welfare['radio_option'][wel]})"
        df_graphers_wid.loc[j, "Indicator Dropdown"] = "Palma ratio"
        df_graphers_wid.loc[j, "subtitle"] = (
            f"The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality. {wid_welfare['subtitle_ineq'][wel]}"
        )
        df_graphers_wid.loc[j, "note"] = wid_welfare["note"][wel]
        df_graphers_wid.loc[j, "type"] = np.nan
        df_graphers_wid.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers_wid.loc[j, "hasMapTab"] = "true"
        df_graphers_wid.loc[j, "tab"] = "map"
        j += 1

    df_graphers_wid["tableSlug"] = wid_tables["name"][tab]

# Keep only pretax national values for WID:
df_graphers_wid = df_graphers_wid[df_graphers_wid["ySlugs"].str.contains("pretax")].reset_index(drop=True)

# Add yAxisMin
df_graphers_wid["yAxisMin"] = yAxisMin

###########################################################################################
# WORLD BANK POVERTY AND INEQUALITY PLATFORM
###########################################################################################
yAxisMin = Y_AXIS_MIN

df_graphers_pip = pd.DataFrame()

j = 0

for survey in range(len(pip_tables)):
    # Gini coefficient
    df_graphers_pip.loc[j, "title"] = f"Gini coefficient"
    df_graphers_pip.loc[j, "ySlugs"] = f"gini"
    df_graphers_pip.loc[j, "Data Radio"] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    df_graphers_pip.loc[j, "Indicator Dropdown"] = "Gini coefficient"
    df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    df_graphers_pip.loc[j, "subtitle"] = (
        f"The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality. Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita)."
    )
    df_graphers_pip.loc[j, "note"] = ""
    df_graphers_pip.loc[j, "type"] = np.nan
    df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers_pip.loc[j, "hasMapTab"] = "true"
    df_graphers_pip.loc[j, "tab"] = "map"
    j += 1

    # Share of the top 10%
    df_graphers_pip.loc[j, "title"] = f"{pip_tables.text[survey].capitalize()} share of the richest 10%"
    df_graphers_pip.loc[j, "ySlugs"] = f"decile10_share"
    df_graphers_pip.loc[j, "Data Radio"] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    df_graphers_pip.loc[j, "Indicator Dropdown"] = "Share of the richest 10%"
    df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    df_graphers_pip.loc[j, "subtitle"] = (
        f"The share of after tax income or consumption received by the richest 10% of the population."
    )
    df_graphers_pip.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita)."
    )
    df_graphers_pip.loc[j, "type"] = np.nan
    df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers_pip.loc[j, "hasMapTab"] = "true"
    df_graphers_pip.loc[j, "tab"] = "map"
    j += 1

    # Share of the bottom 50%
    df_graphers_pip.loc[j, "title"] = f"{pip_tables.text[survey].capitalize()} share of the poorest 50%"
    df_graphers_pip.loc[j, "ySlugs"] = f"bottom50_share"
    df_graphers_pip.loc[j, "Data Radio"] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    df_graphers_pip.loc[j, "Indicator Dropdown"] = "Share of the poorest 50%"
    df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    df_graphers_pip.loc[j, "subtitle"] = (
        f"The share of after tax income or consumption received by the poorest 50% of the population."
    )
    df_graphers_pip.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita)."
    )
    df_graphers_pip.loc[j, "type"] = np.nan
    df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers_pip.loc[j, "hasMapTab"] = "true"
    df_graphers_pip.loc[j, "tab"] = "map"
    j += 1

    # # P90/P10
    # df_graphers_pip.loc[j, "title"] = f"P90/P10 ratio"
    # df_graphers_pip.loc[j, "ySlugs"] = f"p90_p10_ratio"
    # df_graphers_pip.loc[
    #     j, "Data Radio"
    # ] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    # df_graphers_pip.loc[j, "Indicator Dropdown"] = "P90/P10"
    # df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    # df_graphers_pip.loc[
    #     j, "subtitle"
    # ] = f"P90 and P10 are the levels of {pip_tables.text[survey]} below which 90% and 10% of the population live, respectively. This variable gives the ratio of the two. It is a measure of inequality that indicates the gap between the richest and poorest tenth of the population."
    # df_graphers_pip.loc[
    #     j, "note"
    # ] = f"Depending on the country and year, the data relates to disposable {pip_tables.text[survey]} per capita."
    # df_graphers_pip.loc[j, "type"] = np.nan
    # df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    # df_graphers_pip.loc[j, "hasMapTab"] = "true"
    # df_graphers_pip.loc[j, "tab"] = "map"
    # j += 1

    # # P90/P50
    # df_graphers_pip.loc[j, "title"] = f"P90/P50 ratio"
    # df_graphers_pip.loc[j, "ySlugs"] = f"p90_p50_ratio"
    # df_graphers_pip.loc[
    #     j, "Data Radio"
    # ] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    # df_graphers_pip.loc[j, "Indicator Dropdown"] = "P90/P50"
    # df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    # df_graphers_pip.loc[
    #     j, "subtitle"
    # ] = f"The P90/P50 ratio measures the degree of inequality within the richest half of the population. A ratio of 2 means that someone just falling in the richest tenth of the population has twice the median {pip_tables.text[survey]}."
    # df_graphers_pip.loc[
    #     j, "note"
    # ] = f"Depending on the country and year, the data relates to disposable {pip_tables.text[survey]} per capita."
    # df_graphers_pip.loc[j, "type"] = np.nan
    # df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    # df_graphers_pip.loc[j, "hasMapTab"] = "true"
    # df_graphers_pip.loc[j, "tab"] = "map"
    # j += 1

    # # P50/P10
    # df_graphers_pip.loc[j, "title"] = f"P50/P10 ratio"
    # df_graphers_pip.loc[j, "ySlugs"] = f"p50_p10_ratio"
    # df_graphers_pip.loc[
    #     j, "Data Radio"
    # ] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    # df_graphers_pip.loc[j, "Indicator Dropdown"] = "P50/P10"
    # df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    # df_graphers_pip.loc[
    #     j, "subtitle"
    # ] = f"The P50/P10 ratio measures the degree of inequality within the poorest half of the population. A ratio of 2 means that the median {pip_tables.text[survey]} is two times higher than that of someone just falling in the poorest tenth of the population."
    # df_graphers_pip.loc[
    #     j, "note"
    # ] = f"Depending on the country and year, the data relates to disposable {pip_tables.text[survey]} per capita."
    # df_graphers_pip.loc[j, "type"] = np.nan
    # df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    # df_graphers_pip.loc[j, "hasMapTab"] = "true"
    # df_graphers_pip.loc[j, "tab"] = "map"
    # j += 1

    # Palma ratio
    df_graphers_pip.loc[j, "title"] = f"Palma ratio"
    df_graphers_pip.loc[j, "ySlugs"] = f"palma_ratio"
    df_graphers_pip.loc[j, "Data Radio"] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    df_graphers_pip.loc[j, "Indicator Dropdown"] = "Palma ratio"
    df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    df_graphers_pip.loc[j, "subtitle"] = (
        f"The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality. Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita)."
    )
    df_graphers_pip.loc[j, "note"] = ""
    df_graphers_pip.loc[j, "type"] = np.nan
    df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers_pip.loc[j, "hasMapTab"] = "true"
    df_graphers_pip.loc[j, "tab"] = "map"
    j += 1

    # Headcount ratio (rel)
    df_graphers_pip.loc[j, "title"] = "Share of people in relative poverty"
    df_graphers_pip.loc[j, "ySlugs"] = f"headcount_ratio_50_median"
    df_graphers_pip.loc[j, "Data Radio"] = f"{pip_tables['source_name'][tab]} ({pip_tables['dropdown_option'][survey]})"
    df_graphers_pip.loc[j, "Indicator Dropdown"] = f"Share in relative poverty"
    df_graphers_pip.loc[j, "tableSlug"] = f"{pip_tables.table_name[survey]}"
    df_graphers_pip.loc[j, "subtitle"] = (
        f"The share of population with after tax income or consumption below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution."
    )
    df_graphers_pip.loc[j, "note"] = (
        f"Depending on the country and year, the data relates to income measured after taxes and benefits, or to consumption, [per capita](#dod:per-capita)."
    )
    df_graphers_pip.loc[j, "type"] = np.nan
    df_graphers_pip.loc[j, "selectedFacetStrategy"] = np.nan
    df_graphers_pip.loc[j, "hasMapTab"] = "true"
    df_graphers_pip.loc[j, "tab"] = "map"
    j += 1

# Add yAxisMin
df_graphers_pip["yAxisMin"] = yAxisMin

# Concatenate all the graphers into one
df_graphers = pd.concat([df_graphers_wid, df_graphers_pip], ignore_index=True)

# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# Select one default view
df_graphers.loc[
    (df_graphers["Data Radio"] == "World Inequality Database (Incomes before tax)")
    & (df_graphers["Indicator Dropdown"] == "Gini coefficient"),
    ["defaultView"],
] = "true"


# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

save("inequality", all_the_tables, df_header, df_graphers, df_tables)  # type: ignore
