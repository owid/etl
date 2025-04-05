# %% [markdown]
# # Inequality Data Explorer - Source Comparison
# This code creates the tsv file for the inequality comparison explorer, available [here](https://owid.cloud/admin/explorers/preview/inequality-comparison)

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

# WORLD INEQUALITY DATABASE
# Read Google sheets
sheet_id = "18T5IGnpyJwb8KL9USYvME6IaLEcYIo26ioHCpkDnwRQ"

# Welfare type sheet
sheet_name = "welfare"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
wid_welfare = pd.read_csv(url, keep_default_na=False)

# WORLD BANK POVERTY AND INEQUALITY PLATFORM
# Read Google sheets
sheet_id = "17KJ9YcvfdmO_7-Sv2Ij0vmzAQI6rXSIqHfJtgFHN-a8"

# Survey type sheet
sheet_name = "table"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
pip_tables = pd.read_csv(url)

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
    "explorerTitle": "Inequality - World Bank, WID, and LIS",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Compare World Bank, WID, and LIS data on inequality.",
    "isPublished": "true",
    "googleSheet": "",
    "wpBlockId": "57742",
    "entityType": "country or region",
    "pickerColumnSlugs": "gini decile10_share palma_ratio headcount_ratio_50_median p0p100_gini_posttax_nat p90p100_share_posttax_nat palma_ratio_posttax_nat gini_dhi_pc share_p100_dhi_pc palma_ratio_dhi_pc headcount_ratio_50_median_dhi_pc",
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

processing_description = PROCESSING_DESCRIPTION_PIP_INEQUALITY
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

    # Gini coefficient
    df_tables_pip.loc[j, "name"] = f"Gini coefficient (PIP data)"
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
    df_tables_pip.loc[j, "colorScaleScheme"] = "Oranges"
    j += 1

    # Share of the top 10%
    df_tables_pip.loc[j, "name"] = f"{pip_tables.text[tab].capitalize()} share of the richest 10% (PIP data)"
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
    df_tables_pip.loc[j, "colorScaleScheme"] = "OrRd"
    j += 1

    # Share of the bottom 50%
    df_tables_pip.loc[j, "name"] = f"{pip_tables.text[tab].capitalize()} share of the poorest 50% (PIP data)"
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
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "10;15;20;25;30;35"
    df_tables_pip.loc[j, "colorScaleScheme"] = "Blues"
    j += 1

    # Palma ratio
    df_tables_pip.loc[j, "name"] = f"Palma ratio (PIP data)"
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
    df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
    j += 1

    # Headcount ratio (rel)
    df_tables_pip.loc[j, "name"] = f"Share in relative poverty (PIP data)"
    df_tables_pip.loc[j, "slug"] = f"headcount_ratio_50_median"
    df_tables_pip.loc[j, "description"] = new_line.join(
        [
            "The share of population with after tax income or consumption below 50% of the median.",
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
    df_tables_pip.loc[j, "colorScaleNumericBins"] = "5;10;15;20;25;30"
    df_tables_pip.loc[j, "colorScaleScheme"] = "YlOrBr"
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

        # Gini coefficient
        df_tables_wid.loc[j, "name"] = f"Gini coefficient (WID data)"
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
        df_tables_wid.loc[j, "colorScaleScheme"] = "Oranges"
        j += 1

        # Share of the top 10%
        df_tables_wid.loc[j, "name"] = (
            f"{wid_welfare['welfare_type'][wel].capitalize()} share of the richest 10% (WID data)"
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
        df_tables_wid.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the bottom 50%
        df_tables_wid.loc[j, "name"] = (
            f"{wid_welfare['welfare_type'][wel].capitalize()} share of the poorest 50% (WID data)"
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
        df_tables_wid.loc[j, "colorScaleScheme"] = "Blues"
        j += 1

        # Palma ratio
        df_tables_wid.loc[j, "name"] = f"Palma ratio (WID data)"
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
        df_tables_wid.loc[j, "colorScaleScheme"] = "YlOrBr"
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
relative_poverty_description = RELATIVE_POVERTY_DESCRIPTION_LIS

df_tables_lis = pd.DataFrame()
j = 0

for tab in range(len(merged_tables)):
    for wel in range(len(lis_welfare)):
        for eq in range(len(lis_equivalence_scales)):
            # Gini coefficient
            df_tables_lis.loc[j, "name"] = f"Gini coefficient (LIS data)"
            df_tables_lis.loc[j, "slug"] = f"gini_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
                    lis_welfare["description"][wel],
                    lis_equivalence_scales["description"][eq],
                    notes_title,
                    processing_description,
                    processing_gini_mean_median,
                ]
            )
            df_tables_lis.loc[j, "unit"] = np.nan
            df_tables_lis.loc[j, "shortUnit"] = np.nan
            df_tables_lis.loc[j, "type"] = "Numeric"
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_gini"][wel]
            df_tables_lis.loc[j, "colorScaleScheme"] = "Oranges"
            df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
            j += 1

            # Share of the top 10%
            df_tables_lis.loc[j, "name"] = (
                f"{lis_welfare['welfare_type'][wel].capitalize()} share of the richest 10% (LIS data)"
            )
            df_tables_lis.loc[j, "slug"] = f"share_p100_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    f"The share of {lis_welfare['welfare_type'][wel]} received by the richest 10% of the population.",
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
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_top10"][wel]
            df_tables_lis.loc[j, "colorScaleScheme"] = "OrRd"
            df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
            j += 1

            # Share of the bottom 50%
            df_tables_lis.loc[j, "name"] = (
                f"{lis_welfare['welfare_type'][wel].capitalize()} share of the poorest 50% (LIS data)"
            )
            df_tables_lis.loc[j, "slug"] = (
                f"share_bottom50_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            )
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    f"The share of {lis_welfare['welfare_type'][wel]} received by the poorest 50% of the population.",
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
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_bottom50"][wel]
            df_tables_lis.loc[j, "colorScaleScheme"] = "Blues"
            df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
            j += 1

            # Palma ratio
            df_tables_lis.loc[j, "name"] = f"Palma ratio (LIS data)"
            df_tables_lis.loc[j, "slug"] = (
                f"palma_ratio_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            )
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
                    lis_welfare["description"][wel],
                    lis_equivalence_scales["description"][eq],
                    notes_title,
                    processing_description,
                    processing_distribution,
                ]
            )
            df_tables_lis.loc[j, "unit"] = np.nan
            df_tables_lis.loc[j, "shortUnit"] = np.nan
            df_tables_lis.loc[j, "type"] = "Numeric"
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_palma_ratio"][wel]
            df_tables_lis.loc[j, "colorScaleScheme"] = "YlOrBr"
            df_tables_lis.loc[j, "equivalized"] = lis_equivalence_scales["text"][eq]
            j += 1

            # Headcount ratio (rel)
            df_tables_lis.loc[j, "name"] = f"Share in relative poverty (LIS data)"
            df_tables_lis.loc[j, "slug"] = (
                f"headcount_ratio_50_median_{lis_welfare['slug'][wel]}_{lis_equivalence_scales['slug'][eq]}"
            )
            df_tables_lis.loc[j, "description"] = new_line.join(
                [
                    f"The share of the population with {lis_welfare['welfare_type'][wel]} below 50% of the median.",
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
            df_tables_lis.loc[j, "colorScaleNumericBins"] = lis_welfare["scale_relative_poverty"][wel]
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
df_tables = pd.concat([df_tables_pip, df_tables_wid, df_tables_lis], ignore_index=True)
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

datasets_description_subtitle = "The definition of income varies across the data sources."

df_graphers = pd.DataFrame()

j = 0

for tab in range(len(merged_tables)):
    for view in range(len(source_checkbox)):
        # Gini coefficient
        df_graphers.loc[j, "title"] = f"Gini coefficient ({source_checkbox['type_title'][view]})"
        df_graphers.loc[j, "ySlugs"] = source_checkbox["gini"][view]
        df_graphers.loc[j, "Indicator Dropdown"] = "Gini coefficient"
        df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
        df_graphers.loc[j, "subtitle"] = (
            f"The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality. {datasets_description_subtitle}"
        )
        df_graphers.loc[j, "note"] = source_checkbox["note"][view]
        df_graphers.loc[j, "type"] = np.nan
        j += 1

        # Share of the top 10%
        df_graphers.loc[j, "title"] = f"Income share of the richest 10% ({source_checkbox['type_title'][view]})"
        df_graphers.loc[j, "ySlugs"] = source_checkbox["top10"][view]
        df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 10%"
        df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
        df_graphers.loc[j, "subtitle"] = (
            f"The share of income received by the richest 10% of the population. {datasets_description_subtitle}"
        )
        df_graphers.loc[j, "note"] = source_checkbox["note"][view]
        df_graphers.loc[j, "type"] = np.nan
        j += 1

        # Share of the bottom 50%
        df_graphers.loc[j, "title"] = f"Income share of the poorest 50% ({source_checkbox['type_title'][view]})"
        df_graphers.loc[j, "ySlugs"] = source_checkbox["bottom50"][view]
        df_graphers.loc[j, "Indicator Dropdown"] = "Share of the poorest 50%"
        df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
        df_graphers.loc[j, "subtitle"] = (
            f"The share of income received by the poorest 50% of the population. {datasets_description_subtitle}"
        )
        df_graphers.loc[j, "note"] = source_checkbox["note"][view]
        j += 1

        # Palma ratio
        df_graphers.loc[j, "title"] = f"Palma ratio ({source_checkbox['type_title'][view]})"
        df_graphers.loc[j, "ySlugs"] = source_checkbox["palma"][view]
        df_graphers.loc[j, "Indicator Dropdown"] = "Palma ratio"
        df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
        df_graphers.loc[j, "subtitle"] = (
            f"The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality. {datasets_description_subtitle}"
        )
        df_graphers.loc[j, "note"] = source_checkbox["note"][view]
        df_graphers.loc[j, "type"] = np.nan
        j += 1

        # Headcount ratio (rel)
        df_graphers.loc[j, "title"] = f"Share of people in relative poverty ({source_checkbox['type_title'][view]})"
        df_graphers.loc[j, "ySlugs"] = source_checkbox["relative"][view]
        df_graphers.loc[j, "Indicator Dropdown"] = f"Share in relative poverty"
        df_graphers.loc[j, "Income measure Dropdown"] = source_checkbox["type_title"][view].capitalize()
        df_graphers.loc[j, "subtitle"] = (
            f"The share of population with income below 50% of the median. Relative poverty reflects the extent of inequality within the bottom of the distribution. {datasets_description_subtitle}"
        )
        df_graphers.loc[j, "note"] = source_checkbox["note"][view]
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

# Remove relative poverty view for before tax (no PIP data)
df_graphers = df_graphers[
    ~(
        (df_graphers["Income measure Dropdown"] == "Before tax")
        & (df_graphers["Indicator Dropdown"] == "Share in relative poverty")
    )
].reset_index(drop=True)

# %% [markdown]
# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# %%
# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# Select one default view
df_graphers.loc[
    (df_graphers["Indicator Dropdown"] == "Gini coefficient") & (df_graphers["Income measure Dropdown"] == "After tax"),
    ["defaultView"],
] = "true"


# %% [markdown]
# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

# %%
save("inequality-comparison", merged_tables, df_header, df_graphers, df_tables)  # type: ignore
