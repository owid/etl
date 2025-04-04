# %% [markdown]
# # Inequality Data Explorer of the World Inequality Database
# This code creates the tsv file for the inequality explorer from the WID data, available [here](https://owid.cloud/admin/explorers/preview/wid-inequality)

import numpy as np

# %%
import pandas as pd

from ..common_parameters import *

# %% [markdown]
# ## Google sheets auxiliar data
# These spreadsheets provide with different details depending on each type of welfare measure or tables considered.

# %%
# Read Google sheets
sheet_id = "18T5IGnpyJwb8KL9USYvME6IaLEcYIo26ioHCpkDnwRQ"

# Welfare type sheet
sheet_name = "welfare"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
welfare = pd.read_csv(url, keep_default_na=False)

# Tables sheet
sheet_name = "tables"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
tables = pd.read_csv(url, keep_default_na=False)

# %% [markdown]
# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# %%
# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Inequality - World Inequality Database",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Explore World Inequality Database data on inequality.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57750",
    "entityType": "country or region",
    "pickerColumnSlugs": "p0p100_gini_pretax p0p100_gini_posttax_nat p90p100_share_pretax p90p100_share_posttax_nat p99p100_share_pretax p99p100_share_posttax_nat palma_ratio_pretax palma_ratio_posttax_nat",
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
# Table generation

sourceName = SOURCE_NAME_WID
dataPublishedBy = DATA_PUBLISHED_BY_WID
sourceLink = SOURCE_LINK_WID
tolerance = TOLERANCE
new_line = NEW_LINE

yAxisMin = Y_AXIS_MIN

additional_description = ADDITIONAL_DESCRIPTION_WID
ppp_description = PPP_DESCRIPTION_WID

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
        # Define additional description depending on the welfare type
        if wel == 0:
            additional_description = ADDITIONAL_DESCRIPTION_WID_POST_TAX
        else:
            additional_description = ADDITIONAL_DESCRIPTION_WID

        # Gini coefficient
        df_tables.loc[j, "name"] = f"Gini coefficient {welfare['title'][wel]}"
        df_tables.loc[j, "slug"] = f"p0p100_gini_{welfare['slug'][wel]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                "The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality.",
                welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables.loc[j, "unit"] = np.nan
        df_tables.loc[j, "shortUnit"] = np.nan
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_gini"][wel]
        df_tables.loc[j, "colorScaleNumericMinValue"] = 1
        df_tables.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables.loc[j, "colorScaleScheme"] = "Oranges"
        j += 1

        # Share of the top 10%
        df_tables.loc[j, "name"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the richest 10% {welfare['title'][wel]}"
        )
        df_tables.loc[j, "slug"] = f"p90p100_share_{welfare['slug'][wel]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The share of {welfare['welfare_type'][wel]} received by the richest 10% of the population.",
                welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_top10"][wel]
        df_tables.loc[j, "colorScaleNumericMinValue"] = 100
        df_tables.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the top 1%
        df_tables.loc[j, "name"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the richest 1% {welfare['title'][wel]}"
        )
        df_tables.loc[j, "slug"] = f"p99p100_share_{welfare['slug'][wel]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The share of {welfare['welfare_type'][wel]} received by the richest 1% of the population.",
                welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_top1"][wel]
        df_tables.loc[j, "colorScaleNumericMinValue"] = 0
        df_tables.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the top 0.1%
        df_tables.loc[j, "name"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the richest 0.1% {welfare['title'][wel]}"
        )
        df_tables.loc[j, "slug"] = f"p99_9p100_share_{welfare['slug'][wel]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The share of {welfare['welfare_type'][wel]} received by the richest 0.1% of the population.",
                welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_top01"][wel]
        df_tables.loc[j, "colorScaleNumericMinValue"] = 0
        df_tables.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables.loc[j, "colorScaleScheme"] = "OrRd"
        j += 1

        # Share of the bottom 50%
        df_tables.loc[j, "name"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the poorest 50% {welfare['title'][wel]}"
        )
        df_tables.loc[j, "slug"] = f"p0p50_share_{welfare['slug'][wel]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                f"The share of {welfare['welfare_type'][wel]} received by the poorest 50% of the population.",
                welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables.loc[j, "unit"] = "%"
        df_tables.loc[j, "shortUnit"] = "%"
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_bottom50"][wel]
        df_tables.loc[j, "colorScaleNumericMinValue"] = 100
        df_tables.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables.loc[j, "colorScaleScheme"] = "Blues"
        j += 1

        # Palma ratio
        df_tables.loc[j, "name"] = f"Palma ratio {welfare['title'][wel]}"
        df_tables.loc[j, "slug"] = f"palma_ratio_{welfare['slug'][wel]}"
        df_tables.loc[j, "description"] = new_line.join(
            [
                "The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality.",
                welfare["description"][wel],
                additional_description,
            ]
        )
        df_tables.loc[j, "unit"] = np.nan
        df_tables.loc[j, "shortUnit"] = np.nan
        df_tables.loc[j, "type"] = "Numeric"
        df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_palma_ratio"][wel]
        df_tables.loc[j, "colorScaleNumericMinValue"] = 0
        df_tables.loc[j, "colorScaleEqualSizeBins"] = "true"
        df_tables.loc[j, "colorScaleScheme"] = "YlOrBr"
        j += 1

    df_tables["tableSlug"] = tables["name"][tab]

df_tables["sourceName"] = sourceName
df_tables["dataPublishedBy"] = dataPublishedBy
df_tables["sourceLink"] = sourceLink
df_tables["tolerance"] = tolerance

# Make tolerance integer (to not break the parameter in the platform)
df_tables["tolerance"] = df_tables["tolerance"].astype("Int64")

# %% [markdown]
# ### Grapher views
# Similar to the tables, this creates the grapher views by grouping by types of variables and then running by welfare type.

# %%
# Grapher table generation

df_graphers = pd.DataFrame()

j = 0

for tab in range(len(tables)):
    for wel in range(len(welfare)):
        # Gini coefficient
        df_graphers.loc[j, "title"] = f"Gini coefficient {welfare['title'][wel]}"
        df_graphers.loc[j, "ySlugs"] = f"p0p100_gini_{welfare['slug'][wel]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Gini coefficient"
        df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality. {welfare['subtitle_ineq'][wel]}"
        )
        df_graphers.loc[j, "note"] = f"{welfare['note'][wel]}"
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        j += 1

        # Share of the top 10%
        df_graphers.loc[j, "title"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the richest 10% {welfare['title'][wel]}"
        )
        df_graphers.loc[j, "ySlugs"] = f"p90p100_share_{welfare['slug'][wel]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 10%"
        df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The share of {welfare['welfare_type'][wel]} received by the richest 10% of the population. {welfare['subtitle'][wel]}"
        )
        df_graphers.loc[j, "note"] = f"{welfare['note'][wel]}"
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        j += 1

        # Share of the top 1%
        df_graphers.loc[j, "title"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the richest 1% {welfare['title'][wel]}"
        )
        df_graphers.loc[j, "ySlugs"] = f"p99p100_share_{welfare['slug'][wel]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 1%"
        df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The share of {welfare['welfare_type'][wel]} received by the richest 1% of the population. {welfare['subtitle'][wel]}"
        )
        df_graphers.loc[j, "note"] = f"{welfare['note'][wel]}"
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        j += 1

        # Share of the top 0.1%
        df_graphers.loc[j, "title"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the richest 0.1% {welfare['title'][wel]}"
        )
        df_graphers.loc[j, "ySlugs"] = f"p99_9p100_share_{welfare['slug'][wel]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 0.1%"
        df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The share of {welfare['welfare_type'][wel]} received by the richest 0.1% of the population. {welfare['subtitle'][wel]}"
        )
        df_graphers.loc[j, "note"] = f"{welfare['note'][wel]}"
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        j += 1

        # Share of the bottom 50%
        df_graphers.loc[j, "title"] = (
            f"{welfare['welfare_type'][wel].capitalize()} share of the poorest 50% {welfare['title'][wel]}"
        )
        df_graphers.loc[j, "ySlugs"] = f"p0p50_share_{welfare['slug'][wel]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Share of the poorest 50%"
        df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The share of {welfare['welfare_type'][wel]} received by the poorest 50% of the population. {welfare['subtitle'][wel]}"
        )
        df_graphers.loc[j, "note"] = f"{welfare['note'][wel]}"
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        j += 1

        # # Palma ratio
        df_graphers.loc[j, "title"] = f"Palma ratio {welfare['title'][wel]}"
        df_graphers.loc[j, "ySlugs"] = f"palma_ratio_{welfare['slug'][wel]}"
        df_graphers.loc[j, "Indicator Dropdown"] = "Palma ratio"
        df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
        df_graphers.loc[j, "subtitle"] = (
            f"The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality. {welfare['subtitle_ineq'][wel]}"
        )
        df_graphers.loc[j, "note"] = f"{welfare['note'][wel]}"
        df_graphers.loc[j, "type"] = np.nan
        df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
        df_graphers.loc[j, "hasMapTab"] = "true"
        df_graphers.loc[j, "tab"] = "map"
        j += 1

    # BEFORE VS. AFTER TAX
    # Gini coefficient
    df_graphers.loc[j, "title"] = f"Gini coefficient (after tax vs. before tax)"
    df_graphers.loc[j, "ySlugs"] = f"p0p100_gini_pretax p0p100_gini_posttax_nat"
    df_graphers.loc[j, "Indicator Dropdown"] = "Gini coefficient"
    df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
    df_graphers.loc[j, "subtitle"] = (
        f"The Gini coefficient measures inequality on a scale from 0 to 1. Higher values indicate higher inequality."
    )
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    j += 1

    # Share of the top 10%
    df_graphers.loc[j, "title"] = f"Income share of the richest 10% (after tax vs. before tax)"
    df_graphers.loc[j, "ySlugs"] = f"p90p100_share_pretax p90p100_share_posttax_nat"
    df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 10%"
    df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
    df_graphers.loc[j, "subtitle"] = f"The share of income received by the richest 10% of the population."
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    j += 1

    # Share of the top 1%
    df_graphers.loc[j, "title"] = f"Income share of the richest 1% (after tax vs. before tax)"
    df_graphers.loc[j, "ySlugs"] = f"p99p100_share_pretax p99p100_share_posttax_nat"
    df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 1%"
    df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
    df_graphers.loc[j, "subtitle"] = f"The share of income received by the richest 1% of the population."
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    j += 1

    # Share of the top 0.1%
    df_graphers.loc[j, "title"] = f"Income share of the richest 0.1% (after tax vs. before tax)"
    df_graphers.loc[j, "ySlugs"] = f"p99_9p100_share_pretax p99_9p100_share_posttax_nat"
    df_graphers.loc[j, "Indicator Dropdown"] = "Share of the richest 0.1%"
    df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
    df_graphers.loc[j, "subtitle"] = f"The share of income received by the richest 0.1% of the population."
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    j += 1

    # Share of the bottom 50%
    df_graphers.loc[j, "title"] = f"Income share of the poorest 50% (after tax vs. before tax)"
    df_graphers.loc[j, "ySlugs"] = f"p0p50_share_pretax p0p50_share_posttax_nat"
    df_graphers.loc[j, "Indicator Dropdown"] = "Share of the poorest 50%"
    df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
    df_graphers.loc[j, "subtitle"] = f"The share of income received by the poorest 50% of the population."
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    j += 1

    # # Palma ratio
    df_graphers.loc[j, "title"] = f"Palma ratio (after tax vs. before tax)"
    df_graphers.loc[j, "ySlugs"] = f"palma_ratio_pretax palma_ratio_posttax_nat"
    df_graphers.loc[j, "Indicator Dropdown"] = "Palma ratio"
    df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
    df_graphers.loc[j, "subtitle"] = (
        f"The Palma ratio is a measure of inequality that divides the share received by the richest 10% by the share of the poorest 40%. Higher values indicate higher inequality."
    )
    df_graphers.loc[j, "note"] = ""
    df_graphers.loc[j, "type"] = np.nan
    df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
    df_graphers.loc[j, "hasMapTab"] = "false"
    df_graphers.loc[j, "tab"] = "chart"
    j += 1

    df_graphers["tableSlug"] = tables["name"][tab]

# %% [markdown]
# Final adjustments to the graphers table: add `relatedQuestion` link and `defaultView`:

# %%
# Add related question link
df_graphers["relatedQuestionText"] = np.nan
df_graphers["relatedQuestionUrl"] = np.nan

# Add yAxisMin
df_graphers["yAxisMin"] = yAxisMin

# Select one default view
df_graphers.loc[
    (df_graphers["Indicator Dropdown"] == "Gini coefficient") & (df_graphers["Income measure Dropdown"] == "After tax"),
    ["defaultView"],
] = "true"


# %% [markdown]
# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

# %%
save("inequality-wid", tables, df_header, df_graphers, df_tables)  # type: ignore
