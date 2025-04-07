# # Incomes Across the Distribution Explorer of the Luxembourg Income Study
# This code creates the tsv file for the incomes across the distribution explorer from the LIS data, available [here](https://owid.cloud/admin/explorers/preview/lis-incomes-across-distribution)

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

# Tables sheet
sheet_name = "tables"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
tables = pd.read_csv(url, keep_default_na=False)

# Deciles9 sheet (needed to handle thresholds data)
sheet_name = "deciles9"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
deciles9 = pd.read_csv(url, keep_default_na=False)

# Deciles10 sheet (needed to handle average and share data)
sheet_name = "deciles10"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
deciles10 = pd.read_csv(url, keep_default_na=False)

# Top sheet (needed to handle data at the top of the distribution)
sheet_name = "top_pct"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
top_pct = pd.read_csv(url, keep_default_na=False)

# Income aggregation sheet (day, month, year)
sheet_name = "income_aggregation"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
income_aggregation = pd.read_csv(url, keep_default_na=False, dtype={"multiplier": "str"})

# ## Header
# General settings of the explorer are defined here, like the title, subtitle, default country selection, publishing status and others.

# The header is defined as a dictionary first and then it is converted into a index-oriented dataframe
header_dict = {
    "explorerTitle": "Incomes Across the Distribution - Luxembourg Income Study",
    "selection": [
        "Chile",
        "Brazil",
        "South Africa",
        "United States",
        "France",
        "China",
    ],
    "explorerSubtitle": "Explore Luxembourg Income Study data on the distribution of incomes.",
    "isPublished": "true",
    "googleSheet": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
    "wpBlockId": "57755",
    "entityType": "country or region",
    "pickerColumnSlugs": "mean_mi_eq_year median_mi_eq_year mean_dhi_eq_year median_dhi_eq_year",
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
            # I need the original variables to not break the aggregations
            # Mean
            df_tables.loc[j, "name"] = f"Mean {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
            df_tables.loc[j, "slug"] = f"mean_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
            df_tables.loc[j, "description"] = new_line.join(
                [
                    f"Mean {welfare['welfare_type'][wel]}.",
                    welfare["description"][wel],
                    equivalence_scales["description"][eq],
                    ppp_description,
                    notes_title,
                    processing_description,
                    processing_gini_mean_median,
                ]
            )
            df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
            df_tables.loc[j, "shortUnit"] = "$"
            df_tables.loc[j, "type"] = "Numeric"
            df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_mean"][wel]
            df_tables.loc[j, "colorScaleScheme"] = "BuGn"
            j += 1

            # Median
            df_tables.loc[j, "name"] = f"Median {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
            df_tables.loc[j, "slug"] = f"median_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
            df_tables.loc[j, "description"] = new_line.join(
                [
                    f"The level of {welfare['welfare_type'][wel]} below which half of the population falls.",
                    welfare["description"][wel],
                    equivalence_scales["description"][eq],
                    ppp_description,
                    notes_title,
                    processing_description,
                    processing_gini_mean_median,
                ]
            )
            df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
            df_tables.loc[j, "shortUnit"] = "$"
            df_tables.loc[j, "type"] = "Numeric"
            df_tables.loc[j, "colorScaleNumericBins"] = welfare["scale_median"][wel]
            df_tables.loc[j, "colorScaleScheme"] = "Blues"
            j += 1

            # Thresholds - Deciles
            for dec9 in range(len(deciles9)):
                df_tables.loc[j, "name"] = f"{deciles9['ordinal'][dec9].capitalize()} ({welfare['title'][wel]})"
                df_tables.loc[j, "slug"] = (
                    f"thr_{deciles9['lis_notation'][dec9]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The level of {welfare['welfare_type'][wel]} below which {deciles9['decile'][dec9]}0% of the population falls.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_distribution,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                # df_tables.loc[j, "colorScaleNumericBins"] = deciles9["scale_thr"][dec9]
                df_tables.loc[j, "colorScaleScheme"] = "Purples"
                j += 1

            # Averages - Deciles
            for dec10 in range(len(deciles10)):
                df_tables.loc[j, "name"] = f"{deciles10['ordinal'][dec10].capitalize()} ({welfare['title'][wel]})"
                df_tables.loc[j, "slug"] = (
                    f"avg_{deciles10['lis_notation'][dec10]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The mean {welfare['welfare_type'][wel]} within the {deciles10['ordinal'][dec10]} (tenth of the population).",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_distribution,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                # df_tables.loc[j, "colorScaleNumericBins"] = deciles10["scale_avg"][
                #     dec10
                # ]
                df_tables.loc[j, "colorScaleScheme"] = "Greens"
                j += 1

            # Shares - Deciles
            for dec10 in range(len(deciles10)):
                df_tables.loc[j, "name"] = f"{deciles10['ordinal'][dec10].capitalize()} ({welfare['title'][wel]})"
                df_tables.loc[j, "slug"] = (
                    f"share_{deciles10['lis_notation'][dec10]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The share of {welfare['welfare_type'][wel]} received by the {deciles10['ordinal'][dec10]} (tenth of the population).",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        notes_title,
                        processing_description,
                        processing_distribution,
                    ]
                )
                df_tables.loc[j, "unit"] = "%"
                df_tables.loc[j, "shortUnit"] = "%"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = deciles10[f"scale_share_{welfare['slug'][wel]}"][dec10]
                df_tables.loc[j, "colorScaleScheme"] = "OrRd"
                j += 1

            # Income aggregations
            for agg in range(len(income_aggregation)):
                # Mean
                df_tables.loc[j, "name"] = f"Mean {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
                df_tables.loc[j, "slug"] = (
                    f"mean_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"Mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]}.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_gini_mean_median,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = income_aggregation["scale"][agg]
                df_tables.loc[j, "colorScaleScheme"] = "BuGn"
                df_tables.loc[j, "transform"] = (
                    f"multiplyBy mean_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} {income_aggregation['multiplier'][agg]}"
                )
                j += 1

                # Median
                df_tables.loc[j, "name"] = f"Median {welfare['welfare_type'][wel]} ({welfare['title'][wel]})"
                df_tables.loc[j, "slug"] = (
                    f"median_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_tables.loc[j, "description"] = new_line.join(
                    [
                        f"The level of {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} below which half of the population falls.",
                        welfare["description"][wel],
                        equivalence_scales["description"][eq],
                        ppp_description,
                        notes_title,
                        processing_description,
                        processing_gini_mean_median,
                    ]
                )
                df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                df_tables.loc[j, "shortUnit"] = "$"
                df_tables.loc[j, "type"] = "Numeric"
                df_tables.loc[j, "colorScaleNumericBins"] = income_aggregation["scale"][agg]
                df_tables.loc[j, "colorScaleScheme"] = "Blues"
                df_tables.loc[j, "transform"] = (
                    f"multiplyBy median_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} {income_aggregation['multiplier'][agg]}"
                )
                j += 1

                # Thresholds - Deciles
                for dec9 in range(len(deciles9)):
                    df_tables.loc[j, "name"] = f"{deciles9['ordinal'][dec9].capitalize()} ({welfare['title'][wel]})"
                    df_tables.loc[j, "slug"] = (
                        f"thr_{deciles9['lis_notation'][dec9]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                    )
                    df_tables.loc[j, "description"] = new_line.join(
                        [
                            f"The level of {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} below which {deciles9['decile'][dec9]}0% of the population falls.",
                            welfare["description"][wel],
                            equivalence_scales["description"][eq],
                            ppp_description,
                            notes_title,
                            processing_description,
                            processing_distribution,
                        ]
                    )
                    df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                    df_tables.loc[j, "shortUnit"] = "$"
                    df_tables.loc[j, "type"] = "Numeric"
                    df_tables.loc[j, "colorScaleNumericBins"] = deciles9[
                        f"scale_thr_{welfare['slug'][wel]}_{income_aggregation['aggregation'][agg]}"
                    ][dec9]
                    df_tables.loc[j, "colorScaleScheme"] = "Purples"
                    df_tables.loc[j, "transform"] = (
                        f"multiplyBy thr_{deciles9['lis_notation'][dec9]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} {income_aggregation['multiplier'][agg]}"
                    )
                    j += 1

                # Averages - Deciles
                for dec10 in range(len(deciles10)):
                    df_tables.loc[j, "name"] = f"{deciles10['ordinal'][dec10].capitalize()} ({welfare['title'][wel]})"
                    df_tables.loc[j, "slug"] = (
                        f"avg_{deciles10['lis_notation'][dec10]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                    )
                    df_tables.loc[j, "description"] = new_line.join(
                        [
                            f"The mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} within the {deciles10['ordinal'][dec10]} (tenth of the population).",
                            welfare["description"][wel],
                            equivalence_scales["description"][eq],
                            ppp_description,
                            notes_title,
                            processing_description,
                            processing_distribution,
                        ]
                    )
                    df_tables.loc[j, "unit"] = "international-$ in 2017 prices"
                    df_tables.loc[j, "shortUnit"] = "$"
                    df_tables.loc[j, "type"] = "Numeric"
                    df_tables.loc[j, "colorScaleNumericBins"] = deciles10[
                        f"scale_avg_{welfare['slug'][wel]}_{income_aggregation['aggregation'][agg]}"
                    ][dec10]
                    df_tables.loc[j, "colorScaleScheme"] = "Greens"
                    df_tables.loc[j, "transform"] = (
                        f"multiplyBy avg_{deciles10['lis_notation'][dec10]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} {income_aggregation['multiplier'][agg]}"
                    )
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
    for agg in range(len(income_aggregation)):
        for eq in range(len(equivalence_scales)):
            for wel in range(len(welfare)):
                # Mean
                df_graphers.loc[j, "title"] = (
                    f"Mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"mean_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Mean income"
                df_graphers.loc[j, "Decile Dropdown"] = np.nan
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"This data is adjusted for inflation and for differences in living costs between countries. {welfare['subtitle'][wel]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. {equivalence_scales['note'][eq]}"
                )
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

                # Median
                df_graphers.loc[j, "title"] = (
                    f"Median {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"median_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Median income"
                df_graphers.loc[j, "Decile Dropdown"] = np.nan
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"This data is adjusted for inflation and for differences in living costs between countries. {welfare['subtitle'][wel]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. {equivalence_scales['note'][eq]}"
                )
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

                # Thresholds - Deciles
                for dec9 in range(len(deciles9)):
                    df_graphers.loc[j, "title"] = (
                        f"Threshold {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} marking the {deciles9['ordinal'][dec9]} ({welfare['title'][wel]})"
                    )
                    df_graphers.loc[j, "ySlugs"] = (
                        f"thr_{deciles9['lis_notation'][dec9]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                    )
                    df_graphers.loc[j, "Indicator Dropdown"] = "Decile thresholds"
                    df_graphers.loc[j, "Decile Dropdown"] = deciles9["dropdown"][dec9]
                    df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                    df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                    df_graphers.loc[
                        j,
                        "Adjust for cost sharing within households (equivalized income) Checkbox",
                    ] = equivalence_scales["checkbox"][eq]
                    df_graphers.loc[j, "subtitle"] = (
                        f"The level of {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} below which {deciles9['decile'][dec9]}0% of the population falls. {welfare['subtitle'][wel]}"
                    )
                    df_graphers.loc[j, "note"] = (
                        f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. {equivalence_scales['note'][eq]}"
                    )
                    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                    df_graphers.loc[j, "hasMapTab"] = "true"
                    df_graphers.loc[j, "tab"] = "map"
                    df_graphers.loc[j, "yScaleToggle"] = "true"
                    j += 1

                # Averages - Deciles
                for dec10 in range(len(deciles10)):
                    df_graphers.loc[j, "title"] = (
                        f"Mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} within the {deciles10['ordinal'][dec10]} ({welfare['title'][wel]})"
                    )
                    df_graphers.loc[j, "ySlugs"] = (
                        f"avg_{deciles10['lis_notation'][dec10]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                    )
                    df_graphers.loc[j, "Indicator Dropdown"] = "Mean income, by decile"
                    df_graphers.loc[j, "Decile Dropdown"] = deciles10["dropdown"][dec10]
                    df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                    df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                    df_graphers.loc[
                        j,
                        "Adjust for cost sharing within households (equivalized income) Checkbox",
                    ] = equivalence_scales["checkbox"][eq]
                    df_graphers.loc[j, "subtitle"] = (
                        f"The mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} within the {deciles10['ordinal'][dec10]} (tenth of the population). {welfare['subtitle'][wel]}"
                    )
                    df_graphers.loc[j, "note"] = (
                        f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. {equivalence_scales['note'][eq]}"
                    )
                    df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                    df_graphers.loc[j, "hasMapTab"] = "true"
                    df_graphers.loc[j, "tab"] = "map"
                    df_graphers.loc[j, "yScaleToggle"] = "true"
                    j += 1

                # Thresholds - Multiple deciles
                df_graphers.loc[j, "title"] = (
                    f"Threshold {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} for each decile ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"thr_p10_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p20_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p30_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p40_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p50_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p60_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p70_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p80_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_p90_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Decile thresholds"
                df_graphers.loc[j, "Decile Dropdown"] = "All deciles"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "hideRelativeToggle"] = "false"
                df_graphers.loc[j, "subtitle"] = (
                    f"The level of income per {income_aggregation['aggregation'][agg]} below which 10%, 20%, 30%, etc. of the population falls. {welfare['subtitle'][wel]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. {equivalence_scales['note'][eq]}"
                )
                df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
                df_graphers.loc[j, "hasMapTab"] = "false"
                df_graphers.loc[j, "tab"] = "chart"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

                # Averages - Multiple deciles
                df_graphers.loc[j, "title"] = (
                    f"Mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} within each decile ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"avg_p10_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p20_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p30_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p40_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p50_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p60_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p70_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p80_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p90_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_p100_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Mean income, by decile"
                df_graphers.loc[j, "Decile Dropdown"] = "All deciles"
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "hideRelativeToggle"] = "false"
                df_graphers.loc[j, "subtitle"] = (
                    f"The mean {welfare['welfare_type'][wel]} per {income_aggregation['aggregation'][agg]} within each decile (tenth of the population). {welfare['subtitle'][wel]}"
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. {equivalence_scales['note'][eq]}"
                )
                df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
                df_graphers.loc[j, "hasMapTab"] = "false"
                df_graphers.loc[j, "tab"] = "chart"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

            # BEFORE VS. AFTER TAX
            # Mean
            df_graphers.loc[j, "title"] = (
                f"Mean income per {income_aggregation['aggregation'][agg]} (after tax vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"mean_mi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} mean_dhi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Mean income"
            df_graphers.loc[j, "Decile Dropdown"] = np.nan
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This data is adjusted for inflation and for differences in living costs between countries."
            )
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. {equivalence_scales['note'][eq]}"
            )
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            df_graphers.loc[j, "yScaleToggle"] = "true"
            j += 1

            # Median
            df_graphers.loc[j, "title"] = (
                f"Median income per {income_aggregation['aggregation'][agg]} (after tax vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"median_mi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} median_dhi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Median income"
            df_graphers.loc[j, "Decile Dropdown"] = np.nan
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"This data is adjusted for inflation and for differences in living costs between countries."
            )
            df_graphers.loc[j, "note"] = (
                f"This data is expressed in [international-$](#dod:int_dollar_abbreviation) at 2017 prices. {equivalence_scales['note'][eq]}"
            )
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            df_graphers.loc[j, "yScaleToggle"] = "true"
            j += 1

            # Thresholds - Deciles
            for dec9 in range(len(deciles9)):
                df_graphers.loc[j, "title"] = (
                    f"Threshold income per {income_aggregation['aggregation'][agg]} marking the {deciles9['ordinal'][dec9]} (after tax vs. before tax)"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"thr_{deciles9['lis_notation'][dec9]}_mi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} thr_{deciles9['lis_notation'][dec9]}_dhi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Decile thresholds"
                df_graphers.loc[j, "Decile Dropdown"] = deciles9["dropdown"][dec9]
                df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
                df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"The level of income per {income_aggregation['aggregation'][agg]} below which {deciles9['decile'][dec9]}0% of the population falls."
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. {equivalence_scales['note'][eq]}"
                )
                df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
                df_graphers.loc[j, "hasMapTab"] = "false"
                df_graphers.loc[j, "tab"] = "chart"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

            # Averages - Deciles
            for dec10 in range(len(deciles10)):
                df_graphers.loc[j, "title"] = (
                    f"Mean income per {income_aggregation['aggregation'][agg]} within the {deciles10['ordinal'][dec10]} (after tax vs. before tax)"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"avg_{deciles10['lis_notation'][dec10]}_mi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]} avg_{deciles10['lis_notation'][dec10]}_dhi_{equivalence_scales['slug'][eq]}{income_aggregation['slug_suffix'][agg]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Mean income, by decile"
                df_graphers.loc[j, "Decile Dropdown"] = deciles10["dropdown"][dec10]
                df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
                df_graphers.loc[j, "Period Radio"] = f"{income_aggregation['aggregation'][agg].capitalize()}"
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"The mean income per {income_aggregation['aggregation'][agg]} within the {deciles10['ordinal'][dec10]} (tenth of the population)."
                )
                df_graphers.loc[j, "note"] = (
                    f"This data is measured in [international-$](#dod:int_dollar_abbreviation) at 2017 prices to account for inflation and differences in living costs between countries. {equivalence_scales['note'][eq]}"
                )
                df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
                df_graphers.loc[j, "hasMapTab"] = "false"
                df_graphers.loc[j, "tab"] = "chart"
                df_graphers.loc[j, "yScaleToggle"] = "true"
                j += 1

    # This is an adaptation of the code to make both before and after tax graphs and share graphs
    for eq in range(len(equivalence_scales)):
        for wel in range(len(welfare)):
            # Shares - Deciles
            for dec10 in range(len(deciles10)):
                df_graphers.loc[j, "title"] = (
                    f"{welfare['welfare_type'][wel].capitalize()} share of the {deciles10['ordinal'][dec10]} ({welfare['title'][wel]})"
                )
                df_graphers.loc[j, "ySlugs"] = (
                    f"share_{deciles10['lis_notation'][dec10]}_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
                )
                df_graphers.loc[j, "Indicator Dropdown"] = "Decile shares"
                df_graphers.loc[j, "Decile Dropdown"] = deciles10["dropdown"][dec10]
                df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
                df_graphers.loc[j, "Period Radio"] = np.nan
                df_graphers.loc[
                    j,
                    "Adjust for cost sharing within households (equivalized income) Checkbox",
                ] = equivalence_scales["checkbox"][eq]
                df_graphers.loc[j, "subtitle"] = (
                    f"The share of {welfare['welfare_type'][wel]} received by the {deciles10['ordinal'][dec10]} (tenth of the population). {welfare['subtitle'][wel]}"
                )
                df_graphers.loc[j, "note"] = equivalence_scales["note"][eq]
                df_graphers.loc[j, "selectedFacetStrategy"] = np.nan
                df_graphers.loc[j, "hasMapTab"] = "true"
                df_graphers.loc[j, "tab"] = "map"
                j += 1

            # Shares - Multiple deciles
            df_graphers.loc[j, "title"] = (
                f"{welfare['welfare_type'][wel].capitalize()} share for each decile ({welfare['title'][wel]})"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"share_p10_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p20_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p30_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p40_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p50_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p60_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p70_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p80_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p90_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]} share_p100_{welfare['slug'][wel]}_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Decile shares"
            df_graphers.loc[j, "Decile Dropdown"] = "All deciles"
            df_graphers.loc[j, "Income measure Dropdown"] = f"{welfare['dropdown_option'][wel]}"
            df_graphers.loc[j, "Period Radio"] = np.nan
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"The share of {welfare['welfare_type'][wel]} received by each decile (tenth of the population). {welfare['subtitle'][wel]}"
            )
            df_graphers.loc[j, "note"] = equivalence_scales["note"][eq]
            df_graphers.loc[j, "selectedFacetStrategy"] = "entity"
            df_graphers.loc[j, "hasMapTab"] = "false"
            df_graphers.loc[j, "tab"] = "chart"
            j += 1

        # BEFORE VS. AFTER TAX
        # Shares - Deciles
        for dec10 in range(len(deciles10)):
            df_graphers.loc[j, "title"] = (
                f"Income share of the {deciles10['ordinal'][dec10]} (after tax vs. before tax)"
            )
            df_graphers.loc[j, "ySlugs"] = (
                f"share_{deciles10['lis_notation'][dec10]}_mi_{equivalence_scales['slug'][eq]} share_{deciles10['lis_notation'][dec10]}_dhi_{equivalence_scales['slug'][eq]}"
            )
            df_graphers.loc[j, "Indicator Dropdown"] = "Decile shares"
            df_graphers.loc[j, "Decile Dropdown"] = deciles10["dropdown"][dec10]
            df_graphers.loc[j, "Income measure Dropdown"] = "After tax vs. before tax"
            df_graphers.loc[j, "Period Radio"] = np.nan
            df_graphers.loc[
                j,
                "Adjust for cost sharing within households (equivalized income) Checkbox",
            ] = equivalence_scales["checkbox"][eq]
            df_graphers.loc[j, "subtitle"] = (
                f"The share of income received by the {deciles10['ordinal'][dec10]} (tenth of the population)."
            )
            df_graphers.loc[j, "note"] = equivalence_scales["note"][eq]
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
    (df_graphers["Indicator Dropdown"] == "Decile thresholds")
    & (df_graphers["Decile Dropdown"] == "9 (richest)")
    & (df_graphers["Income measure Dropdown"] == "After tax")
    & (df_graphers["Period Radio"] == "Year")
    & (df_graphers["Adjust for cost sharing within households (equivalized income) Checkbox"] == "false"),
    ["defaultView"],
] = "true"

# Reorder dropdown menus
# Decile/quantile Dropdown
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
    "Mean income",
    "Mean income, by decile",
    "Median income",
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

# # Equivalence scales dropdown
# eq_dropdown_list = ["Equivalized", "Per capita", "Equivalized vs. per capita"]

# df_graphers_mapping = pd.DataFrame(
#     {
#         "eq_dropdown": eq_dropdown_list,
#     }
# )
# df_graphers_mapping = df_graphers_mapping.reset_index().set_index("eq_dropdown")
# df_graphers["eq_dropdown_aux"] = df_graphers["Equivalence scale Dropdown"].map(
#     df_graphers_mapping["index"]
# )

# Sort by auxiliary variables and drop
df_graphers = df_graphers.sort_values(["decile_dropdown_aux", "metric_dropdown_aux"], ignore_index=True)
df_graphers = df_graphers.drop(columns=["metric_dropdown_aux", "decile_dropdown_aux"])

# ## Explorer generation
# Here, the header, tables and graphers dataframes are combined to be shown in for format required for OWID data explorers.

save("incomes-across-distribution-lis", tables, df_header, df_graphers, df_tables)  # type: ignore
