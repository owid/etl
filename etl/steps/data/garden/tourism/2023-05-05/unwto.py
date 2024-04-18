"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unwto.start")
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("unwto")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["unwto"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("unwto.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Set multi-level index and check that index is unique
    df.set_index(["country", "year"], inplace=True)
    assert df.index.is_unique, "Index is not well constructed"

    # Convert all the columns of the DataFrame 'df' into float32 data type.
    df = convert_columns_to_float32(df)

    # Shorten the names of the columns in the DataFrame 'df'.
    df = shorten_column_names(df)

    # Calculate the sum of values by year in the DataFrame 'df' and store the result in 'df_sum'.
    df_sum = calculate_sum_by_year(df)

    # Reset the index of 'df_sum'.
    df_sum.reset_index(inplace=True)

    # Concatenate 'df' and 'df_sum' into a single DataFrame 'merged_df'.
    merged_df = concatenate_dataframes(df, df_sum)

    # Calculate the sum of values by year for the countries Bonaire, Sint Eustatius, and Saba
    # in the DataFrame 'merged_df', and store the result in 'sum_bon_sint_saba'.
    sum_bon_sint_saba = calculate_sum_by_year_Bonaire_Sint_Eustatius_Saba(merged_df)

    # Drop rows in 'merged_df' that correspond to the countries 'Saba', 'Sint Eustatius',
    # and 'Bonaire', and store the resulting DataFrame in 'merged_df_drop_'.
    merged_df_drop_ = merged_df.loc[~merged_df.country.isin(["Saba", "Sint Eustatius", "Bonaire"])]
    # Concatenate 'merged_df_drop_' and 'sum_bon_sint_saba' into a single DataFrame 'merged_df_concat'.
    # The rows of 'sum_bon_sint_saba' will be appended to 'merged_df_drop_'.
    merged_df_concat = pd.concat([merged_df_drop_, sum_bon_sint_saba], ignore_index=True)

    # Set index, check that it's unique and reset index
    assert not merged_df_concat[["country", "year"]].duplicated().any(), "Index is not well constructed"  # type: ignore

    # Aggregate data by region (decided not to do for now)
    # Africa, Oceania, and income level categories
    ## regions_ = ["North America",
    #     "South America",
    #     "Europe",
    #     "Africa",
    #     "Asia",
    #     "Oceania",
    #     "European Union (27)"]

    # Add region aggregates to the DataFrame
    # for region in regions_:
    #    merged_df_concat = geo.add_region_aggregates(df=merged_df_concat, country_col='country', year_col='year', region=region)
    # Set and validate the index
    # merged_df_concat_transf.set_index(['country', 'year'], inplace=True)
    # assert len(merged_df_concat_transf.index.levels) == 2 and merged_df_concat_transf.index.is_unique, "Index is not well constructed"

    # Add population data to the DataFrame
    merged_df_concat_transf = geo.add_population_to_dataframe(
        merged_df_concat, country_col="country", year_col="year", population_col="population"
    )
    merged_df_concat_transf.set_index(["country", "year"], inplace=True)

    # Store the original column names
    original_columns = merged_df_concat_transf.columns.tolist()

    # Drop columns with all NaN values
    merged_df_concat_transf = merged_df_concat_transf.dropna(axis=1, how="all")

    # Store the updated column names
    updated_columns = merged_df_concat_transf.columns.tolist()

    # Print the names of dropped columns
    dropped_columns = [col for col in original_columns if col not in updated_columns]
    print("Dropped columns:", dropped_columns)
    print("Number of remaining columns:", len(merged_df_concat_transf.columns))

    # Multiply by a thousand columns that have a unit thousands
    cols_unit_not_thousands = [
        "to_in_av_ca_be_pl_pe_10_in",
        "to_in_av_le_of_st",
        "to_in_nu_of_be_pl",
        "to_in_nu_of_es",
        "to_in_nu_of_ro",
        "to_in_oc_ra_be_pl",
        "to_in_oc_ra_ro",
        "in_to_ex_pa_tr",
        "in_to_ex_tr",
        "ou_to_ex_pa_tr",
        "ou_to_ex_tr",
        "population",
    ]

    for col in merged_df_concat_transf.columns:
        if col not in cols_unit_not_thousands:
            merged_df_concat_transf[col] = merged_df_concat_transf[col] * 1000

    # Perform calculations on columns of interest to transform their values to per 1000 individuals
    columns_to_transform = [
        "ou_to_de_to_de",
        "ou_to_de_ov_vi_to",
        "ou_to_de_sa_da_vi_ex",
        "do_to_tr_to_tr",
        "do_to_tr_ov_vi_to",
        "do_to_tr_sa_da_vi_ex",
        "in_to_ar_to_ar",
        "in_to_ar_ov_vi_to",
        "in_to_ar_sa_da_vi_ex",
        "em_fo_an_be_se_ac",
        "em_to",
    ]

    for col in columns_to_transform:
        merged_df_concat_transf[f"{col}_per_1000"] = per_1000(merged_df_concat_transf, col)

    # Calculate the Business/Personal Tourism column
    merged_df_concat_transf["bus_pers"] = (
        merged_df_concat_transf["in_to_pu_bu_an_pr"] / merged_df_concat_transf["in_to_pu_pe"]
    )

    # Calculate the Inbound/Outbound Ratio (tourists) column
    merged_df_concat_transf["inb_outb_tour"] = (
        merged_df_concat_transf["in_to_ar_ov_vi_to"] / merged_df_concat_transf["ou_to_de_ov_vi_to"]
    )

    # Calculate same-day by tourist trips ratio
    merged_df_concat_transf["same_tourist_ratio"] = (
        merged_df_concat_transf["in_to_ar_sa_da_vi_ex"] / merged_df_concat_transf["in_to_ar_ov_vi_to"]
    )

    # Convert US million to number
    merged_df_concat_transf["ou_to_ex_tr"] = merged_df_concat_transf["ou_to_ex_tr"] * 1e6
    merged_df_concat_transf["in_to_ex_tr"] = merged_df_concat_transf["in_to_ex_tr"] * 1e6

    merged_df_concat_transf.reset_index(inplace=True)  # reset index

    # drop variables that are unlikely to be used
    columns_to_exclude = [
        "in_to_ar_of_wh_cr_pa",
        "do_to_ac_to_gu",
        "do_to_ac_to_ov",
        "em_ot_ac_se",
        "em_ot_to_in",
        "em_pa_tr",
        "em_tr_ag_an_ot_re_se_ac",
        "in_to_ac_to_gu",
        "in_to_ac_to_ov",
        "in_to_ex_pa_tr",
        "in_to_re_af",
        "in_to_re_am",
        "in_to_re_ea_as_an_th_pa",
        "in_to_re_eu",
        "in_to_re_mi_ea",
        "in_to_re_ot_no_cl",
        "in_to_re_so_as",
        "in_to_re_to",
        "in_to_re_of_wh_na_re_ab",
        "in_to_tr_to",
        "ou_to_ex_pa_tr",
        "to_in_nu_of_be_pl",
        "to_in_nu_of_es",
        "to_in_nu_of_ro",
        "to_in_oc_ra_be_pl",
        "in_to_pu_to",
        "in_to_ar_to_ar",
        "do_to_ac_ho_an_si_es_ov",
        "do_to_tr_sa_da_vi_ex",
        "do_to_tr_to_tr",
        "em_ac_se_fo_vi_ho_an_si_es",
        "in_to_ac_ho_an_si_es_ov",
        "in_to_tr_ai",
        "in_to_tr_la",
        "in_to_tr_wa",
        "ou_to_de_to_de",
        "to_in_av_ca_be_pl_pe_10_in",
        "to_in_oc_ra_ro",
        "population",
    ]
    merged_df_concat_transf = merged_df_concat_transf.drop(columns_to_exclude, axis=1)

    df_inflation_adjusted = add_ppp_and_cpi(merged_df_concat_transf)
    df_final = pd.merge(merged_df_concat_transf, df_inflation_adjusted, on=["country", "year"], how="outer")
    df_final.set_index(["country", "year"], verify_integrity=True, inplace=True)

    # Create a new table with the processed data.
    tb_garden = Table(df_final, short_name=paths.short_name)

    tb_garden.metadata = TableMeta(short_name=paths.short_name, primary_key=list(df_final.index.names))
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("unwto.end")


def add_ppp_and_cpi(df_tourism):
    # Load WDI CPI for adjusting inflation
    ds_wdi = cast(Dataset, paths.load_dependency("wdi"))
    tb_wdi = ds_wdi["wdi"]

    df_wdi_cpi = tb_wdi[["fp_cpi_totl"]]
    df_wdi_cpi.reset_index(inplace=True)

    # Load OECD exchange rates
    ds_oecd = cast(Dataset, paths.load_dependency("ppp_exchange_rates"))
    tb_oecd = ds_oecd["ppp_exchange_rates"]

    # Extract tourism expenditure
    inboud_exp = df_tourism[["country", "year", "in_to_ex_tr", "ou_to_ex_tr"]]

    # Merge expenditure, inflation and exchange rates
    merg_inbound_exp = pd.merge(inboud_exp, df_wdi_cpi, on=["country", "year"], how="inner")
    merg_inbound_exp_oecd = pd.merge(merg_inbound_exp, tb_oecd, on=["country", "year"], how="inner")

    # Filter the dataframe for year 2021
    df_2021 = merg_inbound_exp_oecd[merg_inbound_exp_oecd["year"] == 2021]

    # Merge the 2021 values with the original dataframe based on the 'country' column
    df = pd.merge(
        merg_inbound_exp_oecd,
        df_2021[["country", "fp_cpi_totl", "purchasing_power_parities_for_private_consumption"]],
        on="country",
        suffixes=("", "_2021"),
    )
    # CPI from 2021 instead of 2010
    df["fp_cpi_totl_normalized"] = 100 * df["fp_cpi_totl"] / df["fp_cpi_totl_2021"]

    # Convert to inbound expenditure to local currency, adjust for local inflation and convert back to international dollars
    df["inbound_ppp_cpi_adj_2021"] = (
        100 * (df["in_to_ex_tr"] * df["exchange_rates__period_average"]) / df["fp_cpi_totl_normalized"]
    ) / df["purchasing_power_parities_for_private_consumption_2021"]

    # Adjust outbound by US inflaton (2021)
    # Filter the dataframe for the United States
    us_df = df[df["country"] == "United States"]

    # Get the US value in 2021 for fp_cpi_totl
    us_cpi_2021 = us_df.loc[us_df["year"] == 2021, "fp_cpi_totl"].values[0]

    # Normalize the 'fp_cpi_totl' column by dividing it by the US value in 2021
    us_df_copy = us_df.copy()
    us_df_copy.loc[:, "US_cpi"] = (us_df_copy["fp_cpi_totl"] / us_cpi_2021) * 100

    # Merge the normalized values with the original dataframe based on the 'year' column
    df_us_inflation = pd.merge(df, us_df_copy[["year", "US_cpi"]], on="year", how="left")
    df_us_inflation["outbound_us_cpi_adjust"] = 100 * df_us_inflation["ou_to_ex_tr"] / (df_us_inflation["US_cpi"])

    df_inflation_var = pd.merge(
        df[["country", "year", "inbound_ppp_cpi_adj_2021"]],
        df_us_inflation[["country", "year", "outbound_us_cpi_adjust"]],
        on=["country", "year"],
        how="inner",
    )
    return df_inflation_var


def shorten_name(name):
    # remove underscores and convert to title case
    name = name.replace("_", " ").title()
    # extract first letter of each word
    words = name.split()
    initials = "_".join([word[:2].lower() for word in words])

    return initials


def shorten_column_names(df):
    newnames = [shorten_name(name) for name in df.columns]
    df.columns = newnames
    return df


def per_1000(df, column):
    return df[column] / (df["population"] / 1000)


def convert_columns_to_float32(df):
    for col in df.columns:
        if df[col].dtype != "float32":
            df[col] = df[col].astype(str).replace(["", "<NA>"], np.nan).astype("float32")
    return df


def calculate_sum_by_year(df):
    regional_columns = [
        "in_to_re_af",
        "in_to_re_am",
        "in_to_re_ea_as_an_th_pa",
        "in_to_re_eu",
        "in_to_re_mi_ea",
        "in_to_re_ot_no_cl",
        "in_to_re_so_as",
    ]
    df_sum = df[regional_columns].reset_index().groupby("year").sum(numeric_only=True)
    df_sum.columns = [
        "Africa",
        "Americas",
        "East Asia and the Pacific",
        "Europe",
        "Middle East",
        "Not classified",
        "South Asia",
    ]
    return df_sum


def calculate_sum_by_year_Bonaire_Sint_Eustatius_Saba(merged_df):
    numeric_cols = merged_df.select_dtypes(include=np.number).columns
    sum_by_year = {}
    for col in numeric_cols[1:]:
        sum_by_year[col] = (
            merged_df[merged_df["country"].isin(["Saba", "Sint Eustatius", "Bonaire"])]
            .groupby(["year"])[col]
            .apply(lambda x: x.sum(skipna=False))
        )
    sum_by_year = pd.DataFrame(sum_by_year)
    sum_by_year["country"] = "Bonaire Sint Eustatius and Saba"
    sum_by_year = sum_by_year.reset_index()
    return sum_by_year


def concatenate_dataframes(df, df_sum):
    df_melted = df_sum.melt(id_vars=["year"], var_name="country", value_name="inb_tour_region")
    merged_df = pd.concat([df.reset_index(), df_melted], axis=0)
    return merged_df
