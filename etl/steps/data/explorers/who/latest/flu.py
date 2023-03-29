"""
Load in both the FluNet and FluID datasets from garden and merge them.
As we use stacked bar charts in the explorer we need to add a few special steps:

* Ensure there is a row for each week between the start and end week for each country, this is done in create_full_time_series()
* Ensure that for any country we want to show in a stacked bar chart there are 0s instead of NAs

Further steps we take:

* Remove data for country-variable combinations when there are fewer than 20 datapoints - set using MIN_DATA_POINTS
* Calculate aggregates for both the global total and both hemispheres
* For these aggregates we do not use inf_negative as a denominator, species processed is available many more countries and gives a better representation of the data. When using inf_negative many of the early datapoints are near 100%.
* Create monthly aggregates where we sum the count variables and average the rate variables
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset
from etl.steps.data.garden.who.latest.fluid import calculate_patient_rates

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Remove data for countries which have fewer than this value
# MIN_DATA_POINTS = 20
MIN_DATA_POINTS_PER_YEAR = 10
# We hold back the most recent 28 days worth of data as these are often incomplete - it looks as if the data artificially drops if these days are included.
DAYS_HELD_BACK = 28


def run(dest_dir: str) -> None:
    """
    Load in both the fluid and flunet datasets and merge them on country and date
    - Check that all dates match (they should)
    """
    #
    # Load inputs.
    #
    # Load garden dataset.
    flunet_garden: Dataset = paths.load_dependency("flunet")
    fluid_garden: Dataset = paths.load_dependency("fluid")

    # Read table from garden dataset.
    tb_flunet = flunet_garden["flunet"]
    tb_fluid = fluid_garden["fluid"]

    tb_flu = pd.DataFrame(pd.merge(tb_fluid, tb_flunet, on=["country", "date", "hemisphere"], how="outer"))
    assert tb_flu[["country", "date"]].duplicated().sum() == 0

    # Remove data prior to 2009 as we don't show this on the explorer
    tb_flu = tb_flu.loc[tb_flu["date"] > "2008-12-29"]

    tb_flu = create_full_time_series(tb_flu)
    tb_flu = remove_sparse_years(tb_flu, min_datapoints_per_year=MIN_DATA_POINTS_PER_YEAR)
    tb_flu = fill_flu_data_gaps_with_zero(tb_flu)
    #
    tb_flu = create_zero_filled_strain_columns(tb_flu)

    # tb_flu = remove_sparse_timeseries(df=tb_flu, min_data_points=MIN_DATA_POINTS)
    tb_flu = create_regional_aggregates(df=tb_flu)

    # hold back the last 28 days of data as it takes some time for data to filter in from countries
    tb_flu = hold_back_data(df=tb_flu, days_held_back=DAYS_HELD_BACK)
    assert tb_flu[["country", "date"]].duplicated().sum() == 0
    # Create monthly aggregates - sum variables that are counts and recalculate rates based on these monthly totals
    tb_flu_monthly = create_monthly_aggregates(df=tb_flu)

    # Create Tables
    tb_flu = Table(tb_flu, short_name="flu")
    tb_flu_monthly = Table(tb_flu_monthly, short_name="flu_monthly")
    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(
        dest_dir, tables=[tb_flu, tb_flu_monthly], default_metadata=flunet_garden.metadata, formats=["csv"]
    )
    ds_explorer.save()


def hold_back_data(df: pd.DataFrame, days_held_back: int) -> pd.DataFrame:
    """
    Removing the last {days_held_back} days from the data, these values are typically adjusted in the following weeks
    """
    todays_date = datetime.now().date()
    date_limit = todays_date - timedelta(days=days_held_back)
    date_limit = datetime.strftime(date_limit, format="%Y-%m-%d")  # type: ignore
    df = df[df["date"] <= date_limit]
    assert all(df["date"] <= date_limit)
    return df


def fill_na_with_zero(group_df: pd.DataFrame, col: str) -> pd.DataFrame:
    first_non_na = group_df[col].first_valid_index()
    last_non_na = group_df[col].last_valid_index()
    if first_non_na is not None:
        group_df[col].loc[first_non_na + 1 : last_non_na] = group_df[col].loc[first_non_na + 1 : last_non_na].fillna(0)
    return group_df


def fill_flu_data_gaps_with_zero(df: pd.DataFrame) -> pd.DataFrame:
    # put all the right columns in and make sure they aren't categories
    cols = df.columns.drop(["country", "date", "hemisphere", "year"])
    # all columns that contain values on confirmed flu cases
    all_flunet_cols = cols[(cols.str.contains("sentinel|notdefined|combined"))]
    # all columns that will be used in line charts but not stacked bar charts
    fluid_cols = cols[(cols.str.contains("ili|ari"))].to_list()
    not_z_filled_flunet_cols = [col for col in all_flunet_cols if not col.endswith("zfilled")]
    flu_cols = fluid_cols + not_z_filled_flunet_cols
    df[flu_cols] = df[flu_cols].apply(pd.to_numeric)
    df_out = df.copy()
    df_out = df_out.sort_values(["country", "date"]).reset_index(drop=True)
    for col in flu_cols:
        group_cols = ["country", "date", col]
        df_filled = df_out[group_cols].groupby("country", group_keys=False).apply(fill_na_with_zero, col=col)
        df_out[col] = df_filled[col]

    return df_out


def create_zero_filled_strain_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For the stacked bar charts in the grapher to work I think I need to fill the NAs with zeros. I'll keep the original columns too as I think adding 0s into line charts would look weird and be misleading
    """
    strain_columns = [
        "ah1n12009combined",
        "ah1n12009sentinel",
        "ah1n12009nonsentinel",
        "ah1n12009notdefined",
        "ah1combined",
        "ah1sentinel",
        "ah1nonsentinel",
        "ah1notdefined",
        "ah3combined",
        "ah3sentinel",
        "ah3nonsentinel",
        "ah3notdefined",
        "ah5combined",
        "ah5sentinel",
        "ah5nonsentinel",
        "ah5notdefined",
        "ah7n9combined",
        "ah7n9sentinel",
        "ah7n9nonsentinel",
        "ah7n9notdefined",
        "a_no_subtypecombined",
        "a_no_subtypesentinel",
        "a_no_subtypenonsentinel",
        "a_no_subtypenotdefined",
        "byamcombined",
        "byamsentinel",
        "byamnonsentinel",
        "byamnotdefined",
        "bnotdeterminedcombined",
        "bnotdeterminedsentinel",
        "bnotdeterminednonsentinel",
        "bnotdeterminednotdefined",
        "bviccombined",
        "bvicsentinel",
        "bvicnonsentinel",
        "bvicnotdefined",
    ]
    # Add these columns if we need to, for now stick to the above for file size reasons

    strain_columns_zfilled = [s + "_zfilled" for s in strain_columns]
    df[strain_columns_zfilled] = df[strain_columns].fillna(0)
    return df


def create_full_time_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each country ensure there is a value for each week between the start and the end date,
    especially important for the stacked bar charts which don't automatically fill missing dates with NAs

    """
    filled_df = pd.DataFrame()
    for country in df.country.drop_duplicates():
        country_df = df[df["country"] == country]
        min_date = country_df.date.min()
        max_date = country_df.date.max()
        date_series = pd.Series(pd.date_range(min_date, max_date, freq="7D").format(), name="date")

        if len(date_series[~date_series.isin(country_df["date"])]) > 0:
            country_df = pd.merge(country_df, date_series, how="outer")
            country_df[["country", "hemisphere"]] = country_df[["country", "hemisphere"]].fillna(method="ffill")
            assert len(date_series) == country_df.shape[0]
            assert country_df.country.isna().sum() == 0

        filled_df = pd.concat([filled_df, country_df])

    return filled_df


def remove_sparse_years(df: pd.DataFrame, min_datapoints_per_year: int) -> pd.DataFrame:
    """
    If a year has fewer than {min_data_points_per_year} then we should remove all the data for that year -> set it to NA
    Unless it is the current year, then we do not change it.
    """

    df["year"] = pd.to_datetime(df["date"]).dt.year
    constant_cols = ["country", "date", "hemisphere", "year"]
    cols = df.columns.drop(constant_cols)
    current_year = datetime.today().year
    for col in cols:
        df_col = df.loc[:, ["country", "year", col]]
        df_col[col] = pd.to_numeric(df_col[col])
        df_col_bool = (
            df_col.groupby(["country", "year"]).agg(weeks_gt_zero=(col, lambda x: x.gt(0).sum())).reset_index()
        )
        df = pd.merge(df, df_col_bool, on=["country", "year"])

        df.loc[
            (df["weeks_gt_zero"] < min_datapoints_per_year) & (df["year"] < current_year),
            col,
        ] = np.nan
        df = df.drop(columns=["weeks_gt_zero"])

    return df


def create_monthly_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate weekly data into months. For simplicity, if the week commences in a certain month we include it in that month.

    We sum counts and average rates to calculate the monthly values.

    """

    df["month"] = pd.DatetimeIndex(df["date"]).month
    df["year"] = pd.DatetimeIndex(df["date"]).year
    df["month_date"] = pd.to_datetime(df[["year", "month"]].assign(DAY=1))

    # Dropping any values for the current month - we shouldn't show data for months that aren't complete.
    current_month = pd.to_datetime(datetime.now().date().strftime("%Y-%m-01"))
    df = df.drop(columns=["month"])
    df = df[df["month_date"] != current_month]
    cols = df.columns.drop(["date", "year"])
    # Columns that will need to be recalculated after aggregating
    rate_cols = [
        "ili_cases_per_thousand_outpatients",
        "ari_cases_per_thousand_outpatients",
        "sari_cases_per_hundred_inpatients",
        "pcnt_possentinel",
        "pcnt_posnonsentinel",
        "pcnt_posnotdefined",
        "pcnt_poscombined",
    ]
    # columns that we can aggregate by summing
    count_cols = cols.drop(rate_cols)

    month_agg_df = df[count_cols].groupby(["country", "month_date"]).sum(min_count=1, numeric_only=True).reset_index()

    # columns that are aggregated by averaging (rates)
    rate_agg_cols = ["country", "month_date"] + rate_cols
    rate_agg_df = df[rate_agg_cols].groupby(["country", "month_date"]).mean(numeric_only=True).reset_index()

    month_agg_df = pd.merge(month_agg_df, rate_agg_df, on=["country", "month_date"], how="outer")

    return month_agg_df


def create_regional_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create global and hemispherical aggregates for the flu data.
    Recalculate the rate columns at these aggregate levels
    Combine the global and hemisphere aggregates with the original data and return it
    """
    df_orig = df
    # UK data is also counted in england, wales, scotland and n.ireland
    df = df[df["country"] != "United Kingdom"]

    cols = df.columns.drop(["country"])
    # Columns that will need to be recalculated after aggregating
    rate_cols = [
        "ili_cases_per_thousand_outpatients",
        "ari_cases_per_thousand_outpatients",
        "sari_cases_per_hundred_inpatients",
        "pcnt_possentinel",
        "pcnt_posnonsentinel",
        "pcnt_posnotdefined",
        "pcnt_poscombined",
    ]
    # columns that we can aggregate by summing
    count_cols = cols.drop(rate_cols)

    global_aggregate = create_global_aggregate(df=df, count_cols=count_cols, min_countries=20, rate_cols=rate_cols)
    hemisphere_aggregate = create_hemisphere_aggregate(df, count_cols)

    df_orig = df_orig.drop(columns=["hemisphere"])

    df_out = pd.concat([df_orig, global_aggregate, hemisphere_aggregate])

    return df_out


def create_hemisphere_aggregate(df: pd.DataFrame, count_cols) -> pd.DataFrame:
    hemisphere_aggregate = (
        df[count_cols].groupby(["hemisphere", "date"]).sum(min_count=1, numeric_only=True).reset_index()
    )

    hemisphere_aggregate["hemisphere"] = hemisphere_aggregate["hemisphere"].replace(
        {"NH": "Northern Hemisphere", "SH": "Southern Hemisphere"}
    )
    hemisphere_aggregate = hemisphere_aggregate.rename(columns={"hemisphere": "country"})

    hemisphere_aggregate = calculate_percent_positive_aggregate(
        df=hemisphere_aggregate, surveillance_cols=["sentinel", "nonsentinel", "notdefined", "combined"]
    )
    hemisphere_aggregate = calculate_patient_rates(df=hemisphere_aggregate)
    return hemisphere_aggregate


def create_global_aggregate(df: pd.DataFrame, count_cols: list[str], min_countries: int, rate_cols) -> pd.DataFrame:
    global_aggregate = df[count_cols].groupby(["date"]).sum(min_count=1, numeric_only=True).reset_index()
    global_aggregate["country"] = "World"
    cols = global_aggregate.columns.to_list()
    cols = cols[-1:] + cols[:-1]
    global_aggregate = global_aggregate[cols]
    global_aggregate = calculate_percent_positive_aggregate(
        df=global_aggregate, surveillance_cols=["sentinel", "nonsentinel", "notdefined", "combined"]
    )
    global_aggregate = calculate_patient_rates(df=global_aggregate)

    return global_aggregate


def calculate_percent_positive_aggregate(df: pd.DataFrame, surveillance_cols: list[str]) -> pd.DataFrame:
    """
    Sometimes the 0s in the inf_negative* columns should in fact be zero. Here we convert rows to NA where:
    inf_negative* == 0 and the sum of the positive and negative tests does not equal the number of processed tests.

    This should keep true 0s where the share of positive tests is actually 100%, typically when there is a small number of tests.

    Because the data is patchy in some places the WHO recommends three methods for calclating the share of influenza tests that are positive.
    In order of preference
    1. Positive tests divided by specimens processed: inf_all/spec_processed_nb
    2. Positive tests divided by specimens received: inf_all/spec_received_nb

    We do no consider inf_negative at the regional level because in the earlier years it is often only available for single countries and over-inflates the share of positive tests.
    This is not the case for specimens processed.

    Remove rows where the percent is > 100
    Remove rows where the percent = 100 but all available denominators are 0.
    """
    for col in surveillance_cols:

        df.loc[
            (df["inf_negative" + col] == 0)
            & (df["inf_negative" + col] + df["inf_all" + col] != df["spec_processed_nb" + col]),
            "inf_negative" + col,
        ] = np.nan

        # df["pcnt_pos_1" + col] = (df["inf_all" + col] / (df["inf_all" + col] + df["inf_negative" + col])) * 100
        df["pcnt_pos_2" + col] = (df["inf_all" + col] / df["spec_processed_nb" + col]) * 100
        df["pcnt_pos_3" + col] = (df["inf_all" + col] / df["spec_received_nb" + col]) * 100

        # hierachically fill the 'pcnt_pos' column with values from the columns described above in order of preference: 1->2->3
        df["pcnt_pos" + col] = df["pcnt_pos_2" + col]
        df["pcnt_pos" + col] = df["pcnt_pos" + col].fillna(df["pcnt_pos_3" + col])

        df = df.drop(columns=["pcnt_pos_2" + col, "pcnt_pos_3" + col])

        # Drop rows where pcnt_pos is >100
        df.loc[df["pcnt_pos" + col] > 100, "pcnt_pos" + col] = np.nan

        # Rows where the percentage positive is 100 but all possible denominators are 0
        df.loc[
            (df["pcnt_pos" + col] == 100)
            & (df["inf_negative" + col] == 0)
            & (df["spec_processed_nb" + col] == 0)
            & (df["spec_received_nb" + col] == 0),
            "pcnt_pos" + col,
        ] = np.nan
        # df = df.dropna(axis=1, how="all")

    return df
