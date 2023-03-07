"""Load a garden dataset and create an explorers dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset
from etl.steps.data.garden.who.latest.fluid import calculate_patient_rates
from etl.steps.data.garden.who.latest.flunet import calculate_percent_positive

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MIN_DATA_POINTS = 20


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
    tb_flu = create_zero_filled_strain_columns(tb_flu)
    tb_flu = remove_sparse_timeseries(df=tb_flu, min_data_points=MIN_DATA_POINTS)
    tb_flu = create_regional_aggregates(df=tb_flu)

    # Create monthly aggregates - sum variables that are counts and recalculate rates based on these monthly totals
    tb_flu_monthly = create_monthly_aggregates(df=tb_flu)
    #assert tb_flu[["country", "date"]].duplicated().sum() == 0
    tb_flu = Table(tb_flu, short_name="flu")
    tb_flu_monthly = Table(tb_flu_monthly, short_name="flu_monthly")
    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(
        dest_dir, tables=[tb_flu, tb_flu_monthly], default_metadata=flunet_garden.metadata, formats=["csv"]
    )
    ds_explorer.save()


def create_zero_filled_strain_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For the stacked bar charts in the grapher to work I think I need to fill the NAs with zeros. I'll keep the original columns too as I think adding 0s into line charts would look weird and be misleading
    """
    strain_columns = [
        "ah1n12009combined",
        "ah1combined",
        "ah3combined",
        "ah5combined",
        "ah7n9combined",
        "a_no_subtypecombined",
        "byamcombined",
        "bnotdeterminedcombined",
        "bviccombined",
        "inf_acombined",
        "inf_bcombined",
        "ah1n12009sentinel",
        "ah1sentinel",
        "ah3sentinel",
        "ah5sentinel",
        "ah7n9sentinel",
        "byamsentinel",
        "bnotdeterminedsentinel",
        "bvicsentinel",
        "inf_asentinel",
        "inf_bsentinel",
        "ah1n12009nonsentinel",
        "ah1n12009notdefined",
        "ah1notdefined",
        "ah1nonsentinel",
        "ah3nonsentinel",
        "ah3notdefined",
        "ah5nonsentinel",
        "ah5notdefined",
        "ah7n9nonsentinel",
        "ah7n9notdefined",
        "a_no_subtypenotdefined",
        "a_no_subtypesentinel",
        "byamnonsentinel",
        "byamnotdefined",
        "bnotdeterminednonsentinel",
        "bnotdeterminednotdefined",
        "bvicnonsentinel",
        "bvicnotdefined",
    ]
    # Add these columns if we need to, for now stick to the above for file size reasons

    strain_columns_zfilled = [s + "_zfilled" for s in strain_columns]
    df[strain_columns_zfilled] = df[strain_columns].fillna(0)
    return df


def remove_sparse_timeseries(df: pd.DataFrame, min_data_points: int) -> pd.DataFrame:
    """
    For each country identify if they have < {min_data_points} confirmed flu cases.

    If they do then set all their values for flu cases to NA, we don't want to show super sparse countries

    For each of the ari/sari/ili columns we apply the same rule, if it has less than {min_data_points} then we set it to NA for that country

    Also, remove flunet columns (not zero-filled) that are only NA or 0 as we don't want line charts for these.
    """
    countries = df["country"].drop_duplicates()
    cols = df.columns.drop(["country", "date", "hemisphere"])
    # all columns that have been zerofilled so they can be used in stacked bar charts
    z_filled_cols = [col for col in cols if col.endswith("zfilled")]
    # all columns that contain values on confirmed flu cases
    all_flunet_cols = cols[(cols.str.contains("sentinel|notdefined|combined"))]
    # all columns that will be used in line charts but not stacked bar charts
    fluid_cols = cols[(cols.str.contains("ili|ari"))].to_list()

    not_z_filled_flunet_cols = [col for col in all_flunet_cols if not col.endswith("zfilled")]
    assert len(not_z_filled_flunet_cols) + len(z_filled_cols) + len(fluid_cols) == len(cols)
    for country in countries:
        # Removing all flunet values for a country where
        if all(df.loc[(df["country"] == country), z_filled_cols].fillna(0).astype(bool).sum() <= min_data_points):
            df.loc[(df["country"] == country), all_flunet_cols] = np.NaN
        for fluid_col in fluid_cols:
            # Removing rows from fluid columns where there are fewer than {min_data_points} for a country
            df[fluid_col] = df[fluid_col].astype(np.float32)
            if df.loc[(df["country"] == country), fluid_col].fillna(0).astype(bool).sum() <= min_data_points:
                df.loc[(df["country"] == country), fluid_col] = np.NaN
        for flunet_col in not_z_filled_flunet_cols:
            # Removing rows from columns to be used in line charts where there are no non-NA or 0 values for a country (where it would show a flat 0 or NA line)
            df[flunet_col] = df[flunet_col].astype(np.float32)
            if df.loc[(df["country"] == country), flunet_col].fillna(0).astype(bool).sum() == 0:
                df.loc[(df["country"] == country), flunet_col] = np.NaN

    return df


def create_monthly_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    df["month"] = pd.DatetimeIndex(df["date"]).month
    df["year"] = pd.DatetimeIndex(df["date"]).year
    df["month_date"] = pd.to_datetime(df[["year", "month"]].assign(DAY=1))
    df = df.drop(columns=["month"])

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

    month_agg_df = calculate_percent_positive(
        df=month_agg_df, surveillance_cols=["sentinel", "nonsentinel", "notdefined", "combined"]
    )

    month_agg_df = calculate_patient_rates(df=month_agg_df)

    return month_agg_df


def create_regional_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create global and hemispherical aggregates for the flu data.
    Recalculate the rate columns at these aggregate levels
    Combine the global and hemisphere aggregates with the original data and return it
    """
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

    global_aggregate = create_global_aggregate(df, count_cols)
    hemisphere_aggregate = create_hemisphere_aggregate(df, count_cols)
    uk_aggregate = create_united_kingdom_aggregate(df, count_cols)

    df = df.drop(columns=["hemisphere"])

    df = pd.concat([df, global_aggregate, hemisphere_aggregate, uk_aggregate])

    return df


def create_united_kingdom_aggregate(df: pd.DataFrame, count_cols) -> pd.DataFrame:
    """
    Summing the flunet data for England, Wales, Scotland and N.Ireland to create a United Kingdom entity
    """
    uk_df = df[df["country"].isin(["England", "Wales", "Scotland", "Northern Ireland"])]

    # Check all nations are in the subset - in case of name changes
    assert len(uk_df.country.drop_duplicates()) == 4

    uk_agg = uk_df[count_cols].groupby(["date"]).sum(min_count=1, numeric_only=True).reset_index()

    uk_agg["country"] = "United Kingdom"

    cols = uk_agg.columns.to_list()
    cols = cols[-1:] + cols[:-1]
    uk_agg = uk_agg[cols]
    uk_agg = calculate_percent_positive(
        df=uk_agg, surveillance_cols=["sentinel", "nonsentinel", "notdefined", "combined"]
    )
    uk_agg = calculate_patient_rates(df=uk_agg)

    return uk_agg


def create_hemisphere_aggregate(df: pd.DataFrame, count_cols) -> pd.DataFrame:
    hemisphere_aggregate = (
        df[count_cols].groupby(["hemisphere", "date"]).sum(min_count=1, numeric_only=True).reset_index()
    )

    hemisphere_aggregate["hemisphere"] = hemisphere_aggregate["hemisphere"].replace(
        {"NH": "Northern Hemisphere", "SH": "Southern Hemisphere"}
    )
    hemisphere_aggregate = hemisphere_aggregate.rename(columns={"hemisphere": "country"})

    hemisphere_aggregate = calculate_percent_positive(
        df=hemisphere_aggregate, surveillance_cols=["sentinel", "nonsentinel", "notdefined", "combined"]
    )
    hemisphere_aggregate = calculate_patient_rates(df=hemisphere_aggregate)
    return hemisphere_aggregate


def create_global_aggregate(df: pd.DataFrame, count_cols) -> pd.DataFrame:
    global_aggregate = df[count_cols].groupby(["date"]).sum(min_count=1, numeric_only=True).reset_index()
    global_aggregate["country"] = "World"
    cols = global_aggregate.columns.to_list()
    cols = cols[-1:] + cols[:-1]
    global_aggregate = global_aggregate[cols]
    global_aggregate = calculate_percent_positive(
        df=global_aggregate, surveillance_cols=["sentinel", "nonsentinel", "notdefined", "combined"]
    )
    global_aggregate = calculate_patient_rates(df=global_aggregate)
    return global_aggregate
