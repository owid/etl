"""Load a meadow dataset and create a garden dataset.

Check out this issue with the refactoring plan on next update https://github.com/owid/etl/issues/4215#issue-2966045001
"""

import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr
from owid.catalog.core.datasets import NULLABLE_DTYPES
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.steps.data.garden.who.latest.fluid import (
    MIN_DATA_POINTS_PER_YEAR,
    remove_sparse_years,
)

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("flunet.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("flunet")

    # Read table from meadow dataset.
    tb = ds_meadow["flunet"].reset_index(drop=True)

    # Convert nullable types to float64, otherwise we risk pd.NA and np.nan being mixed up.
    float64_cols = [col for col, dtype in tb.dtypes.items() if dtype in NULLABLE_DTYPES]
    tb[float64_cols] = tb[float64_cols].astype(float)

    #
    # Process data.
    #
    log.info("flunet.harmonize_countries")
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    tb = clean_and_format_data(tb)
    tb = split_by_surveillance_type(tb)

    tb = calculate_percent_positive(tb, surveillance_cols=["SENTINEL", "NONSENTINEL", "NOTDEFINED", "COMBINED"])
    tb = create_united_kingdom_aggregate(tb)
    tb = remove_sparse_years(tb, min_datapoints_per_year=MIN_DATA_POINTS_PER_YEAR)
    tb = tb.reset_index(drop=True)
    cols_check = tb.columns.drop(["country", "hemisphere", "date", "year"])
    tb[cols_check] = tb[cols_check].dropna(axis="rows", how="all")
    tb_garden = tb.reset_index(drop=True)
    tb_garden.metadata.short_name = paths.short_name
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("flunet.end")


def remove_nan_dates(tb: Table) -> Table:
    """There are rare mistakes in data with invalid date format."""
    ix = tb.date.isnull()
    if ix.any():
        log.warning(f"Removing {ix.sum()} rows with invalid date format")
    tb = tb[~ix]
    date_meta = tb["date"].metadata
    tb["date"] = tb["date"].dt.date.astype(str)
    tb["date"].metadata = date_meta
    return tb


def remove_rows_that_sum_incorrectly(tb: Table) -> Table:
    """
    Let's remove rows that don't add up correctly e.g.
    where influenza a + influenza b does not equal influenza all

    We can't be sure which of the columns are correct and as there are relatively few we should just remove them.
    """
    orig_rows = tb.shape[0]
    tb = tb.drop(tb[(tb["inf_a"].fillna(0) + tb["inf_b"].fillna(0)) != tb["inf_all"].fillna(0)].index)
    tb = tb.drop(
        tb[
            (
                tb["ah1n12009"].fillna(0)
                + tb["ah1"].fillna(0)
                + tb["ah3"].fillna(0)
                + tb["ah5"].fillna(0)
                + tb["ah7n9"].fillna(0)
                + tb["anotsubtyped"].fillna(0)
                + tb["anotsubtypable"].fillna(0)
                + tb["aother_subtype"].fillna(0)
            )
            != tb["inf_a"].fillna(0)
        ].index
    )

    tb = tb.drop(
        tb[
            (
                tb["bvic_2del"].fillna(0)
                + tb["bvic_3del"].fillna(0)
                + tb["bvic_nodel"].fillna(0)
                + tb["bvic_delunk"].fillna(0)
                + tb["byam"].fillna(0)
                + tb["bnotdetermined"].fillna(0)
            )
            != tb["inf_b"].fillna(0)
        ].index
    )
    new_rows = tb.shape[0]
    rows_dropped = orig_rows - new_rows
    log.info(f"{rows_dropped} rows dropped as the disaggregates did not sum correctly")
    assert rows_dropped < 20000, "More than 20,000 rows dropped, this is much more than expected"
    return tb


def split_by_surveillance_type(tb: Table) -> Table:
    """
    Pivoting the table so there is a column per variable and per surveillance type

    Summing each column and skipping NAs so there is a column of combined values
    """
    flu_cols = tb.columns.drop(["country", "date", "origin_source", "hemisphere"])

    tb_piv = tb.pivot(index=["country", "hemisphere", "date"], columns="origin_source").reset_index()

    tb_piv.columns = list(map("".join, tb_piv.columns))
    sentinel_list = ["SENTINEL", "NONSENTINEL", "NOTDEFINED"]
    for col in flu_cols:
        sum_cols = [col + s for s in sentinel_list]
        tb_piv[col + "COMBINED"] = tb_piv[sum_cols].sum(axis=1, min_count=1)
    return tb_piv


def combine_columns(tb: Table) -> Table:
    """
    Combine columns of:
    * Influenza A with no subtype
    * Influenza B Victoria substrains
    Summing so that NaNs are skipped and not converted to 0
    """
    tb["a_no_subtype"] = tb[["anotsubtyped", "anotsubtypable", "aother_subtype"]].sum(axis=1, min_count=1)
    tb["bvic"] = tb[["bvic_2del", "bvic_3del", "bvic_nodel", "bvic_delunk"]].sum(axis=1, min_count=1)

    return tb


def create_date_from_iso_week(date_iso):
    """
    Convert iso week to date format
    """
    return pr.to_datetime(date_iso, format="%Y-%m-%d", utc=True, errors="coerce")


def clean_and_format_data(tb: Table) -> Table:
    """
    Clean data by:
    * Converting date to date format
    * Combining subtype columns together
    * Drop unused columns
    """

    tb["date"] = create_date_from_iso_week(tb["iso_weekstartdate"])
    tb = remove_nan_dates(tb)
    tb = remove_rows_that_sum_incorrectly(tb)
    tb = combine_columns(tb)
    sel_cols = [
        "country",
        "hemisphere",
        "date",
        "origin_source",
        "ah1n12009",
        "ah1",
        "ah3",
        "ah5",
        "ah7n9",
        "a_no_subtype",
        "inf_a",
        "byam",
        "bnotdetermined",
        "bvic",
        "inf_b",
        "inf_all",
        "inf_negative",
        "spec_processed_nb",
        "spec_received_nb",
    ]
    tb = tb[sel_cols]
    tb = tb.drop_duplicates()

    return tb


def calculate_percent_positive(tb: Table, surveillance_cols: list[str]) -> Table:
    """
    Sometimes the 0s in the inf_negative* columns should in fact be zero. Here we convert rows where:
    inf_negative* == 0 and the sum of the positive and negative tests does not equal the number of processed tests.

    This should keep true 0s where the share of positive tests is actually 100%, typically when there is a small number of tests.

    Because the data is patchy in some places the WHO recommends three methods for calclating the share of influenza tests that are positive.
    In order of preference
    1. Postive tests divided by positive and negative tests summmed: inf_all/(inf_all + inf_neg)
    2. Positive tests divided by specimens processed: inf_all/spec_processed_nb
    3. Positive tests divided by specimens received: inf_all/spec_received_nb

    Remove rows where the percent is > 100
    Remove rows where the percent = 100 but all available denominators are 0.
    """
    for col in surveillance_cols:
        tb.loc[
            (tb["inf_negative" + col] == 0)
            & (tb["inf_negative" + col] + tb["inf_all" + col] != tb["spec_processed_nb" + col]),
            "inf_negative" + col,
        ] = np.nan

        tb["pcnt_pos_1" + col] = (tb["inf_all" + col] / (tb["inf_all" + col] + tb["inf_negative" + col])) * 100
        tb["pcnt_pos_2" + col] = (tb["inf_all" + col] / tb["spec_processed_nb" + col]) * 100
        tb["pcnt_pos_3" + col] = (tb["inf_all" + col] / tb["spec_received_nb" + col]) * 100

        # hierachically fill the 'pcnt_pos' column with values from the columns described above in order of preference: 1->2->3
        tb["pcnt_pos" + col] = tb["pcnt_pos_1" + col]
        tb["pcnt_pos" + col] = tb["pcnt_pos" + col].fillna(tb["pcnt_pos_2" + col])
        tb["pcnt_pos" + col] = tb["pcnt_pos" + col].fillna(tb["pcnt_pos_3" + col])

        tb = tb.drop(columns=["pcnt_pos_1" + col, "pcnt_pos_2" + col, "pcnt_pos_3" + col])

        # Drop rows where pcnt_pos is >100
        tb.loc[tb["pcnt_pos" + col] > 100, "pcnt_pos" + col] = np.nan

        # Rows where the percentage positive is 100 but all possible denominators are 0
        tb.loc[
            (tb["pcnt_pos" + col] == 100)
            & (tb["inf_negative" + col] == 0)
            & (tb["spec_processed_nb" + col] == 0)
            & (tb["spec_received_nb" + col] == 0),
            "pcnt_pos" + col,
        ] = np.nan

    return tb


def create_united_kingdom_aggregate(tb: Table) -> Table:
    """
    Summing the flunet data for England, Wales, Scotland and N.Ireland to create a United Kingdom entity
    """
    cols = tb.columns.drop(["country"])
    # Columns that will need to be recalculated after aggregating
    rate_cols = [
        "pcnt_posSENTINEL",
        "pcnt_posNONSENTINEL",
        "pcnt_posNOTDEFINED",
        "pcnt_posCOMBINED",
    ]
    # columns that we can aggregate by summing
    count_cols = cols.drop(rate_cols)
    uk_tb = tb[tb["country"].isin(["England", "Wales", "Scotland", "Northern Ireland"])]

    # Check all nations are in the subset - in case of name changes
    assert len(uk_tb.country.drop_duplicates()) == 4

    uk_agg = uk_tb[count_cols].groupby(["date"]).sum(min_count=1, numeric_only=True).reset_index()

    uk_agg["country"] = "United Kingdom"
    uk_agg["hemisphere"] = "NH"

    cols = uk_agg.columns.to_list()
    cols = cols[-1:] + cols[:-1]
    uk_agg = uk_agg[cols]
    uk_agg = calculate_percent_positive(
        tb=uk_agg, surveillance_cols=["SENTINEL", "NONSENTINEL", "NOTDEFINED", "COMBINED"]
    )

    return pr.concat([tb, uk_agg])
