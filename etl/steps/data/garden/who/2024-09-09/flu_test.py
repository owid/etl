"""Load a garden dataset and create a grapher dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("flunet")
    # Read table from garden dataset.
    tb = ds_garden["flunet"]
    # Harmonize countries
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb["date"] = pd.to_datetime(tb["iso_weekstartdate"], format="%Y-%m-%d", utc=True).dt.date.astype(str)
    tb = clean_and_format_data(tb)
    tb = split_by_surveillance_type(tb)

    tb = calculate_percent_positive(tb, surveillance_cols=["SENTINEL", "NONSENTINEL", "NOTDEFINED", "COMBINED"])

    # Format date

    # Select out only variables that we care about
    # tb_test = (
    #    tb[["country", "date", "origin_source", "spec_processed_nb", "spec_received_nb", "inf_all", "inf_negative"]]
    #    .dropna(subset=["spec_processed_nb", "spec_received_nb"])
    #    .copy()
    # )
    # tb_test["inf_tests"] = tb_test["inf_all"] + tb_test["inf_negative"]
    # tb_test = tb_test.drop(columns=["inf_all", "inf_negative"])
    tb = tb[["country", "date", "pcnt_posCOMBINED", "denomCOMBINED"]]
    tb = tb.dropna(subset=["denomCOMBINED", "pcnt_posCOMBINED"], how="any")

    tb = tb.format(["country", "date"], short_name="flu_test")
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def remove_nan_dates(tb: Table) -> Table:
    """There are rare mistakes in data with invalid date format."""
    ix = tb.date.isnull()
    if ix.any():
        log.warning(f"Removing {ix.sum()} rows with invalid date format")
    tb = tb[~ix]
    # tb["date"] = tb["date"].dt.date.astype(str)
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


def clean_and_format_data(tb: Table) -> Table:
    """
    Clean data by:
    * Converting date to date format
    * Combining subtype columns together
    * Drop unused columns
    """

    tb = remove_nan_dates(tb)
    tb = remove_rows_that_sum_incorrectly(tb)
    tb = combine_columns(tb)
    sel_cols = [
        "country",
        "hemisphere",
        "date",
        "origin_source",
        "inf_all",
        "inf_negative",
        "spec_processed_nb",
        "spec_received_nb",
    ]
    tb = tb[sel_cols]
    tb = tb.drop_duplicates()

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


def calculate_percent_positive(tb: Table, surveillance_cols: list[str]) -> Table:
    """
    Sometimes the 0s in the inf_negative* columns should in fact be NA. Here we convert rows where:
    inf_negative = 0 and the sum of the positive and negative tests does not equal the number of processed tests.

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
        # Identifying rows where the inf negative value is 0, but is likely actually NA
        tb.loc[
            (tb["inf_negative" + col] == 0)
            & (tb["inf_negative" + col] + tb["inf_all" + col] != tb["spec_processed_nb" + col]),
            "inf_negative" + col,
        ] = np.nan
        # Calculating the denominator separately
        tb["denom_1" + col] = tb["inf_all" + col] + tb["inf_negative" + col]
        tb["denom_2" + col] = tb["spec_processed_nb" + col]
        tb["denom_3" + col] = tb["spec_received_nb" + col]

        tb["pcnt_pos_1" + col] = (tb["inf_all" + col] / (tb["denom_1" + col])) * 100
        tb["pcnt_pos_2" + col] = (tb["inf_all" + col] / (tb["denom_2" + col])) * 100
        tb["pcnt_pos_3" + col] = (tb["inf_all" + col] / (tb["denom_3" + col])) * 100

        # hierachically fill the 'pcnt_pos' column with values from the columns described above in order of preference: 1->2->3
        tb["pcnt_pos" + col] = tb["pcnt_pos_1" + col]
        tb["denom" + col] = tb["denom_1" + col]

        idx_2 = tb["pcnt_pos" + col].isna()
        tb.loc[idx_2, "pcnt_pos" + col] = tb["pcnt_pos_2" + col]
        tb.loc[idx_2, "denom" + col] = tb["denom_2" + col]

        idx_3 = tb["pcnt_pos" + col].isna()
        tb.loc[idx_3, "pcnt_pos" + col] = tb["pcnt_pos_2" + col]
        tb.loc[idx_3, "denom" + col] = tb["denom_2" + col]

        tb = tb.drop(columns=["pcnt_pos_1" + col, "pcnt_pos_2" + col, "pcnt_pos_3" + col])
        tb = tb.drop(columns=["denom_1" + col, "denom_2" + col, "denom_3" + col])

        # Replace inf with NAs
        tb["pcnt_pos" + col] = tb["pcnt_pos" + col].replace([np.inf, -np.inf], np.nan)
        tb["denom" + col] = tb["denom" + col].replace([np.inf, -np.inf], np.nan)

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
