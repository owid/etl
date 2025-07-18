"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


BREAK_STR = ".. Not available,  I break in series"


def run() -> None:
    # Load inputs.

    snap_1_1 = paths.load_snapshot("affordable_housing_income.xlsx")
    snap_1_2 = paths.load_snapshot("affordable_housing.xlsx")

    # Load and clean tables from the snapshots.
    tb_soc = snap_1_1.read_excel(
        sheet_name="Figure HC1.1.2", usecols="J:AL", header=2
    )  # housing costs share of consumption
    tb_soc = tb_soc.dropna(subset=["Country"])  # drop empty rows
    tb_soc = pr.melt(
        tb_soc,
        id_vars=["Country"],
        var_name="year",
        value_name="hc_share",
    )

    tb_soc = geo.harmonize_countries(
        tb_soc,
        countries_file=paths.country_mapping_path,
        country_col="Country",
        warn_on_unused_countries=False,
    )

    # housing cost burden split by owner and renter
    tb_hc_b = snap_1_2.read_excel(sheet_name="HC12_A1", header=4)
    tenure_types_b = ["Owner with mortgage", "Rent (private and subsidised)"]
    tb_hc_b = remove_notes(tb_hc_b, index_col="index")
    tb_hc_b = melt_housing_records(tb_hc_b, tenure_types=tenure_types_b, value_col="hc_burden")
    tb_hc_b["quintile"] = "All quintiles"

    # housing cost burden national average
    tb_hc_b_avg = snap_1_2.read_excel(sheet_name="HC12_A1_a", header=4)  #
    tb_hc_b_avg = remove_notes(tb_hc_b_avg, index_col="country")
    tb_hc_b_avg = tb_hc_b_avg.melt(id_vars=["country"], var_name="year", value_name="hc_burden")
    tb_hc_b_avg["quintile"] = "All quintiles"
    tb_hc_b_avg["tenure_type"] = "All tenures"

    # housing cost burden split by quintile
    tb_hc_b_q = snap_1_2.read_excel(sheet_name="HC12_A2", header=[4, 5])
    tb_hc_b_q.columns = [f"{item[0]}_{item[1]}" for item in tb_hc_b_q.columns]
    tb_hc_b_q = remove_notes(tb_hc_b_q)
    # reshape from wide to long format
    tb_hc_b_q = melt_housing_records(tb_hc_b_q, tenure_types=tenure_types_b, value_col="hc_burden")

    # housing cost overburden split by owner and renter, each quintile
    tb_hc_ob_q = snap_1_2.read_excel(sheet_name="HC12_A3", header=[4, 5])
    tb_hc_ob_q.columns = [f"{item[0]}_{item[1]}" for item in tb_hc_ob_q.columns]
    tb_hc_ob_q = remove_notes(tb_hc_ob_q)
    tenure_types_ob = ["Owner with mortgage", "Rent (private)", "Rent (subsidised)"]
    tb_hc_ob_q = melt_housing_records(tb_hc_ob_q, tenure_types=tenure_types_ob, value_col="hc_overburden")

    # housing cost overburden split by owner and renter, all quintiles
    tb_hc_ob = snap_1_2.read_excel(sheet_name="HC12_A3_a", header=4)
    tb_hc_ob = remove_notes(tb_hc_ob, index_col="index")
    tb_hc_ob = melt_housing_records(tb_hc_ob, tenure_types=tenure_types_ob, value_col="hc_overburden")
    tb_hc_ob["quintile"] = "All quintiles"

    # housing cost overburden for low income households (bottom quintile)
    tb_hc_ob_li = snap_1_2.read_excel(sheet_name="HC12_A3_b", header=3)
    tb_hc_ob_li = remove_notes(tb_hc_ob_li, index_col="country")
    tb_hc_ob_li = tb_hc_ob_li.melt(id_vars=["country"], var_name="year", value_name="hc_overburden")
    tb_hc_ob_li["quintile"] = "Bottom quintile"
    tb_hc_ob_li["tenure_type"] = "All tenures"

    # housing cost overburden for all households (average across nation)
    tb_hc_ob_avg = snap_1_2.read_excel(sheet_name="HC12_A3_c", header=3)
    tb_hc_ob_avg = remove_notes(tb_hc_ob_avg, index_col="country")
    tb_hc_ob_avg = tb_hc_ob_avg.melt(id_vars=["country"], var_name="year", value_name="hc_overburden")
    tb_hc_ob_avg["quintile"] = "All quintiles"
    tb_hc_ob_avg["tenure_type"] = "All tenures"

    burden_tables = [
        tb_hc_b,
        tb_hc_b_avg,
        tb_hc_b_q,
        tb_hc_ob_q,
        tb_hc_ob,
        tb_hc_ob_li,
        tb_hc_ob_avg,
    ]

    # Combine all burden and overburden tables
    for tb in burden_tables:
        tb = tb.reset_index()

    tb_b_full = pr.concat(burden_tables, axis=0)

    tb_b_full = geo.harmonize_countries(
        tb_b_full, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    index_cols = ["country", "year", "quintile", "tenure_type"]

    # get all combinations of country, year, quintile, and tenure type
    full_index = tb_b_full[index_cols].drop_duplicates()  # type:ignore
    only_hc_burden = tb_b_full[tb_b_full["hc_burden"].notna()][index_cols + ["hc_burden"]]
    only_hc_overburden = tb_b_full[tb_b_full["hc_overburden"].notna()][index_cols + ["hc_overburden"]]

    # merge with the burden table to ensure all combinations are present
    tb_b = pr.merge(
        full_index,
        only_hc_burden,
        on=["country", "year", "quintile", "tenure_type"],
        how="left",
    )

    tb_b = pr.merge(
        tb_b,
        only_hc_overburden,
        on=["year", "country", "quintile", "tenure_type"],
        how="left",
    )

    # convert into percentage
    tb_b["hc_burden"] = tb_b["hc_burden"] * 100
    tb_b["hc_overburden"] = tb_b["hc_overburden"] * 100

    # format columns
    tb_b = tb_b.replace(
        {"Rent (private and subsidised)": "Rent (private and subsidized)", "Rent (subsidised)": "Rent (subsidized)"}
    )
    tb_b["quintile"] = tb_b["quintile"].str.capitalize()

    tb_b["year"] = tb_b["year"].astype(str)

    # remove data from the share of cost table for Germany in 2020 (limited data collection)
    tb_b = tb_b[~((tb_b["country"] == "Germany") & (tb_b["year"] == "2020"))]

    tb_soc = tb_soc.format(["year", "country"], short_name="housing_costs_share")
    tb_b = tb_b.format(["year", "country", "quintile", "tenure_type"], short_name="housing_costs_burden")

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_soc, tb_b])

    # Save garden dataset.
    ds_garden.save()


def remove_notes(tb, break_str: str = BREAK_STR, index_col: str = "index"):
    tb = tb.rename(columns={tb.columns[0]: index_col})
    tb = tb.dropna(subset=[index_col])
    # remove notes
    # check that the break string is in the index column
    assert (
        tb[index_col].str.contains(break_str).any()
    ), "The break string is not found in the index column, please check the spreadsheet format"
    # find the index of the first occurrence of the break string
    num_rows = tb[tb[index_col] == break_str].index[0]

    # check that after break string, there are no more rows
    assert (
        tb[tb.columns[1:]].iloc[num_rows + 1 :].isna().all().all()
    ), "There are more rows after the break string, please check the spreadsheet format"
    return tb.iloc[:num_rows, :]


def melt_housing_records(tb, tenure_types=[], value_col="value", index_col="index"):
    # check whether tb has MultiIndex columns
    tb = pr.melt(
        tb,
        id_vars=[index_col],
        var_name="year",  # this will be the year and quintile for multi-index columns
        value_name=value_col,
    )  # melt the table to long format
    # check if the year column contains year and quinitile information
    if not str(tb["year"].iloc[0]).isnumeric():
        # split the year column into year and quintile
        tb[["year", "quintile"]] = tb["year"].str.split("_", expand=True)
    # split index into country and tenure type
    tb["tenure_type"] = tb[index_col].apply(lambda x: x if x in tenure_types else pd.NA)
    tb["country"] = tb[index_col].apply(lambda x: pd.NA if x in tenure_types else x)
    # Forward fill country names
    tb["country"] = tb["country"].ffill()
    # drop rows without tenure type
    tb = tb.dropna(subset=["tenure_type"])
    # drop index column
    tb = tb.drop(columns=[index_col])

    return tb
