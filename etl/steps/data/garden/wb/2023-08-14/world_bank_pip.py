"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_data(tb: Table) -> Table:
    # rename columns
    tb = tb.rename(columns={"headcount": "headcount_ratio", "poverty_gap": "poverty_gap_index"})

    # Calculate number in poverty
    tb["headcount"] = tb["headcount_ratio"] * tb["reporting_pop"]
    tb["headcount"] = tb["headcount"].round(0)

    # Calculate shortfall of incomes
    tb["total_shortfall"] = tb["poverty_gap_index"] * tb["poverty_line"] * tb["reporting_pop"]

    # Calculate average shortfall of incomes (averaged across population in poverty)
    tb["avg_shortfall"] = tb["total_shortfall"] / tb["headcount"]

    # Calculate income gap ratio (according to Ravallion's definition)
    tb["income_gap_ratio"] = (tb["total_shortfall"] / tb["headcount"]) / tb["poverty_line"]

    # Same for relative poverty
    for pct in [40, 50, 60]:
        tb[f"headcount_{pct}_median"] = tb[f"headcount_ratio_{pct}_median"] * tb["reporting_pop"]
        tb[f"headcount_{pct}_median"] = tb[f"headcount_{pct}_median"].round(0)
        tb[f"total_shortfall_{pct}_median"] = (
            tb[f"poverty_gap_index_{pct}_median"] * tb["median"] * pct / 100 * tb["reporting_pop"]
        )
        tb[f"avg_shortfall_{pct}_median"] = tb[f"total_shortfall_{pct}_median"] / tb[f"headcount_{pct}_median"]
        tb[f"income_gap_ratio_{pct}_median"] = (tb[f"total_shortfall_{pct}_median"] / tb[f"headcount_{pct}_median"]) / (
            tb["median"] * pct / 100
        )

    # Shares to percentages
    # executing the function over list of vars
    pct_indicators = ["headcount_ratio", "income_gap_ratio", "poverty_gap_index"]
    tb.loc[:, pct_indicators] = tb[pct_indicators] * 100

    # Create a new column for the poverty line in cents and string
    tb["poverty_line_cents"] = (tb["poverty_line"] * 100).astype(int).astype(str)

    # Make the table wide, with poverty_line_cents as columns
    tb = tb.pivot(
        index=[
            "ppp_version",
            "country",
            "year",
            "reporting_level",
            "welfare_type",
            "survey_comparability",
            "comparable_spell",
            "reporting_pop",
            "mean",
            "median",
            "mld",
            "gini",
            "polarization",
            "decile1",
            "decile2",
            "decile3",
            "decile4",
            "decile5",
            "decile6",
            "decile7",
            "decile8",
            "decile9",
            "decile10",
            "is_interpolated",
            "distribution_type",
            "estimation_type",
            "headcount_40_median",
            "headcount_50_median",
            "headcount_60_median",
            "headcount_ratio_40_median",
            "headcount_ratio_50_median",
            "headcount_ratio_60_median",
            "income_gap_ratio_40_median",
            "income_gap_ratio_50_median",
            "income_gap_ratio_60_median",
            "poverty_gap_index_40_median",
            "poverty_gap_index_50_median",
            "poverty_gap_index_60_median",
            "avg_shortfall_40_median",
            "avg_shortfall_50_median",
            "avg_shortfall_60_median",
            "total_shortfall_40_median",
            "total_shortfall_50_median",
            "total_shortfall_60_median",
            "poverty_severity_40_median",
            "poverty_severity_50_median",
            "poverty_severity_60_median",
            "watts_40_median",
            "watts_50_median",
            "watts_60_median",
        ],
        columns="poverty_line_cents",
        values=[
            "headcount",
            "headcount_ratio",
            "income_gap_ratio",
            "poverty_gap_index",
            "avg_shortfall",
            "total_shortfall",
            "poverty_severity",
            "watts",
        ],
    )

    # Flatten column names
    tb.columns = ["_".join(col).strip() for col in tb.columns.values]

    # Reset index
    tb = tb.reset_index()

    # Changing the decile(i) variables for decile(i)_share
    for i in range(1, 11):
        tb = tb.rename(columns={f"decile{i}": f"decile{i}_share"})

    return tb


# NOTE: Create stacked variables


def calculate_inequality(tb: Table) -> Table:
    """
    Calculate inequality measures: decile averages and ratios
    """
    # NOTE: I need the thresholds to complete this function

    col_decile_share = []
    col_decile_avg = []
    col_decile_thr = []

    for i in range(1, 11):
        if i != 10:
            varname_thr = f"decile{i}_thr"
            col_decile_thr.append(varname_thr)

        varname_share = f"decile{i}_share"
        varname_avg = f"decile{i}_avg"
        tb[varname_avg] = tb[varname_share] * tb["mean"] / 0.1

        col_decile_share.append(varname_share)
        col_decile_avg.append(varname_avg)

    # Multiplies decile columns by 100
    tb.loc[:, col_decile_share] = tb[col_decile_share] * 100

    # Palma ratio and other average/share ratios
    tb["palma_ratio"] = tb["decile10_share"] / (
        tb["decile1_share"] + tb["decile2_share"] + tb["decile3_share"] + tb["decile4_share"]
    )
    tb["s80_s20_ratio"] = (tb["decile9_share"] + tb["decile10_share"]) / (tb["decile1_share"] + tb["decile2_share"])
    tb["p90_p10_ratio"] = tb["decile9_thr"] / tb["decile1_thr"]
    tb["p90_p50_ratio"] = tb["decile9_thr"] / tb["decile5_thr"]
    tb["p50_p10_ratio"] = tb["decile5_thr"] / tb["decile1_thr"]


def sanity_check(tb: Table) -> Table:
    # Dropping errors

    print("Dropping rows with issues...")
    start_time = time.time()

    # stacked values not adding up to 100%
    print(f"{len(df_final)} rows before stacked values check")
    df_final["sum_pct"] = df_final[col_stacked_pct].sum(axis=1)
    df_final = df_final[~((df_final["sum_pct"] >= 100.1) | (df_final["sum_pct"] <= 99.9))].reset_index(drop=True)
    print(f"{len(df_final)} rows after stacked values check")

    # missing poverty values (headcount, poverty gap, total shortfall)
    print(f"{len(df_final)} rows before missing values check")
    cols_to_check = (
        col_headcount + col_headcount_ratio + col_povertygap + col_tot_shortfall + col_stacked_n + col_stacked_pct
    )
    df_final = df_final[~df_final[cols_to_check].isna().any(1)].reset_index(drop=True)
    print(f"{len(df_final)} rows after missing values check")

    # headcount monotonicity check
    print(f"{len(df_final)} rows before headcount monotonicity check")
    m_check_vars = []
    for i in range(len(col_headcount)):
        if i > 0:
            check_varname = f"m_check_{i}"
            df_final[check_varname] = df_final[f"{col_headcount[i]}"] >= df_final[f"{col_headcount[i-1]}"]
            m_check_vars.append(check_varname)
    df_final["check_total"] = df_final[m_check_vars].all(1)
    df_final = df_final[df_final["check_total"] == True].reset_index(drop=True)
    print(f"{len(df_final)} rows after headcount monotonicity check")

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("world_bank_pip"))

    # Read table from meadow dataset.
    tb = ds_meadow["world_bank_pip"].reset_index()

    #

    # Process data: Make table wide and change column names
    tb = process_data(tb)

    # Calculate inequality measures
    tb = calculate_inequality(tb)

    #
    # NOTE: Separate income and consumption data.

    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.set_index(["ppp_version", "country", "year", "reporting_level", "welfare_type"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
