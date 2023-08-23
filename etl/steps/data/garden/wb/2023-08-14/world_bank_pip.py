"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define absolute poverty lines used depending on PPP version
povlines_dict = {
    2011: [100, 190, 320, 550, 1000, 2000, 3000, 4000],
    2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
}


def process_data(tb: Table) -> Table:
    # rename columns
    tb = tb.rename(columns={"headcount": "headcount_ratio", "poverty_gap": "poverty_gap_index"})

    # Changing the decile(i) variables for decile(i)_share
    for i in range(1, 11):
        tb = tb.rename(columns={f"decile{i}": f"decile{i}_share"})

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
            "decile1_share",
            "decile2_share",
            "decile3_share",
            "decile4_share",
            "decile5_share",
            "decile6_share",
            "decile7_share",
            "decile8_share",
            "decile9_share",
            "decile10_share",
            "decile1_thr",
            "decile2_thr",
            "decile3_thr",
            "decile4_thr",
            "decile5_thr",
            "decile6_thr",
            "decile7_thr",
            "decile8_thr",
            "decile9_thr",
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

    return tb


def create_stacked_variables(tb: Table, povline_dict: dict, ppp_version: int) -> tuple([Table, list, list]):
    """
    Create stacked variables from the indicators to plot them as stacked area/bar charts
    """

    # Select poverty lines between 2011 and 2017 and sort in case they are not in order
    povlines = povlines_dict[ppp_version].sort()

    # Above variables

    col_above_n = []
    col_above_pct = []

    for p in povlines:
        varname_n = f"headcount_above_{p}"
        varname_pct = f"headcount_ratio_above_{p}"

        tb[varname_n] = tb["reporting_pop"] - tb[f"headcount_{p}"]
        tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]

        col_above_n.append(varname_n)
        col_above_pct.append(varname_pct)

    tb.loc[:, col_above_pct] = tb[col_above_pct] * 100

    # Stacked variables

    col_stacked_n = []
    col_stacked_pct = []

    for i in range(len(povlines)):
        # if it's the first value only get people below this poverty line (and percentage)
        if i == 0:
            varname_n = f"headcount_stacked_below_{povlines[i]}"
            varname_pct = f"headcount_ratio_stacked_below_{povlines[i]}"
            tb[varname_n] = tb[f"headcount_{povlines[i]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)

        # If it's the last value calculate the people between this value and the previous
        # and also the people over this poverty line (and percentages)
        elif i == len(povlines) - 1:
            varname_n = f"headcount_stacked_below_{povlines[i]}"
            varname_pct = f"headcount_ratio_stacked_below_{povlines[i]}"
            tb[varname_n] = tb[f"headcount_{povlines[i]}"] - tb[f"headcount_{povlines[i-1]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)
            varname_n = f"headcount_stacked_above_{povlines[i]}"
            varname_pct = f"headcount_ratio_stacked_above_{povlines[i]}"
            tb[varname_n] = tb["reporting_pop"] - tb[f"headcount_{povlines[i]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)

        # If it's any value between the first and the last calculate the people between this value and the previous (and percentage)
        else:
            varname_n = f"headcount_stacked_below_{povlines[i]}"
            varname_pct = f"headcount_ratio_stacked_below_{povlines[i]}"
            tb[varname_n] = tb[f"headcount_{povlines[i]}"] - tb[f"headcount_{povlines[i-1]}"]
            tb[varname_pct] = tb[varname_n] / tb["reporting_pop"]
            col_stacked_n.append(varname_n)
            col_stacked_pct.append(varname_pct)

    tb.loc[:, col_stacked_pct] = tb[col_stacked_pct] * 100

    # Calculate stacked variables which "jump" the original order

    tb[f"headcount_stacked_between_{povlines[1]}_{povlines[4]}"] = (
        tb[f"headcount_{povlines[4]}"] - tb[f"headcount_{povlines[1]}"]
    )
    tb[f"headcount_stacked_between_{povlines[4]}_{povlines[6]}"] = (
        tb[f"headcount_{povlines[6]}"] - tb[f"headcount_{povlines[4]}"]
    )

    tb[f"headcount_ratio_stacked_between_{povlines[1]}_{povlines[4]}"] = (
        tb[f"headcount_ratio_{povlines[4]}"] - tb[f"headcount_ratio_{povlines[1]}"]
    )
    tb[f"headcount_ratio_stacked_between_{povlines[4]}_{povlines[6]}"] = (
        tb[f"headcount_ratio_{povlines[6]}"] - tb[f"headcount_ratio_{povlines[4]}"]
    )

    return tb, col_stacked_n, col_stacked_pct


def calculate_inequality(tb: Table) -> Table:
    """
    Calculate inequality measures: decile averages and ratios
    """

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


def identify_rural_urban(tb: Table) -> Table:
    """
    Amend the entity to reflect if data refers to urban or rural only
    """

    tb.loc[(tb["reporting_level"].isin(["urban", "rural"])), "country"] = (
        tb.loc[(tb["reporting_level"].isin(["urban", "rural"])), "country"]
        + " ("
        + tb.loc[(tb["reporting_level"].isin(["urban", "rural"])), "reporting_level"]
        + ")"
    )

    return tb


def sanity_checks(
    tb: Table, povlines_dict: dict, ppp_version: int, col_stacked_n: list, col_stacked_pct: list
) -> Table:
    """
    Sanity checks for the table
    """

    # Select poverty lines between 2011 and 2017 and sort in case they are not in order
    povlines = povlines_dict[ppp_version].sort()

    obs_before_checks = len(tb)

    # Create lists of variables to check
    col_headcount = []
    col_headcount_ratio = []
    col_povertygap = []
    col_tot_shortfall = []
    col_watts = []
    col_poverty_severity = []
    col_decile_share = []
    col_decile_thr = []

    for p in povlines:
        col_headcount.append(f"headcount_{p}")
        col_headcount_ratio.append(f"headcount_ratio_{p}")
        col_povertygap.append(f"poverty_gap_index_{p}")
        col_tot_shortfall.append(f"total_shortfall_{p}")
        col_watts.append(f"watts_{p}")
        col_poverty_severity.append(f"poverty_severity_{p}")

    for i in range(1, 11):
        col_decile_share.append(f"decile{i}_share")
        if i != 10:
            col_decile_thr.append(f"decile{i}_thr")

    # Negative values
    mask = (
        tb[
            col_headcount
            + col_headcount_ratio
            + col_povertygap
            + col_tot_shortfall
            + col_watts
            + col_poverty_severity
            + col_decile_share
            + col_decile_thr
            + ["mean", "median", "mld", "gini", "polarization"]
        ]
        .lt(0)
        .any(1)
    )
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.fatal(
            f"""There are {len(tb_error)} observations with negative values!
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )
        # NOTE: Check if we want to delete these observations
        # tb = tb[~mask].reset_index(drop=True)

    # stacked values not adding up to 100%
    tb["sum_pct"] = tb[col_stacked_pct].sum(axis=1)
    mask = (tb["sum_pct"] >= 100.1) | (tb["sum_pct"] <= 99.9)
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""{len(tb_error)} observations of stacked values are not adding up to 100% and will be deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type', 'sum_pct']]}"""
        )
        tb = tb[~mask].reset_index(drop=True)

    # missing poverty values (headcount, poverty gap, total shortfall)
    cols_to_check = (
        col_headcount + col_headcount_ratio + col_povertygap + col_tot_shortfall + col_stacked_n + col_stacked_pct
    )
    mask = tb[cols_to_check].isna().any(1)
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with missing poverty values and will be deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )
        tb = tb[~mask].reset_index(drop=True)

    # Missing median
    mask = tb["median"].isna()
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with missing median. They will be not deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )

    # Missing mean
    mask = tb["mean"].isna()
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with missing mean. They will be not deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )

    # Missing gini
    mask = tb["gini"].isna()
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with missing gini. They will be not deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )

    # Missing decile shares
    mask = tb[col_decile_share].isna().any(1)
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with missing decile shares. They will be not deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )

    # Missing decile thresholds
    mask = tb[col_decile_thr].isna().any(1)
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with missing decile thresholds. They will be not deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )

    # headcount monotonicity check
    m_check_vars = []
    for i in range(len(col_headcount)):
        if i > 0:
            check_varname = f"m_check_{i}"
            tb[check_varname] = tb[f"{col_headcount[i]}"] >= tb[f"{col_headcount[i-1]}"]
            m_check_vars.append(check_varname)
    tb["check_total"] = tb[m_check_vars].all(1)

    tb_error = tb[~tb["check_total"]].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with headcount not monotonically increasing and will be deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )
        tb = tb[tb["check_total"]].reset_index(drop=True)

    # Threshold monotonicity check
    m_check_vars = []
    for i in range(1, 10):
        if i > 1:
            check_varname = f"m_check_{i}"
            tb[check_varname] = tb[f"decile{i}_thr"] >= tb[f"decile{i-1}_thr"]
            m_check_vars.append(check_varname)

    tb["check_total"] = tb[m_check_vars].all(1)

    tb_error = tb[~tb["check_total"]].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with thresholds not monotonically increasing and will be deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )
        tb = tb[tb["check_total"]].reset_index(drop=True)

    # Shares monotonicity check
    m_check_vars = []
    for i in range(1, 11):
        if i > 1:
            check_varname = f"m_check_{i}"
            tb[check_varname] = tb[f"decile{i}_share"] >= tb[f"decile{i-1}_share"]
            m_check_vars.append(check_varname)

    tb["check_total"] = tb[m_check_vars].all(1)

    tb_error = tb[~tb["check_total"]].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""There are {len(tb_error)} observations with shares not monotonically increasing and will be deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type']]}"""
        )
        tb = tb[tb["check_total"]].reset_index(drop=True)

    # Shares not adding up to 100%
    tb["sum_pct"] = tb[col_decile_share].sum(axis=1)
    mask = (tb["sum_pct"] >= 100.1) | (tb["sum_pct"] <= 99.9)
    tb_error = tb[mask].reset_index(drop=True)

    if len(tb_error) > 0:
        log.warning(
            f"""{len(tb_error)} observations of shares are not adding up to 100% and will be deleted:
            {tb_error[['country', 'year', 'reporting_level', 'welfare_type', 'sum_pct']]}"""
        )
        tb = tb[~mask].reset_index(drop=True)

    # delete columns created for the checks
    tb = tb.drop(columns=m_check_vars + ["check_total", "sum_pct"])

    obs_after_checks = len(tb)
    log.info(f"Sanity checks deleted {obs_before_checks - obs_after_checks} observations.")

    return tb


def separate_ppp_data(tb: Table) -> tuple([Table, Table]):
    """
    Separate out ppp data from the main dataset
    """

    # Filter table to include only the right ppp_version
    # Also, drop columns with all NaNs (which are the ones that are not relevant for the ppp_version)
    tb_2011 = tb[tb["ppp_version"] == 2011].reset_index(drop=True).dropna(axis=1, how="all", ignore_index=True)
    tb_2017 = tb[tb["ppp_version"] == 2017].reset_index(drop=True).dropna(axis=1, how="all", ignore_index=True)

    return tb_2011, tb_2017


def separate_filled_data(tb: Table) -> Table:
    """
    Separate out filled data from the main dataset
    """
    # Regions are not marked as interpolated: we keep them in the dataset anyway, by including nulls

    tb = tb[(tb["is_interpolated"] == 0) | (tb["is_interpolated"].isnull())].reset_index(drop=True)

    return tb


def inc_or_cons_data(tb: Table) -> tuple([Table, Table, Table]):
    """
    Separate income and consumption data
    """

    # Separate out consumption-only, income-only. Also, create a table with both income and consumption
    tb_inc = tb[tb["welfare_type"] == "income"].reset_index(drop=True)
    tb_cons = tb[tb["welfare_type"] == "consumption"].reset_index(drop=True)
    tb_inc_or_cons = tb.copy()

    # If both inc and cons are available in a given year, drop inc

    # Flag duplicates – indicating multiple welfare_types
    # Sort values to ensure the welfare_type consumption is marked as False when there are multiple welfare types
    tb_inc_or_cons = tb_inc_or_cons.sort_values(
        by=["ppp_version", "country", "year", "reporting_level", "welfare_type"], ignore_index=True
    )
    tb_inc_or_cons["duplicate_flag"] = tb_inc_or_cons.duplicated(
        subset=["ppp_version", "country", "year", "reporting_level"]
    )

    # Drop income where income and consumption are available
    tb_inc_or_cons = tb_inc_or_cons[
        (~tb_inc_or_cons["duplicate_flag"]) | (tb_inc_or_cons["welfare_type"] == "consumption")
    ]
    tb_inc_or_cons.drop(columns=["duplicate_flag"], inplace=True)

    # print(f'After dropping duplicates there were {len(tb_inc_or_cons)} rows.')

    return tb_inc, tb_cons, tb_inc_or_cons


def regional_headcount(tb: Table) -> Table:
    """
    Create regional headcount dataset, by patching missing values with the difference between world and regional headcount
    """

    # Keep only regional data: for regions, reporting_level is null
    tb_regions = tb[tb["reporting_level"].isnull()].reset_index(drop=True)

    tb_regions = tb_regions[["country", "year", "headcount_215"]]
    tb_regions = tb_regions.pivot(index="year", columns="country", values="headcount_215")

    # Drop rows with more than one region with null headcount
    cols_to_check = [e for e in list(tb_regions.columns) if e not in ["year"]]
    tb_regions["check_total"] = tb_regions[cols_to_check].isnull().sum(1)
    tb_regions = tb_regions[tb_regions["check_total"] <= 1].reset_index()
    tb_regions = tb_regions.drop(columns="check_total")
    print(f"{len(tb_regions)} rows after missing values check")

    # Get difference between world and (total) regional headcount, to patch rows with one missing value
    cols_to_sum = [e for e in list(df_regions.columns) if e not in ["Year", "World"]]
    df_regions["incomplete_sum"] = df_regions[cols_to_sum].sum(1)
    df_regions["difference_for_missing"] = df_regions["World"] - df_regions["incomplete_sum"]

    # Fill null values with the difference and drop aux variables
    col_dictionary = dict.fromkeys(cols_to_sum, df_regions["difference_for_missing"])
    df_regions.loc[:, cols_to_sum] = df_regions[cols_to_sum].fillna(col_dictionary)
    df_regions = df_regions.drop(columns=["World", "incomplete_sum", "difference_for_missing"])

    # Get headcount values for China and India
    df_chn_ind = df_country_filled[
        (df_country_filled["Entity"].isin(["China", "India"])) & (df_country_filled["reporting_level"] == "national")
    ].reset_index(drop=True)

    df_chn_ind["number_extreme_poverty"] = df_chn_ind["headcount"] * df_chn_ind["reporting_pop"]
    df_chn_ind["number_extreme_poverty"] = df_chn_ind["number_extreme_poverty"].round(0)

    df_chn_ind = df_chn_ind[["Entity", "Year", "number_extreme_poverty"]]

    # Make table wide and merge with regional data
    df_chn_ind = df_chn_ind.pivot(index="Year", columns="Entity", values="number_extreme_poverty")

    df_final = pd.merge(df_regions, df_chn_ind, on="Year", how="left")

    df_final["East Asia and Pacific excluding China"] = df_final["East Asia and Pacific"] - df_final["China"]
    df_final["South Asia excluding India"] = df_final["South Asia"] - df_final["India"]

    df_final = pd.melt(df_final, id_vars=["Year"], value_name="number_extreme_poverty")
    df_final = df_final[["Entity", "Year", "number_extreme_poverty"]]

    df_final = df_final.rename(
        columns={"number_extreme_poverty": "Number of people living in extreme poverty - by world region"}
    )

    # Export the dataset
    df_final.to_csv(f"data/ppp_{ppp}/final/OWID_internal_upload/admin_database/pip_regional_headcount.csv", index=False)

    # upload_to_s3(df_final, 'PIP/datasets', f'pip_regional_headcount.csv')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Done. Execution time:", elapsed_time, "seconds")


def survey_count(df_country, ppp):
    print("Creating dataset which counts the surveys in the recent decade...")
    start_time = time.time()

    df_country = standardise(df_country, ppp)

    # Generate a new dataset to count the surveys available for each entity
    # Create a list of all the years and entities available

    min_year = df_country["Year"].min()
    max_year = df_country["Year"].max()
    year_list = list(range(min_year, max_year + 1))
    entity_list = list(df_country["Entity"].unique())

    # Create two dataframes with all the years and entities
    year_df = pd.DataFrame(year_list)
    entity_df = pd.DataFrame(entity_list)

    # Make a cartesian product of both dataframes: join all the combinations between all the entities and all the years
    cross = pd.merge(entity_df, year_df, how="cross")
    cross = cross.rename(columns={"0_x": "Entity", "0_y": "Year"})

    # Merge cross and df_country, to include all the possible rows in the dataset
    df_country = pd.merge(
        cross, df_country[["Entity", "Year", "reporting_level"]], on=["Entity", "Year"], how="left", indicator=True
    )

    # Mark with 1 if there are surveys available, 0 if not
    df_country["survey_available"] = np.where(df_country["_merge"] == "both", 1, 0)

    # Sum for each entity the surveys available for the previous 9 years and the current year
    df_country["surveys_past_decade"] = (
        df_country["survey_available"]
        .groupby(df_country["Entity"], sort=False)
        .rolling(min_periods=1, window=10)
        .sum()
        .astype(int)
        .values
    )
    df_country = df_country[["Entity", "Year", "surveys_past_decade"]]
    df_country = df_country.rename(columns={"surveys_past_decade": "Number of surveys in the past decade"})

    df_country.sort_values(by=["Entity", "Year"], ignore_index=True, inplace=True)

    # Export the dataset
    df_country.to_csv(f"data/ppp_{ppp}/final/OWID_internal_upload/admin_database/pip_survey_count.csv", index=False)

    # upload_to_s3(df_country, 'PIP/datasets', f'pip_survey_count.csv')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Done. Execution time:", elapsed_time, "seconds")


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("world_bank_pip"))

    # Read table from meadow dataset.
    tb = ds_meadow["world_bank_pip"].reset_index()

    # Process data
    # Make table wide and change column names
    tb = process_data(tb)

    # Calculate inequality measures
    tb = calculate_inequality(tb)

    # Harmonize country names
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Amend the entity to reflect if data refers to urban or rural only
    tb = identify_rural_urban(tb)

    # Separate out ppp and filled data from the main dataset
    tb_2011, tb_2017 = separate_ppp_data(tb)

    # Create stacked variables from headcount and headcount_ratio
    tb_2011, col_stacked_n_2011, col_stacked_pct_2011 = create_stacked_variables(
        tb_2011, povlines_dict, ppp_version=2011
    )
    tb_2017, col_stacked_n_2017, col_stacked_pct_2017 = create_stacked_variables(
        tb_2017, povlines_dict, ppp_version=2017
    )

    # Sanity checks
    tb_2011 = sanity_checks(
        tb_2011, povlines_dict, ppp_version=2011, col_stacked_n=col_stacked_n_2011, col_stacked_pct=col_stacked_pct_2011
    )
    tb_2017 = sanity_checks(
        tb_2017, povlines_dict, ppp_version=2017, col_stacked_n=col_stacked_n_2017, col_stacked_pct=col_stacked_pct_2017
    )

    # Separate out filled data from the main dataset
    tb_2011_non_filled = separate_filled_data(tb_2011)
    tb_2017_non_filled = separate_filled_data(tb_2017)

    # Separate out consumption-only, income-only. Also, create a table with both income and consumption
    tb_inc_2011, tb_cons_2011, tb_inc_or_cons_2011 = inc_or_cons_data(tb_2011)
    tb_inc_2017, tb_cons_2017, tb_inc_or_cons_2017 = inc_or_cons_data(tb_2017)
    tb_inc_2011_non_filled, tb_cons_2011_non_filled, tb_inc_or_cons_2011_non_filled = inc_or_cons_data(
        tb_2011_non_filled
    )
    tb_inc_2017_non_filled, tb_cons_2017_non_filled, tb_inc_or_cons_2017_non_filled = inc_or_cons_data(
        tb_2017_non_filled
    )

    # Define tables to upload
    tables = [
        tb_inc_2011,
        tb_cons_2011,
        tb_inc_or_cons_2011,
        tb_inc_2017,
        tb_cons_2017,
        tb_inc_or_cons_2017,
        tb_2011_non_filled,
        tb_2017_non_filled,
        tb_inc_2011_non_filled,
        tb_cons_2011_non_filled,
        tb_inc_or_cons_2011_non_filled,
        tb_inc_2017_non_filled,
        tb_cons_2017_non_filled,
        tb_inc_or_cons_2017_non_filled,
    ]

    # Set index and sort
    for tb in tables:
        tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tables], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
