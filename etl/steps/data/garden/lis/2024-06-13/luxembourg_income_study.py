"""
Load the three LIS meadow datasets and create one garden dataset, `luxembourg_income_study`.

NOTE: To extract the log of the process (to review sanity checks, for example), run the following command in the terminal:
    nohup poetry run etl run luxembourg_income_study > output.log 2>&1 &

"""

from typing import List

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from shared import add_metadata_vars, add_metadata_vars_distribution
from tabulate import tabulate

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Set relative and absolute poverty lines
RELATIVE_POVLINES = [40, 50, 60]

# Set age suffixes
AGE_DICT = {"all": "", "adults": "_adults"}

# Define welfare variables for sanity checks
WELFARE_LIST = ["dhi", "mi", "dhci"]

# Define equivalization
EQUIVALIZATION_LIST = ["eq", "pc"]

# Set table format when printing
TABLEFMT = "pretty"


def run(dest_dir: str) -> None:
    # Create a new garden dataset
    ds_meadow = paths.load_dataset("luxembourg_income_study")

    tables = []
    for age, age_suffix in AGE_DICT.items():
        # Load inputs.

        ######################################################
        # Key variables
        ######################################################

        # Load `keyvars` meadow dataset, rename and drop variables
        tb_keyvars = load_keyvars(age=age_suffix, ds_meadow=ds_meadow)

        # Create additional (relative) poverty variables
        tb_keyvars = create_relative_pov_variables(tb_keyvars=tb_keyvars, relative_povlines=RELATIVE_POVLINES)

        # Make table wide
        tb_keyvars = make_table_wide(tb=tb_keyvars, cols_to_wide=["variable", "eq"])

        # Rename one pop variable created to keep it and drop all the others
        tb_keyvars = tb_keyvars.rename(columns={"pop_dhi_eq": "pop"})
        tb_keyvars = tb_keyvars[tb_keyvars.columns.drop(list(tb_keyvars.filter(like="pop_")))]

        ######################################################
        # Absolute poverty
        ######################################################

        # Load `abs_poverty` meadow dataset, rename variables
        tb_abs_poverty = load_abs_poverty(tb_keyvars=tb_keyvars, age=age_suffix, ds_meadow=ds_meadow)

        # Calculate additional absolute poverty variables
        tb_abs_poverty = create_absolute_pov_variables(tb_abs_poverty=tb_abs_poverty)

        # Make table wide
        tb_abs_poverty = make_table_wide(tb=tb_abs_poverty, cols_to_wide=["variable", "eq", "povline"])

        ######################################################
        # Distributional data
        ######################################################

        # Load `distribution` meadow dataset, rename variables
        tb_distribution = load_distribution(age=age_suffix, ds_meadow=ds_meadow)

        # Calculate income ratios, decile averages and groups of shares
        tb_distribution = create_distributional_variables(
            tb_distribution=tb_distribution, age=age_suffix, ds_meadow=ds_meadow
        )

        # Make table wide
        tb_distribution = make_table_wide(tb=tb_distribution, cols_to_wide=["variable", "eq"])

        # Merge tables
        tb = pr.merge(tb_keyvars, tb_abs_poverty, on=["country", "year"], how="left")
        tb = pr.merge(tb, tb_distribution, on=["country", "year"], how="left")

        # Replace inf values (division by 0)
        tb = tb.replace({np.inf: np.nan})

        # Drop population
        tb = tb.drop(columns=["pop"])

        # Verify index and sort
        tb = tb.format(["country", "year"], short_name=f"luxembourg_income_study{age_suffix}")

        # Add metadata by code
        tb = add_metadata_vars(tb)

        #
        # Save outputs.

        # Add table of processed data to the new dataset.
        tables.append(tb)

    ######################################################
    # Percentile data
    ######################################################

    # Add percentile data
    tables.append(percentiles_table(tb_name="lis_percentiles", ds_meadow=ds_meadow, tb_keyvars=tables[0]))
    tables.append(percentiles_table(tb_name="lis_percentiles_adults", ds_meadow=ds_meadow, tb_keyvars=tables[1]))

    # Add tables to dataset
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    # Sanity checks
    tb_sanity = ds_garden["luxembourg_income_study"].reset_index()
    tb_sanity_adults = ds_garden["luxembourg_income_study_adults"].reset_index()
    sanity_checks(tb_sanity)
    sanity_checks(tb_sanity_adults)
    ds_garden.save()


#########################################################################
# Data processing functions
#########################################################################


# This function makes the table wide and modifies some columns before that
# It is applied to the three LIS datasets
def make_table_wide(tb: Table, cols_to_wide: List[str]) -> Table:
    # Drop dataset variable, to not see it multiplied
    tb = tb.drop(columns=["dataset"])

    # Change names of equivalized variable, to create a distinguishable name
    tb["eq"] = tb["eq"].replace({1: "eq", 0: "pc"})

    # Create pivot table and join different levels of column
    tb = tb.pivot(index=["country", "year"], columns=cols_to_wide, join_column_levels_with="_").reset_index(drop=True)

    return tb


# Load `keyvars` meadow dataset, rename and drop variables
def load_keyvars(age: str, ds_meadow: Dataset) -> Table:
    tb_keyvars = ds_meadow[f"lis_keyvars{age}"].reset_index()

    # Use less technical names for some variables
    tb_keyvars.columns = tb_keyvars.columns.str.replace("fgt0", "headcount_ratio")
    tb_keyvars.columns = tb_keyvars.columns.str.replace("fgt1", "poverty_gap_index")

    # Drop unused poverty variables
    tb_keyvars = tb_keyvars.drop(
        columns=[
            "fgt2_40",
            "fgt2_50",
            "fgt2_60",
            "meanpoor_40",
            "meanpoor_50",
            "meanpoor_60",
            "meangap_40",
            "meangap_50",
            "meangap_60",
        ]
    )

    return tb_keyvars


# Create additional (relative) poverty variables
def create_relative_pov_variables(tb_keyvars: Table, relative_povlines: List[int]) -> Table:
    for povline in relative_povlines:
        # Rename relative poverty variables suffix from 40/50/60 to 40/50/60_median
        tb_keyvars.columns = tb_keyvars.columns.str.replace(f"{povline}", f"{povline}_median")

        # Calculate number in poverty
        tb_keyvars[f"headcount_{povline}_median"] = (
            tb_keyvars[f"headcount_ratio_{povline}_median"] / 100
        ) * tb_keyvars["pop"]
        tb_keyvars[f"headcount_{povline}_median"] = tb_keyvars[f"headcount_{povline}_median"].round(0)

        # Calculate income gap ratio
        tb_keyvars[f"income_gap_ratio_{povline}_median"] = (
            tb_keyvars[f"poverty_gap_index_{povline}_median"] / tb_keyvars[f"headcount_ratio_{povline}_median"]
        )

        # Calculate average shortfall
        tb_keyvars[f"avg_shortfall_{povline}_median"] = (
            tb_keyvars[f"income_gap_ratio_{povline}_median"] * tb_keyvars["median"] * povline / 100
        )

        # Calculate total shortfall
        tb_keyvars[f"total_shortfall_{povline}_median"] = (
            tb_keyvars[f"avg_shortfall_{povline}_median"] * tb_keyvars[f"headcount_{povline}_median"]
        )

        # Make income gap ratio in %
        tb_keyvars[f"income_gap_ratio_{povline}_median"] = tb_keyvars[f"income_gap_ratio_{povline}_median"] * 100

    return tb_keyvars


# Load `abs_poverty` meadow dataset, rename variables
def load_abs_poverty(tb_keyvars: Table, age: str, ds_meadow: Dataset) -> Table:
    tb_abs_poverty = ds_meadow[f"lis_abs_poverty{age}"].reset_index()

    # Add population variable from keyvars
    tb_abs_poverty = pr.merge(
        tb_abs_poverty, tb_keyvars[["country", "year", "pop"]], on=["country", "year"], how="left"
    )

    # Use less technical names for some variables
    tb_abs_poverty.columns = tb_abs_poverty.columns.str.replace("fgt0", "headcount_ratio")
    tb_abs_poverty.columns = tb_abs_poverty.columns.str.replace("fgt1", "poverty_gap_index")

    return tb_abs_poverty


# Calculate additional absolute poverty variables
def create_absolute_pov_variables(tb_abs_poverty: Table) -> Table:
    # Calculate number in poverty
    tb_abs_poverty["headcount"] = tb_abs_poverty["headcount_ratio"] / 100 * tb_abs_poverty["pop"]
    tb_abs_poverty["headcount"] = tb_abs_poverty["headcount"].round(0)

    # Calculate income gap ratio
    tb_abs_poverty["income_gap_ratio"] = tb_abs_poverty["poverty_gap_index"] / tb_abs_poverty["headcount_ratio"]

    # Calculate average shortfall. I need to convert the poverty line to dollars per year (hence the / 100 * 365)
    tb_abs_poverty["avg_shortfall"] = tb_abs_poverty["income_gap_ratio"] * tb_abs_poverty["povline"] / 100 * 365

    # Calculate total shortfall
    tb_abs_poverty["total_shortfall"] = tb_abs_poverty["avg_shortfall"] * tb_abs_poverty["headcount"]

    # Make income gap ratio in %
    tb_abs_poverty["income_gap_ratio"] = tb_abs_poverty["income_gap_ratio"] * 100

    # Make pov lines string to make the pivot table possible
    tb_abs_poverty["povline"] = tb_abs_poverty["povline"].astype(str)

    # Remove population column
    tb_abs_poverty = tb_abs_poverty.drop(columns=["pop"])

    # Also remove unused poverty variables
    tb_abs_poverty = tb_abs_poverty.drop(columns=["fgt2", "meanpoor", "meangap"])

    return tb_abs_poverty


# Load `distribution` meadow dataset, rename variables
def load_distribution(age: str, ds_meadow: Dataset) -> Table:
    tb_distribution = ds_meadow[f"lis_distribution{age}"].reset_index()

    # Transform percentile variable to `pxx`
    tb_distribution["percentile"] = "p" + tb_distribution["percentile"].astype(str)

    # Make pivot table only with percentiles (To estimate ratios more easily)
    tb_distribution = tb_distribution.pivot(
        index=["country", "year", "dataset", "variable", "eq"], columns=["percentile"], join_column_levels_with="_"
    ).reset_index(drop=True)

    return tb_distribution


# Calculate income ratios and decile averages
def create_distributional_variables(tb_distribution: Table, age: str, ds_meadow: Dataset) -> Table:
    # Calculate Palma ratio and other average/share ratios
    tb_distribution["palma_ratio"] = tb_distribution["share_p100"] / (
        tb_distribution["share_p10"]
        + tb_distribution["share_p20"]
        + tb_distribution["share_p30"]
        + tb_distribution["share_p40"]
    )
    tb_distribution["s80_s20_ratio"] = (tb_distribution["share_p100"] + tb_distribution["share_p90"]) / (
        tb_distribution["share_p10"] + tb_distribution["share_p20"]
    )
    tb_distribution["p90_p10_ratio"] = tb_distribution["thr_p90"] / tb_distribution["thr_p10"]
    tb_distribution["p90_p50_ratio"] = tb_distribution["thr_p90"] / tb_distribution["thr_p50"]
    tb_distribution["p50_p10_ratio"] = tb_distribution["thr_p50"] / tb_distribution["thr_p10"]

    # Calculate share of the botton 50%
    tb_distribution["share_bottom50"] = (
        tb_distribution["share_p10"]
        + tb_distribution["share_p20"]
        + tb_distribution["share_p30"]
        + tb_distribution["share_p40"]
        + tb_distribution["share_p50"]
    )

    # Calculate share of the middle 40%
    tb_distribution["share_middle40"] = (
        tb_distribution["share_p60"]
        + tb_distribution["share_p70"]
        + tb_distribution["share_p80"]
        + tb_distribution["share_p90"]
    )

    # Create decile averages
    # Add mean data to dataframe

    # Load keyvars again
    tb_mean = load_keyvars(age=age, ds_meadow=ds_meadow)
    tb_mean = tb_mean[["country", "year", "variable", "eq", "mean"]]

    tb_distribution = pr.merge(tb_distribution, tb_mean, on=["country", "year", "variable", "eq"], how="left")

    for i in range(1, 11):
        perc = i * 10
        tb_distribution[f"avg_p{perc}"] = tb_distribution[f"share_p{perc}"] / 100 * tb_distribution["mean"] / 0.1

    # Drop mean from dataframe
    tb_distribution = tb_distribution.drop(columns=["mean"])

    return tb_distribution


def percentiles_table(tb_name: str, ds_meadow: Dataset, tb_keyvars: Table) -> Table:
    # Read table from meadow dataset.
    tb = ds_meadow[tb_name].reset_index()

    # Drop dataset variable
    tb = tb.drop(columns=["dataset"])

    # Change names of equivalized variable, to create a distinguishable name
    tb["eq"] = tb["eq"].replace({1: "eq", 0: "pc"})

    # Rename variable column to welfare
    tb = tb.rename(columns={"variable": "welfare", "eq": "equivalization"})

    tb_keyvars = tb_keyvars.reset_index()
    tb_keyvars = tb_keyvars[
        ["country", "year", "mean_dhci_eq", "mean_dhci_pc", "mean_dhi_eq", "mean_dhi_pc", "mean_mi_eq", "mean_mi_pc"]
    ]

    # The names of the variables starting on mean contain this structure : main_welfare_equivalization. I want to make the table long and have a column with the welfare and equivalization variables
    tb_keyvars = tb_keyvars.melt(
        id_vars=["country", "year"],
        value_vars=[
            "mean_dhci_eq",
            "mean_dhci_pc",
            "mean_dhi_eq",
            "mean_dhi_pc",
            "mean_mi_eq",
            "mean_mi_pc",
        ],
        var_name="welfare_equivalization",
        value_name="mean",
    )

    # Remove mean_ from the variable name
    tb_keyvars["welfare_equivalization"] = tb_keyvars["welfare_equivalization"].str.replace("mean_", "")

    # Split welfare_equivalization column into two columns, welfare and equivalization
    tb_keyvars[["welfare", "equivalization"]] = tb_keyvars["welfare_equivalization"].str.split("_", expand=True)

    # Drop welfare_equivalization column
    tb_keyvars = tb_keyvars.drop(columns=["welfare_equivalization"])

    # Merge the two tables
    tb = pr.merge(tb, tb_keyvars, on=["country", "year", "welfare", "equivalization"], how="left")

    # Calculate average income by percentile, using mean and share (I don't divide by 100 because share is in % and it cancels out with the % size of the percentile)
    tb["avg"] = tb["mean"] * tb["share"]

    # Drop mean
    tb = tb.drop(columns=["mean"])

    # Set indices and sort.
    tb = tb.format(["country", "year", "welfare", "equivalization", "percentile"])

    # Add metadata by code
    tb = add_metadata_vars_distribution(tb)

    return tb


def sanity_checks(tb: Table) -> Table:
    """
    Perform sanity checks on the data
    """

    tb = tb.copy()

    check_between_0_and_1(tb, variables=["gini"], welfare=WELFARE_LIST)
    check_shares_sum_100(tb, welfare=WELFARE_LIST, margin=0.5)
    check_negative_values(tb)
    check_monotonicity(tb, metric=["avg", "thr", "share"], welfare=WELFARE_LIST)
    check_avg_between_thr(tb, welfare=WELFARE_LIST)

    return tb


def check_between_0_and_1(tb: Table, variables: list, welfare: list):
    """
    Check that indicators are between 0 and 1
    """

    tb = tb.copy()

    for e in EQUIVALIZATION_LIST:
        for v in variables:
            for w in welfare:
                col = f"{v}_{w}_{e}"
                mask = (tb[col] > 1) | (tb[col] < 0)
                any_error = mask.any()

                if any_error:
                    # Filter only values lower than 0 or higher than 1
                    tb_error = tb[mask].copy().reset_index()
                    paths.log.fatal(
                        f"""Values for {col} are not between 0 and 1 in {tb_error.m.short_name}:
                        {_tabulate(tb_error[['country', 'year', col]])}"""
                    )

    return tb


def check_shares_sum_100(tb: Table, welfare: list, margin: float):
    """
    Check if the sum of the variables is 100
    """

    tb = tb.copy()
    # Create a list of variables containing pxpy_share
    variables = [f"share_p{i}" for i in range(10, 110, 10)]
    for e in EQUIVALIZATION_LIST:
        for w in welfare:
            # Set columns to evaluate
            cols = [f"{v}_{w}_{e}" for v in variables]
            # Get sum of shares
            tb["sum_check"] = tb[cols].sum(axis=1)
            # Count the nulls between the 10 decile share variables
            tb["null_check"] = tb[cols].isnull().sum(1)

            mask = (tb["sum_check"] >= 100 + margin) | (tb["sum_check"] <= 100 - margin) & (tb["null_check"] == 0)
            any_error = mask.any()

            if any_error:
                tb_error = tb[mask].reset_index(drop=True).copy()
                paths.log.fatal(
                    f"""{len(tb_error)} share observations ({w}_{e}) are not adding up to 100% in {tb_error.m.short_name}:
                    {_tabulate(tb_error[['country', 'year', 'sum_check']].sort_values(by='sum_check', ascending=False).reset_index(drop=True), floatfmt=".1f")}"""
                )

    return tb


def check_negative_values(tb: Table):
    """
    Check if there are negative values in the variables
    """

    tb = tb.copy()

    # Define columns as all the columns minus country and year and the ones containing "gini"
    variables = [col for col in tb.columns if "gini" not in col and col not in ["country", "year"]]

    for v in variables:
        # Create a mask to check if any value is negative
        mask = tb[v] < 0
        any_error = mask.any()

        if any_error:
            tb_error = tb[mask].reset_index(drop=True).copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {v} are negative in {tb_error.m.short_name}:
                {_tabulate(tb_error[['country', 'year', v]])}"""
            )

    return tb


def check_monotonicity(tb: Table, metric: list, welfare: list):
    """
    Check monotonicity for shares, thresholds and averages
    """

    tb = tb.copy()

    # Create a list of variables containing pxpy_share
    variables = [f"p{i}" for i in range(10, 110, 10)]

    for e in EQUIVALIZATION_LIST:
        for w in welfare:
            for m in metric:
                # Set columns to evaluate
                cols = [f"{m}_{v}_{w}_{e}" for v in variables]

                check_vars = []
                for i in range(len(cols) - 1):
                    # Create a column that checks if the next value is higher than the previous one
                    tb[f"monotonicity_check_{i}"] = tb[cols[i + 1]] >= tb[cols[i]]
                    check_vars.append(f"monotonicity_check_{i}")

                # Create a column that checks if all the previous columns are True
                tb["monotonicity_check"] = tb[check_vars].all(1)

                # Count the nulls between the 10 decile share variables
                tb["null_check"] = tb[cols].isnull().sum(1)

                # Create a mask to check if all the previous columns are True
                mask = (~tb["monotonicity_check"]) & (tb["null_check"] == 0)
                any_error = mask.any()

                if any_error:
                    tb_error = tb[mask].reset_index(drop=True).copy()
                    paths.log.fatal(
                        f"""{len(tb_error)} observations for {m}_{w}_{e} are not monotonically increasing in {tb_error.m.short_name}:
                        {_tabulate(tb_error[['country', 'year'] + cols], floatfmt=".2f")}"""
                    )

    return tb


def check_avg_between_thr(tb: Table, welfare: list) -> Table:
    """
    Check that each avg is between the corresponding thr
    """

    tb = tb.copy()

    for e in EQUIVALIZATION_LIST:
        for w in welfare:
            check_cols = []
            check_nulls = []
            for i in range(10, 110, 10):
                # Create lower bound, avg and upper bound columns
                tb["avg"] = tb[f"avg_p{i}_{w}_{e}"]

                if i == 10:
                    tb["thr_lower"] = 0
                    tb["thr_upper"] = tb[f"thr_p{i}_{w}_{e}"]

                    # Count the nulls between the vars I am checking
                    tb[f"null_check_{i}"] = tb[["thr_lower", "avg", "thr_upper"]].isnull().sum(1)

                    # Create check column
                    tb[f"check_{i}"] = (tb["avg"] >= tb["thr_lower"]) & (tb["avg"] <= tb["thr_upper"])

                elif i < 100:
                    tb["thr_lower"] = tb[f"thr_p{i-10}_{w}_{e}"]
                    tb["thr_upper"] = tb[f"thr_p{i}_{w}_{e}"]

                    # Count the nulls between the vars I am checking
                    tb[f"null_check_{i}"] = tb[["thr_lower", "avg", "thr_upper"]].isnull().sum(1)

                    # Create check column
                    tb[f"check_{i}"] = (tb["avg"] >= tb["thr_lower"]) & (tb["avg"] <= tb["thr_upper"])

                else:
                    tb["thr_lower"] = tb[f"thr_p{i-10}_{w}_{e}"]
                    # Count the nulls between the vars I am checking
                    tb[f"null_check_{i}"] = tb[["thr_lower", "avg"]].isnull().sum(1)

                    # Create check column
                    tb[f"check_{i}"] = tb["avg"] >= tb["thr_lower"]

                check_cols.append(f"check_{i}")
                check_nulls.append(f"null_check_{i}")

            tb["check"] = tb[check_cols].all(1)
            tb["null_check"] = tb[check_nulls].sum(1)

            mask = (~tb["check"]) & (tb["null_check"] == 0)
            any_error = mask.any()

            if any_error:
                tb_error = tb[mask].reset_index(drop=True).copy()
                paths.log.fatal(
                    f"""{len(tb_error)} observations for avg {w}_{e} are not between the corresponding thresholds in {tb_error.m.short_name}:
                    {_tabulate(tb_error[['country', 'year'] + check_cols])}"""
                )

    return tb


def _tabulate(tb: Table, headers="keys", tablefmt=TABLEFMT, **kwargs):
    return tabulate(tb, headers=headers, tablefmt=tablefmt, **kwargs)
