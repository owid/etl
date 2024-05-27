"""
Load World Inequality Database meadow dataset and create a garden dataset.

NOTE: To extract the log of the process (to review sanity checks, for example), run the following command in the terminal:
    nohup poetry run etl run world_inequality_database > output.log 2>&1 &

"""


import owid.catalog.processing as pr
from owid.catalog import Table
from shared import add_metadata_vars, add_metadata_vars_distribution
from structlog import get_logger
from tabulate import tabulate

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define combinations of variables to calculate relative poverty
EXTRAPOLATED_DICT = {"no": "", "yes": "_extrapolated"}
WELFARE_VARS = ["pretax", "posttax_nat", "posttax_dis", "wealth"]

# Set table format when printing
TABLEFMT = "pretty"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_inequality_database")

    # Read tables from meadow dataset.
    tb = ds_meadow["world_inequality_database"].reset_index()
    tb_percentiles = ds_meadow["world_inequality_database_distribution"].reset_index()
    tb_fiscal = ds_meadow["world_inequality_database_fiscal"].reset_index()

    #
    # Process data.
    # Change units and drop unnecessary columns
    tb = drop_columns_and_transform(tb)

    # Sanity checks
    sanity_checks(tb)

    ########################################
    # Percentile data
    ########################################

    # Process data.
    # Multiple share and share_extrapolated columns by 100
    tb_percentiles[["share", "share_extrapolated"]] *= 100

    # Multiply columns containing share in tb_fiscal by 100
    tb_fiscal[list(tb_fiscal.filter(like="share"))] *= 100

    # Add relative poverty values
    tb_relative_poverty = add_relative_poverty(tb, tb_percentiles, EXTRAPOLATED_DICT, WELFARE_VARS)

    # Merge tables
    tb = pr.merge(tb, tb_relative_poverty, on=["country", "year"], how="left")

    # Add metadata by code (key indicators)
    tb = add_metadata_vars(tb)

    # Add metadata by code (distributions)
    tb_percentiles = add_metadata_vars_distribution(tb_percentiles)

    # Set index and sort
    tb = tb.format()
    tb_percentiles = tb_percentiles.format(["country", "year", "welfare", "p", "percentile"])
    tb_fiscal = tb_fiscal.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset and add the garden table.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb, tb_percentiles, tb_fiscal],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


# Data processing function (cleaning and small transformations)
def drop_columns_and_transform(tb: Table) -> Table:
    """
    Drop columns and transform shares by multiplying by 100
    """
    # Multiply shares by 100
    tb[list(tb.filter(like="share"))] *= 100

    # Delete age and pop, two one-value variables
    tb = tb.drop(columns=["age", "pop", "age_extrapolated", "pop_extrapolated"])

    # Delete some share ratios we are not using, and also the p0p40 (share) variable only available for pretax
    drop_list = ["s90_s10_ratio", "s90_s50_ratio", "p0p40"]

    for var in drop_list:
        tb = tb[tb.columns.drop(list(tb.filter(like=var)))]

    return tb


def add_relative_poverty(tb: Table, tb_percentiles: Table, extrapolated_dict: dict, welfare_vars: list) -> Table:
    """
    Add relative poverty values by estimating the median and checking that value against the percentile distribution
    """

    # Make copies of the tables
    tb = tb.copy()
    tb_percentiles = tb_percentiles.copy()

    # Make tb_percentiles wide, by creating a column for each welfare
    tb_percentiles = tb_percentiles.pivot(
        index=["country", "year", "p", "percentile"], columns="welfare", values=["thr", "thr_extrapolated"]
    )

    # Flatten column names
    tb_percentiles.columns = ["_".join(col).strip() for col in tb_percentiles.columns.values]
    tb_percentiles = tb_percentiles.reset_index()

    # Calculate 40, 50, and 60 percent of the median for each country and year
    for var in welfare_vars:
        for yn, extrapolated in extrapolated_dict.items():
            for pct in [40, 50, 60]:
                tb[f"median{pct}pct_{var}{extrapolated}"] = tb[f"median_{var}{extrapolated}"] * pct / 100

    # Merge the two tables
    tb_percentiles = pr.merge(tb_percentiles, tb, on=["country", "year"], how="left")

    # Calculate absolute difference between thresholds and percentage of median
    for var in welfare_vars:
        for yn, extrapolated in extrapolated_dict.items():
            for pct in [40, 50, 60]:
                tb_percentiles[f"abs_diff{pct}pct_{var}{extrapolated}"] = abs(
                    tb_percentiles[f"thr{extrapolated}_{var}"] - tb_percentiles[f"median{pct}pct_{var}{extrapolated}"]
                )

    # For each country and year, find the percentile with the minimum absolute difference for each welafre, extrapolation and pct
    tb_relative_poverty = Table()
    for var in welfare_vars:
        for yn, extrapolated in extrapolated_dict.items():
            for pct in [40, 50, 60]:
                tb_percentiles[f"min{pct}pct_{var}{extrapolated}"] = tb_percentiles.groupby(["country", "year"])[
                    f"abs_diff{pct}pct_{var}{extrapolated}"
                ].transform("min")

                # Create a table with the minimum absolute difference for each country and year
                tb_min = tb_percentiles[
                    tb_percentiles[f"abs_diff{pct}pct_{var}{extrapolated}"]
                    == tb_percentiles[f"min{pct}pct_{var}{extrapolated}"]
                ]
                # The result generates multiple values for some countries and years, so we need to drop duplicates
                tb_min = tb_min.drop_duplicates(subset=["country", "year"], keep="last")

                # Select only what is needed
                tb_min = tb_min[["country", "year", "p"]]
                # Multiply by 100 to get the headcount ratio in percentage and rename
                tb_min["p"] *= 100
                tb_min = tb_min.rename(columns={"p": f"headcount_ratio_{pct}_median_{var}{extrapolated}"})

                # Merge this table with tb_relative_poverty
                if tb_relative_poverty.empty:
                    tb_relative_poverty = tb_min
                else:
                    tb_relative_poverty = pr.merge(tb_relative_poverty, tb_min, on=["country", "year"], how="outer")

    return tb_relative_poverty


def sanity_checks(tb: Table) -> Table:
    """
    Perform sanity checks on the data
    """

    tb = tb.copy()

    check_between_0_and_1(tb, variables=["p0p100_gini"], welfare=WELFARE_VARS)
    check_shares_sum_100(tb, welfare=WELFARE_VARS, margin=0.5)
    check_negative_values(tb)
    check_monotonicity(tb, metric=["avg", "thr", "share"], welfare=WELFARE_VARS)
    check_avg_between_thr(tb, welfare=WELFARE_VARS)

    return tb


def check_between_0_and_1(tb: Table, variables: list, welfare: list):
    """
    Check that indicators are between 0 and 1
    """

    tb = tb.copy()

    for e in EXTRAPOLATED_DICT:
        for v in variables:
            for w in welfare:
                # Filter only values lower than 0 or higher than 1
                col = f"{v}_{w}{EXTRAPOLATED_DICT[e]}"
                mask = (tb[col] > 1) | (tb[col] < 0)
                tb_error = tb[mask].copy().reset_index()

                if not tb_error.empty and w != "wealth":
                    log.fatal(
                        f"""Values for {col} are not between 0 and 1:
                        {tabulate(tb_error[['country', 'year', col]], headers = 'keys', tablefmt = TABLEFMT)}"""
                    )

                elif not tb_error.empty and w == "wealth":
                    log.warning(
                        f"""Values for {col} are not between 0 and 1:
                        {tabulate(tb_error[['country', 'year', col]], headers = 'keys', tablefmt = TABLEFMT)}"""
                    )

    return tb


def check_shares_sum_100(tb: Table, welfare: list, margin: float):
    """
    Check if the sum of the variables is 100
    """

    tb = tb.copy()
    # Create a list of variables containing pxpy_share
    variables = [f"p{i}p{i+10}_share" for i in range(0, 100, 10)]
    for e in EXTRAPOLATED_DICT:
        for w in welfare:
            # Set columns to evaluate
            cols = [f"{v}_{w}{EXTRAPOLATED_DICT[e]}" for v in variables]
            # Get sum of shares
            tb["sum_check"] = tb[cols].sum(axis=1)
            # Count the nulls between the 10 decile share variables
            tb["null_check"] = tb[cols].isnull().sum(1)

            mask = (tb["sum_check"] >= 100 + margin) | (tb["sum_check"] <= 100 - margin) & (tb["null_check"] == 0)
            tb_error = tb[mask].reset_index(drop=True).copy()

            if not tb_error.empty:
                log.fatal(
                    f"""{len(tb_error)} share observations ({w}{EXTRAPOLATED_DICT[e]}) are not adding up to 100%:
                    {tabulate(tb_error[['country', 'year', 'sum_check']].sort_values(by='sum_check', ascending=False).reset_index(drop=True), headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )

    return tb


def check_negative_values(tb: Table):
    """
    Check if there are negative values in the variables
    """

    tb = tb.copy()

    # Define columns as all the columns minus country and year, the ones containing "share" and the ones containing "gini"
    variables = [
        col for col in tb.columns if "gini" not in col and "wealth" not in col and col not in ["country", "year"]
    ]

    for v in variables:
        # Create a mask to check if any value is negative
        mask = tb[v] < 0
        tb_error = tb[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
            log.warning(
                f"""{len(tb_error)} observations for {v} are negative:
                {tabulate(tb_error[['country', 'year', v]], headers = 'keys', tablefmt = TABLEFMT)}"""
            )

    return tb


def check_monotonicity(tb: Table, metric: list, welfare: list):
    """
    Check monotonicity for shares, thresholds and averages
    """

    tb = tb.copy()

    # Create a list of variables containing pxpy_share
    variables = [f"p{i}p{i+10}" for i in range(0, 100, 10)]

    for e in EXTRAPOLATED_DICT:
        for w in welfare:
            for m in metric:
                # Set columns to evaluate
                cols = [f"{v}_{m}_{w}{EXTRAPOLATED_DICT[e]}" for v in variables]

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
                tb_error = tb[mask].reset_index(drop=True).copy()

                if not tb_error.empty:
                    log.fatal(
                        f"""{len(tb_error)} observations for {m}_{w}{EXTRAPOLATED_DICT[e]} are not monotonically increasing:
                        {tabulate(tb_error[['country', 'year'] + cols], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".2f")}"""
                    )

    return tb


def check_avg_between_thr(tb: Table, welfare: list) -> Table:
    """
    Check that each avg is between the corresponding thr
    """

    tb = tb.copy()

    for e in EXTRAPOLATED_DICT:
        for w in welfare:
            check_cols = []
            check_nulls = []
            for i in range(0, 100, 10):
                # Create lower bound, avg and upper bound columns
                tb["thr_lower"] = tb[f"p{i}p{i+10}_thr_{w}{EXTRAPOLATED_DICT[e]}"]
                tb["avg"] = tb[f"p{i}p{i+10}_avg_{w}{EXTRAPOLATED_DICT[e]}"]

                if i < 90:
                    tb["thr_upper"] = tb[f"p{i+10}p{i+20}_thr_{w}{EXTRAPOLATED_DICT[e]}"]

                    # Count the nulls between the vars I am checking
                    tb[f"null_check_{i}"] = tb[["thr_lower", "avg", "thr_upper"]].isnull().sum(1)

                    # Create check column
                    tb[f"check_{i}"] = (tb["avg"] >= tb["thr_lower"]) & (tb["avg"] <= tb["thr_upper"])
                else:
                    # Count the nulls between the vars I am checking
                    tb[f"null_check_{i}"] = tb[["thr_lower", "avg"]].isnull().sum(1)

                    # Create check column
                    tb[f"check_{i}"] = tb["avg"] >= tb["thr_lower"]

                check_cols.append(f"check_{i}")
                check_nulls.append(f"null_check_{i}")

            tb["check"] = tb[check_cols].all(1)
            tb["null_check"] = tb[check_nulls].sum(1)

            mask = (~tb["check"]) & (tb["null_check"] == 0)

            tb_error = tb[mask].reset_index(drop=True).copy()

            if not tb_error.empty:
                log.fatal(
                    f"""{len(tb_error)} observations for avg {w}{EXTRAPOLATED_DICT[e]} are not between the corresponding thresholds:
                    {tabulate(tb_error[['country', 'year'] + check_cols], headers = 'keys', tablefmt = TABLEFMT)}"""
                )

    return tb
