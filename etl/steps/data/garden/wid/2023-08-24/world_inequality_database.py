"""Load World Inequality Database meadow dataset and create a garden dataset."""


import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from shared import add_metadata_vars, add_metadata_vars_distribution
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define combinations of variables to calculate relative poverty
extrapolated_dict = {"no": "", "yes": "_extrapolated"}
welfare_vars = ["pretax", "posttax_nat", "posttax_dis", "wealth"]


# Data processing function (cleaning and small transformations)
def data_processing(tb: Table) -> Table:
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
    log.info("add_relative_poverty.start")

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


def run(dest_dir: str) -> None:
    log.info("world_inequality_database.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("world_inequality_database")

    # Read table from meadow dataset.
    tb = ds_meadow["world_inequality_database"].reset_index()

    #
    # Process data.
    # Change units and drop unnecessary columns
    tb = data_processing(tb)

    ########################################
    # Percentile data
    ########################################

    # Read table from meadow dataset.
    tb_percentiles = ds_meadow["world_inequality_database_distribution"].reset_index()

    #
    # Process data.
    # Multiple share and share_extrapolated columns by 100
    tb_percentiles[["share", "share_extrapolated"]] *= 100

    # Add relative poverty values
    tb_relative_poverty = add_relative_poverty(tb, tb_percentiles, extrapolated_dict, welfare_vars)

    # Merge tables
    tb = pr.merge(tb, tb_relative_poverty, on=["country", "year"], how="left")

    # Add metadata by code
    tb = add_metadata_vars(tb)

    # Add metadata by code
    tb_percentiles = add_metadata_vars_distribution(tb_percentiles)

    # Set index and sort
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()
    tb_percentiles = tb_percentiles.set_index(
        ["country", "year", "welfare", "p", "percentile"], verify_integrity=True
    ).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset and add the garden table.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_percentiles], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("world_inequality_database.end")
