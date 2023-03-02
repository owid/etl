"""Load the three LIS meadow datasets and create one garden dataset, `luxembourg_income_study`."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_metadata_vars
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Set relative and absolute poverty lines
relative_povlines = [40, 50, 60]


def run(dest_dir: str) -> None:
    log.info("luxembourg_income_study.start")

    #
    # Load inputs.

    ######################################################
    # Key variables
    ######################################################

    # Load `keyvars` meadow dataset, rename and drop variables
    df_keyvars = load_keyvars()

    # Create additional (relative) poverty variables
    df_keyvars = create_relative_pov_variables(df_keyvars, relative_povlines)

    # Make table wide
    df_keyvars = make_table_wide(df_keyvars, ["variable", "eq"])

    # Rename one pop variable created to keep it and drop all the others
    df_keyvars = df_keyvars.rename(columns={"pop_dhi_eq": "pop"})
    df_keyvars = df_keyvars[df_keyvars.columns.drop(list(df_keyvars.filter(like="pop_")))]

    ######################################################
    # Absolute poverty
    ######################################################

    # Load `abs_poverty` meadow dataset, rename variables
    df_abs_poverty = load_abs_poverty(df_keyvars)

    # Calculate additional absolute poverty variables
    df_abs_poverty = create_absolute_pov_variables(df_abs_poverty)

    # Make table wide
    df_abs_poverty = make_table_wide(df_abs_poverty, ["variable", "eq", "povline"])

    ######################################################
    # Distributional data
    ######################################################

    # Load `distribution` meadow dataset, rename variables
    df_distribution = load_distribution()

    # Calculate income ratios, decile averages and groups of shares
    df_distribution = create_distributional_variables(df_distribution)

    # Make table wide
    df_distribution = make_table_wide(df_distribution, ["variable", "eq"])

    # Merge tables
    df = pd.merge(df_keyvars, df_abs_poverty, on=["country", "year"], how="left")
    df = pd.merge(df, df_distribution, on=["country", "year"], how="left")

    # Replace inf values (division by 0)
    df = df.replace({np.inf: np.nan})

    # Drop population
    df = df.drop(columns=["pop"])

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True)
    df = df.sort_index().sort_index(axis=1)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="luxembourg_income_study")

    # Add metadata by code
    tb_garden = add_metadata_vars(tb_garden)

    #
    # Save outputs.
    #
    # Create a new garden dataset
    ds_garden = Dataset.create_empty(dest_dir)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)

    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("luxembourg_income_study.end")


#########################################################################
# Data processing functions
#########################################################################

# This function makes the table wide and modifies some columns before that
# It is applied to the three LIS datasets
def make_table_wide(df: pd.DataFrame, cols_to_wide: list) -> pd.DataFrame:

    # Drop dataset variable, to not see it multiplied
    df = df.drop(columns=["dataset"])

    # Change names of equivalized variable, to create a distinguishable name
    df["eq"] = df["eq"].replace({1: "eq", 0: "pc"})

    # Create pivot table and join different levels of column
    df = df.pivot(index=["country", "year"], columns=cols_to_wide).reset_index()
    df.columns = [" ".join(col).strip().replace(" ", "_") for col in df.columns.values]

    return df


# Load `keyvars` meadow dataset, rename and drop variables
def load_keyvars() -> pd.DataFrame:
    ds_meadow: Dataset = paths.load_dependency("luxembourg_income_study")
    tb_meadow = ds_meadow["lis_keyvars"]
    df_keyvars = pd.DataFrame(tb_meadow)

    # Use less technical names for some variables
    df_keyvars.columns = df_keyvars.columns.str.replace("fgt0", "headcount_ratio")
    df_keyvars.columns = df_keyvars.columns.str.replace("fgt1", "poverty_gap_index")

    # Drop unused poverty variables
    df_keyvars = df_keyvars.drop(
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

    return df_keyvars


# Create additional (relative) poverty variables
def create_relative_pov_variables(df_keyvars: pd.DataFrame, relative_povlines: list) -> pd.DataFrame:
    # Get DHI median (the one relative poverty is calculated from for any income/consumption) and merge
    df_median_dhi = df_keyvars[["country", "year", "median", "eq"]][df_keyvars["variable"] == "dhi"].reset_index(
        drop=True
    )
    df_keyvars = pd.merge(df_keyvars, df_median_dhi, on=["country", "year", "eq"], how="left", suffixes=(None, "_dhi"))

    for povline in relative_povlines:
        # Rename relative poverty variables suffix from 40/50/60 to 40/50/60_median
        df_keyvars.columns = df_keyvars.columns.str.replace(f"{povline}", f"{povline}_median")

        # Calculate number in poverty
        df_keyvars[f"headcount_{povline}_median"] = (
            df_keyvars[f"headcount_ratio_{povline}_median"] / 100
        ) * df_keyvars["pop"]
        df_keyvars[f"headcount_{povline}_median"] = df_keyvars[f"headcount_{povline}_median"].round(0)

        # Calculate income gap ratio
        df_keyvars[f"income_gap_ratio_{povline}_median"] = (
            df_keyvars[f"poverty_gap_index_{povline}_median"] / df_keyvars[f"headcount_ratio_{povline}_median"]
        )

        # Calculate average shortfall
        df_keyvars[f"avg_shortfall_{povline}_median"] = (
            df_keyvars[f"income_gap_ratio_{povline}_median"] * df_keyvars["median_dhi"] * povline / 100
        )

        # Calculate total shortfall
        df_keyvars[f"total_shortfall_{povline}_median"] = (
            df_keyvars[f"avg_shortfall_{povline}_median"] * df_keyvars[f"headcount_{povline}_median"]
        )

        # Make income gap ratio in %
        df_keyvars[f"income_gap_ratio_{povline}_median"] = df_keyvars[f"income_gap_ratio_{povline}_median"] * 100

    # Drop median dhi
    df_keyvars = df_keyvars.drop(columns=["median_dhi"])

    return df_keyvars


# Load `abs_poverty` meadow dataset, rename variables
def load_abs_poverty(df_keyvars: pd.DataFrame) -> pd.DataFrame:
    ds_meadow: Dataset = paths.load_dependency("luxembourg_income_study")
    tb_meadow = ds_meadow["lis_abs_poverty"]
    df_abs_poverty = pd.DataFrame(tb_meadow)

    # Add population variable from keyvars
    df_abs_poverty = pd.merge(
        df_abs_poverty, df_keyvars[["country", "year", "pop"]], on=["country", "year"], how="left"
    )

    # Use less technical names for some variables
    df_abs_poverty.columns = df_abs_poverty.columns.str.replace("fgt0", "headcount_ratio")
    df_abs_poverty.columns = df_abs_poverty.columns.str.replace("fgt1", "poverty_gap_index")

    return df_abs_poverty


# Calculate additional absolute poverty variables
def create_absolute_pov_variables(df_abs_poverty: pd.DataFrame) -> pd.DataFrame:
    # Calculate number in poverty
    df_abs_poverty["headcount"] = df_abs_poverty["headcount_ratio"] / 100 * df_abs_poverty["pop"]
    df_abs_poverty["headcount"] = df_abs_poverty["headcount"].round(0)

    # Calculate income gap ratio
    df_abs_poverty["income_gap_ratio"] = df_abs_poverty["poverty_gap_index"] / df_abs_poverty["headcount_ratio"]

    # Calculate average shortfall. I need to convert the poverty line to dollars per year (hence the / 100 * 365)
    df_abs_poverty["avg_shortfall"] = df_abs_poverty["income_gap_ratio"] * df_abs_poverty["povline"] / 100 * 365

    # Calculate total shortfall
    df_abs_poverty["total_shortfall"] = df_abs_poverty["avg_shortfall"] * df_abs_poverty["headcount"]

    # Make income gap ratio in %
    df_abs_poverty["income_gap_ratio"] = df_abs_poverty["income_gap_ratio"] * 100

    # Make pov lines string to make the pivot table possible
    df_abs_poverty["povline"] = df_abs_poverty["povline"].astype(str)

    # Remove population column
    df_abs_poverty = df_abs_poverty.drop(columns=["pop"])

    # Also remove unused poverty variables
    df_abs_poverty = df_abs_poverty.drop(columns=["fgt2", "meanpoor", "meangap"])

    return df_abs_poverty


# Load `distribution` meadow dataset, rename variables
def load_distribution() -> pd.DataFrame:
    ds_meadow: Dataset = paths.load_dependency("luxembourg_income_study")
    tb_meadow = ds_meadow["lis_distribution"]
    df_distribution = pd.DataFrame(tb_meadow)

    # Transform percentile variable to `pxx`
    df_distribution["percentile"] = "p" + df_distribution["percentile"].astype(str)

    # Make pivot table only with percentiles (To estimate ratios more easily)
    df_distribution = df_distribution.pivot(
        index=["country", "year", "dataset", "variable", "eq"], columns=["percentile"]
    ).reset_index()
    df_distribution.columns = [" ".join(col).strip().replace(" ", "_") for col in df_distribution.columns.values]

    return df_distribution


# Calculate income ratios and decile averages
def create_distributional_variables(df_distribution: pd.DataFrame) -> pd.DataFrame:

    # Calculate Palma ratio and other average/share ratios
    df_distribution["palma_ratio"] = df_distribution["share_p100"] / (
        df_distribution["share_p10"]
        + df_distribution["share_p20"]
        + df_distribution["share_p30"]
        + df_distribution["share_p40"]
    )
    df_distribution["s80_s20_ratio"] = (df_distribution["share_p100"] + df_distribution["share_p90"]) / (
        df_distribution["share_p10"] + df_distribution["share_p20"]
    )
    df_distribution["p90_p10_ratio"] = df_distribution["thr_p90"] / df_distribution["thr_p10"]
    df_distribution["p90_p50_ratio"] = df_distribution["thr_p90"] / df_distribution["thr_p50"]
    df_distribution["p50_p10_ratio"] = df_distribution["thr_p50"] / df_distribution["thr_p10"]

    # Calculate share of the botton 50%
    df_distribution["share_bottom50"] = (
        df_distribution["share_p10"]
        + df_distribution["share_p20"]
        + df_distribution["share_p30"]
        + df_distribution["share_p40"]
        + df_distribution["share_p50"]
    )

    # Calculate share of the middle 40%
    df_distribution["share_middle40"] = (
        df_distribution["share_p60"]
        + df_distribution["share_p70"]
        + df_distribution["share_p80"]
        + df_distribution["share_p90"]
    )

    # Create decile averages
    # Add mean data to dataframe

    # Load keyvars again
    df_mean = load_keyvars()
    df_mean = df_mean[["country", "year", "variable", "eq", "mean"]]

    df_distribution = pd.merge(df_distribution, df_mean, on=["country", "year", "variable", "eq"], how="left")

    for i in range(1, 11):
        perc = i * 10
        df_distribution[f"avg_p{perc}"] = df_distribution[f"share_p{perc}"] / 100 * df_distribution["mean"] / 0.1

    # Drop mean from dataframe
    df_distribution = df_distribution.drop(columns=["mean"])

    return df_distribution
