"""Load a meadow dataset and create a garden dataset."""
from typing import List, cast

import numpy as np
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    log.info("hmd: reading table from meadow dataset.")
    tb_lt = ds_meadow["life_tables"].reset_index()
    tb_ex = ds_meadow["exposures"].reset_index()
    tb_de = ds_meadow["deaths"].reset_index()

    #
    # Process data.
    #
    # Standardise dimension values
    log.info("hmd: standardising sex dimension values.")
    tb_lt["sex"] = tb_lt["sex"].map(
        {
            "Males": "male",
            "Females": "female",
            "Total": "both",
        }
    )
    tb_ex["sex"] = tb_ex["sex"].map(
        {
            "Male": "male",
            "Female": "female",
            "Total": "both",
        }
    )
    tb_de["sex"] = tb_de["sex"].map(
        {
            "Male": "male",
            "Female": "female",
            "Total": "both",
        }
    )
    # Sanity checks (compare each table agains table `life_tables`)
    log.info("hmd: checking dimension values.")
    columns_dim = ["format", "type", "sex", "age"]
    for col in columns_dim:
        not_in_ex = set(tb_lt[col]) - set(tb_ex[col])
        not_in_lt = set(tb_ex[col]) - set(tb_lt[col])
        not_in_de = set(tb_de[col]) - set(tb_lt[col])
        assert not not_in_lt, f"Found values in column {col} in exposures but not in life tables!"
        assert not not_in_ex, f"Found values in column {col} in life tables but not in exposures!"
        assert not not_in_de, f"Found values in column {col} in life tables but not in deaths!"

    # Harmonise countries
    log.info("hmd: harmonising countries.")
    tb_lt = geo.harmonize_countries(
        df=tb_lt, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_ex = geo.harmonize_countries(
        df=tb_ex, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_de = geo.harmonize_countries(
        df=tb_de, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Combine LE + Exposures + Deaths
    log.info("hmd: combining `life tables`, `exposures` and `deaths`.")
    columns_primary = ["format", "type", "country", "year", "sex", "age"]
    tb = tb_lt.merge(tb_ex, on=columns_primary, how="outer")
    tb = tb.merge(tb_de, on=columns_primary, how="outer")

    # Add extra variables: life expectancy f-m, f/m
    log.info("hmd: calculating extra variables (ratio and difference in life expectancy for f and m).")
    tb = add_le_diff_and_ratios(tb, columns_primary)

    # Scale central death rates
    tb["central_death_rate"] = tb["central_death_rate"] * 1000
    tb["probability_of_death"] = tb["probability_of_death"] * 100

    # Final metadata touches
    tb.metadata.short_name = paths.short_name
    # Set index
    tb = tb.set_index(columns_primary, verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_le_diff_and_ratios(tb: Table, columns_primary: List[str]) -> Table:
    """Add metrics on life expectancy ratios and differences between females and males."""
    ## Get relevant metric, split into f and m tables
    metrics = {
        "life_expectancy": ["ratio_fm", "diff_fm"],
        "central_death_rate": ["ratio_mf"],
    }
    for metric, operations in metrics.items():
        tb_metric = tb[columns_primary + [metric]].dropna(subset=[metric])
        tb_metric_m = tb_metric[tb_metric["sex"] == "male"].drop(columns=["sex"])
        tb_metric_f = tb_metric[tb_metric["sex"] == "female"].drop(columns=["sex"])

        ## Merge f and m tables
        tb_metric = tb_metric_f.merge(tb_metric_m, on=list(set(columns_primary) - {"sex"}), suffixes=("_f", "_m"))
        ## Calculate extra variables
        if "diff_fm" in operations:
            tb_metric[f"{metric}_fm_diff"] = tb_metric[f"{metric}_f"] - tb_metric[f"{metric}_m"]
            tb_metric[f"{metric}_fm_diff"] = tb_metric[f"{metric}_fm_diff"].replace([np.inf, -np.inf], np.nan)
        if "diff_mf" in operations:
            tb_metric[f"{metric}_mf_diff"] = tb_metric[f"{metric}_m"] - tb_metric[f"{metric}_f"]
            tb_metric[f"{metric}_mf_diff"] = tb_metric[f"{metric}_mf_diff"].replace([np.inf, -np.inf], np.nan)
        if "ratio_fm" in operations:
            tb_metric[f"{metric}_fm_ratio"] = tb_metric[f"{metric}_f"] / tb_metric[f"{metric}_m"]
            tb_metric[f"{metric}_fm_ratio"] = tb_metric[f"{metric}_fm_ratio"].replace([np.inf, -np.inf], np.nan)
        if "ratio_mf" in operations:
            tb_metric[f"{metric}_mf_ratio"] = tb_metric[f"{metric}_m"] / tb_metric[f"{metric}_f"]
            tb_metric[f"{metric}_mf_ratio"] = tb_metric[f"{metric}_mf_ratio"].replace([np.inf, -np.inf], np.nan)
        # drop individual sex columns
        tb_metric = tb_metric.drop(columns=[f"{metric}_f", f"{metric}_m"])
        ## Set sex dimension to none
        tb_metric["sex"] = "both"
        ## optional cast
        tb_metric = cast(Table, tb_metric)

        ## Add table to main table
        tb = tb.merge(tb_metric, on=columns_primary, how="left")

    return tb
