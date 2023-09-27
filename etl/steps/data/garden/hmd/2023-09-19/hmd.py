"""Load a meadow dataset and create a garden dataset."""
from typing import List, cast

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
    # Sanity checks
    log.info("hmd: checking dimension values.")
    columns_dim = ["format", "type", "sex", "age"]
    for col in columns_dim:
        not_in_ex = set(tb_lt[col]) - set(tb_ex[col])
        not_in_lt = set(tb_ex[col]) - set(tb_lt[col])
        assert not not_in_lt, f"Found values in column {col} in exposures but not in life tables!"
        assert not not_in_ex, f"Found values in column {col} in life tables but not in exposures!"

    # Harmonise countries
    log.info("hmd: harmonising countries.")
    tb_lt = geo.harmonize_countries(
        df=tb_lt, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_ex = geo.harmonize_countries(
        df=tb_ex, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Combine LE + Exposures
    log.info("hmd: combining life tables and exposures.")
    columns_primary = ["format", "type", "country", "year", "sex", "age"]
    tb = tb_lt.merge(tb_ex, on=columns_primary, how="outer")

    # Add extra variables: life expectancy f-m, f/m
    log.info("hmd: calculating extra variables (ratio and difference in life expectancy for f and m).")
    tb = add_le_diff_and_ratios(tb, columns_primary)

    # Scale central death rates
    tb["central_death_rate"] = tb["central_death_rate"] * 1000

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
    metric = "life_expectancy"
    tb_le = tb[columns_primary + [metric]].dropna(subset=[metric])
    tb_le_m = tb_le[tb_le["sex"] == "male"].drop(columns=["sex"])
    tb_le_f = tb_le[tb_le["sex"] == "female"].drop(columns=["sex"])
    ## Merge f and m tables
    tb_le = tb_le_f.merge(tb_le_m, on=list(set(columns_primary) - {"sex"}), suffixes=("_f", "_m"))
    ## Calculate extra variables
    tb_le["life_expectancy_fm_diff"] = tb_le["life_expectancy_f"] - tb_le["life_expectancy_m"]
    tb_le["life_expectancy_fm_ratio"] = tb_le["life_expectancy_f"] / tb_le["life_expectancy_m"]
    ## Set sex dimension to none
    tb_le["sex"] = "both"
    ## optional cast
    tb_le = cast(Table, tb_le)

    ## Add table to main table
    tb = tb.merge(tb_le, on=columns_primary, how="left")

    return tb
