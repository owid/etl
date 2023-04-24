# To run with subset only: GHE_SUBSET_ONLY=1 etl grapher/who/2022-09-30/ghe --grapher
# NOTE: This is a massive dataset with 50M rows and 50k variables (there are just 4 actual
# columns, but 12500 dimension combinations). It takes ~1.5h to upload it to grapher with
# 40 workers.
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
GHE_SUBSET_ONLY = False


def run(dest_dir: str) -> None:
    # Load garden dataset
    ds_garden: Dataset = paths.load_dependency("ghe")

    # Read table from garden dataset.
    tb_garden = ds_garden["ghe"]

    # Process table
    tb_grapher = process(tb_garden)
    # Add table metadata
    tb_grapher.metadata = tb_garden.metadata
    for col in tb_grapher.columns:
        tb_grapher[col].metadata = tb_garden[col].metadata
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_grapher], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def process(table: Table) -> Table:
    table = table.copy()
    # Since this script expects a certain structure make sure it is actually met
    expected_primary_keys = ["country", "year", "age_group", "sex", "cause"]
    if table.primary_key != expected_primary_keys:
        raise Exception(
            "GHE Table to transform to grapher contained unexpected primary key dimensions:"
            f" {table.primary_key} instead of {expected_primary_keys}"
        )
    # We want to export all columns except causegroup and level (for now)
    columns_to_export = [
        "death_count",
        "death_rate100k",
        "daly_count",
        "daly_rate100k",
    ]
    if set(columns_to_export).difference(set(table.columns)):
        raise Exception(
            "GHE table to transform to grapher did not contain the expected columns but instead had:"
            f" {list(table.columns)}"
        )

    table = table.reset_index()
    table = table.drop(columns=["flag_level"])

    # Add World totals (should probably be in Garden)
    table = add_global_totals(table)

    # Use subset of the data for now
    if GHE_SUBSET_ONLY:
        table = select_subset_causes(table)

    table = table.set_index(["country", "year", "cause", "sex", "age_group"])

    table = table.loc[:, columns_to_export]

    table.metadata.short_name = paths.short_name
    return table


def select_subset_causes(table: pd.DataFrame) -> pd.DataFrame:
    """Selects a subset of all the data.

    There are many diaggregations by age and sex we don't want all of them at the moment so we have this option to only load the subset.

    Basically, we only keep "all ages" age-group and "both sexes" sex group. EXCEPT, when the cause is "Self-harm" where we keep all dimensions (https://github.com/owid/owid-issues/issues/759#issuecomment-1455220066)
    """
    causes_include_all = ["Self-harm"]
    table = table[
        ((table["sex"] == "Both sexes") & (table["age_group"] == "ALLAges")) | (table["cause"].isin(causes_include_all))
    ]
    return table


def add_global_totals(df: pd.DataFrame) -> pd.DataFrame:
    # Get age_group=all and sex=all (avoid duplicates)
    df_ = df[(df["sex"] == "Both sexes") & (df["age_group"] == "ALLAges")]
    # Group by year and cause, sum
    glob_total = df_.groupby(["year", "cause"], as_index=False, observed=True)[["daly_count", "death_count"]].sum()
    # Fill other fields
    glob_total["country"] = "World"
    glob_total["age_group"] = "ALLAges"
    glob_total["sex"] = "Both sexes"
    # Merge with complete table
    df = pd.concat([df, glob_total])
    return df
