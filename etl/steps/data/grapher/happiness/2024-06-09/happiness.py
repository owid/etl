"""Load a garden dataset and create a grapher dataset."""
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

AGES_COLUMNS = [
    "happiness_below_30",
    "happiness_30_to_44",
    "happiness_45_to_59",
    "happiness_60_and_above",
    "happiness_all_ages",
]


def pivot_age_groups(tb: Table):
    new_tb_rows = []
    for cty in tb["country"].unique():
        cty_tb = tb[tb["country"] == cty]
        for year in cty_tb["year"].unique():
            new_row_dict = {"country": cty, "year": year}
            row_tb = cty_tb[cty_tb["year"] == year]
            ages = ["below 30", "30-44", "45-59", "60 and above", "all ages"]
            for idx in range(len(ages)):
                age_entry = ages[idx]
                age_column = AGES_COLUMNS[idx]
                age_row = row_tb[row_tb["age_group"] == age_entry]
                if len(age_row) == 0:
                    new_row_dict[age_column] = pd.NA
                else:
                    new_row_dict[age_column] = age_row["happiness_score"].values[0]
            new_tb_rows.append(new_row_dict)
    tb_pivot = Table(
        pd.DataFrame(
            new_tb_rows,
            columns=[
                "country",
                "year",
            ]
            + AGES_COLUMNS,
        )
    )
    tb_pivot = tb_pivot.copy_metadata(tb)
    return tb_pivot


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("happiness")

    # Read table from garden dataset.
    tb = ds_garden["happiness"].reset_index()

    # pivot table
    tb = tb.drop(columns=["population"])

    tb = pivot_age_groups(tb)

    for age_col in AGES_COLUMNS:
        tb[age_col] = tb[age_col].astype("Float64")

    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    # origins get added in grapher dataset, so do not warn about missing origins.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=False, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
