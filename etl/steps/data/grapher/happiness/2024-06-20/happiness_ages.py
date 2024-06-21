"""Load a garden dataset and create a grapher dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def pivot_age_groups(tb):
    new_tb_rows = []
    for cty in tb["country"].unique():
        cty_tb = tb[tb["country"] == cty]
        for year in cty_tb["year"].unique():
            new_row_dict = {"country": cty, "year": year}
            row_tb = cty_tb[cty_tb["year"] == year]
            new_row_dict["happiness_below_30"] = row_tb[row_tb["age_group"] == "below 30"]["happiness_score"].values[0]
            new_row_dict["happiness_30_to_44"] = row_tb[row_tb["age_group"] == "30-44"]["happiness_score"].values[0]
            new_row_dict["happiness_45_to_59"] = row_tb[row_tb["age_group"] == "45-59"]["happiness_score"].values[0]
            new_row_dict["happiness_60_and_above"] = row_tb[row_tb["age_group"] == "60 and above"][
                "happiness_score"
            ].values[0]
            new_tb_rows.append(new_row_dict)
    tb_pivot = Table(
        pd.DataFrame(
            new_tb_rows,
            columns=[
                "country",
                "year",
                "happiness_below_30",
                "happiness_30_to_44",
                "happiness_45_to_59",
                "happiness_60_and_above",
            ],
        )
    )
    tb_pivot = tb_pivot.copy_metadata(tb)
    return tb_pivot


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("happiness_ages")

    # Read table from garden dataset.
    tb = ds_garden["happiness_ages"].reset_index()

    #
    # Process data.
    #
    tb = tb.drop(columns=["population"])

    tb = pivot_age_groups(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
