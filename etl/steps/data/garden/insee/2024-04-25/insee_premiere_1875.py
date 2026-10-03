"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep
COLUMNS_TO_KEEP = [
    "p90_p10_ratio",
    "s80_s20_ratio",
    "gini",
    "headcount_50_median",
    "headcount_60_median",
    "headcount_ratio_50_median",
    "headcount_ratio_60_median",
    "income_gap_ratio_50_median",
    "income_gap_ratio_60_median",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("insee_premiere_1875")

    # Read table from meadow dataset.
    tb_inequality = ds_meadow["inequality"].reset_index()
    tb_poverty = ds_meadow["poverty"].reset_index()

    # Merge both tables
    tb = pr.merge(tb_inequality, tb_poverty, on=["country", "year"], how="outer", short_name=paths.short_name)

    #
    # Process data.
    tb = tb.format(["country", "year"])

    # Keep relevant columns
    tb = tb[COLUMNS_TO_KEEP]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
