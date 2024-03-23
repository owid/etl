"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load inputs.
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("papers_with_code_language")

    # Read table from meadow dataset.
    tb = ds_meadow["papers_with_code_language"].reset_index()
    # Calculate 'days_since' column.
    tb["days_since"] = (
        pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days
    tb = tb.drop("date", axis=1)

    tb = group_sort_filter(tb, "performance_language_average")

    tb = tb.set_index(["days_since", "name"], verify_integrity=True).sort_index()
    # Save outputs.
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def group_sort_filter(tb, column):
    # Group the DataFrame by 'days_since', 'name', and find the maximum value for the performance
    grouped = tb.groupby(["days_since", "name"]).agg({column: "max"})
    grouped = grouped.dropna(subset=[column]).reset_index()
    # Ensure the tables are sorted
    grouped = grouped.sort_values(by="days_since")
    # Find the maximum performing model on each day
    max_performing = grouped.groupby("days_since")[column].idxmax()
    best_daily_models = grouped.loc[max_performing]
    # Sort the DataFrame by 'days_since'
    best_daily_models = best_daily_models.sort_values(by="days_since")
    # Use cummax to compare current row performance with the previous highest one
    best_daily_models["is_improved"] = best_daily_models[column].gt(
        best_daily_models[column].cummax().shift(fill_value=-float("inf"))
    )
    # Filter rows where 'is_improved' is True
    return best_daily_models[best_daily_models["is_improved"]].drop(columns="is_improved")
