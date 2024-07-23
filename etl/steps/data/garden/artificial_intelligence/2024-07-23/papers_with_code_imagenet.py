import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("papers_with_code_imagenet")

    # Read table from meadow dataset.
    tb = ds_meadow["papers_with_code_imagenet"].reset_index()

    # Calculate 'days_since' column.
    tb["days_since"] = (
        pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days
    tb = tb.drop("date", axis=1)

    # Get the best performing model for each day.
    grouped = tb.groupby(["days_since", "name"]).agg({"top_1_accuracy": "max"})
    grouped = grouped.dropna(subset=["top_1_accuracy"]).reset_index()
    grouped = grouped.sort_values(by="days_since")
    max_performing = grouped.groupby("days_since")["top_1_accuracy"].idxmax()
    best_daily_models = grouped.loc[max_performing]
    best_daily_models = best_daily_models.sort_values(by="days_since")
    best_daily_models["is_improved"] = best_daily_models["top_1_accuracy"].gt(
        best_daily_models["top_1_accuracy"].cummax().shift(fill_value=-float("inf"))
    )

    best_daily_models = best_daily_models.format(["days_since", "name"])

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[best_daily_models], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
