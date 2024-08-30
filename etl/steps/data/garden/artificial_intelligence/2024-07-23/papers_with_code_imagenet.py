from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("papers_with_code_imagenet")

    # Read table from meadow dataset.
    tb = ds_meadow["papers_with_code_imagenet"].reset_index()

    # Get the best performing model for each day.
    grouped = tb.groupby(["date", "name"]).agg({"top_1_accuracy": "max"})
    grouped = grouped.dropna(subset=["top_1_accuracy"]).reset_index()
    grouped = grouped.sort_values(by="date")
    max_performing = grouped.groupby("date")["top_1_accuracy"].idxmax()
    best_daily_models = grouped.loc[max_performing]
    best_daily_models = best_daily_models.sort_values(by="date")
    best_daily_models["is_improved"] = best_daily_models["top_1_accuracy"].gt(
        best_daily_models["top_1_accuracy"].cummax().shift(fill_value=-float("inf"))
    )
    best_daily_models = best_daily_models[best_daily_models["is_improved"]].drop(columns="is_improved")

    best_daily_models = best_daily_models.format(["date", "name"])

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[best_daily_models], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
