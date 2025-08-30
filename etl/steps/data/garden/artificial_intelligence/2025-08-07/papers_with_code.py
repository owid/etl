"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("papers_with_code")

    # Read table from meadow dataset.
    tb = ds_meadow.read("papers_with_code")

    #
    # Process data.
    #
    # Apply group_sort_filter to each metric column separately
    metric_columns = [
        "competitions_pass_1",
        "interviews_pass_1",
        "top_1_accuracy",
        "top_5_accuracy",
        "average_mmlu_accuracy",
        "math_accuracy",
    ]

    processed_tables = []
    for column in metric_columns:
        if column in tb.columns:
            tb_processed = group_sort_filter(tb, column)
            processed_tables.append(tb_processed)

    processed_tables = []
    for column in metric_columns:
        if column in tb.columns:
            tb_processed = group_sort_filter(tb, column)

            processed_tables.append(tb_processed)

    # Merge all dataframes
    tb = processed_tables[0]
    for current_df in processed_tables[1:]:
        tb = tb.merge(current_df, on=["model_name", "paper_date"], how="outer")
    tb = tb.rename(columns={"model_name": "country", "paper_date": "date"})

    # Improve table format.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def group_sort_filter(tb, column):
    """
    Processes the Table to group, sort, and filter by performance improvement.
    """
    # Use the actual column names from the table
    grouped = tb.groupby(["paper_date", "model_name"]).agg({column: "max"})
    grouped = grouped.dropna(subset=[column]).reset_index()
    grouped = grouped.sort_values(by="paper_date")
    print(grouped)
    max_performing = grouped.groupby("paper_date")[column].idxmax()
    best_daily_models = grouped.loc[max_performing]
    best_daily_models = best_daily_models.sort_values(by="paper_date")
    best_daily_models["is_improved"] = best_daily_models[column].gt(
        best_daily_models[column].cummax().shift(fill_value=-float("inf"))
    )
    best_daily_models["model_name"] = "State of the art"

    return best_daily_models[best_daily_models["is_improved"]].drop(columns="is_improved")
