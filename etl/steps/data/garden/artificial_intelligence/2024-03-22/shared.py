import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset


def load_and_process_dataset(
    file_indicator: str, column_to_process: list, dest_dir: str, paths: PathFinder, perform_merge: bool = False
):
    """
    Loads a dataset, processes it, and creates a new garden dataset.

    Args:
    - file_indicator: The unique part of the dataset name to load.
    - column_to_process: List of column names to process. Pass one column for simple processing, pass two for merging.
    - dest_dir: Destination directory to save the new dataset.
    - paths: An instance of PathFinder to get dataset paths.
    - perform_merge: Boolean indicating whether to merge two processed tables. Default is False.
    """
    # Load meadow dataset.
    ds_meadow = paths.load_dataset(f"papers_with_code_{file_indicator}")

    # Read table from meadow dataset.
    tb = ds_meadow[f"papers_with_code_{file_indicator}"].reset_index()

    # Calculate 'days_since' column.
    tb["days_since"] = (
        pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days
    tb = tb.drop("date", axis=1)

    if perform_merge:
        tb_first = group_sort_filter(tb, column_to_process[0])
        tb_second = group_sort_filter(tb, column_to_process[1])
        tb_combined = pr.merge(tb_first, tb_second, on=["days_since", "name"], how="outer")
        tb_to_save = tb_combined
    else:
        tb_to_save = group_sort_filter(tb, column_to_process[0])

    tb_to_save = tb_to_save.set_index(["days_since", "name"], verify_integrity=True).sort_index()

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_to_save], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def group_sort_filter(tb, column):
    """
    Processes the Table to group, sort, and filter by performance improvement.

    """
    grouped = tb.groupby(["days_since", "name"]).agg({column: "max"})
    grouped = grouped.dropna(subset=[column]).reset_index()
    grouped = grouped.sort_values(by="days_since")
    max_performing = grouped.groupby("days_since")[column].idxmax()
    best_daily_models = grouped.loc[max_performing]
    best_daily_models = best_daily_models.sort_values(by="days_since")
    best_daily_models["is_improved"] = best_daily_models[column].gt(
        best_daily_models[column].cummax().shift(fill_value=-float("inf"))
    )
    return best_daily_models[best_daily_models["is_improved"]].drop(columns="is_improved")
