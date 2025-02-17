import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset


def preprocess_data(tb: Table, index_columns: list, pivot_column: str, value_column: str) -> Table:
    """
    Preprocesses the input data by transforming the date column, creating a pivot table, and renaming group entries.

    Args:
        tb (Table): The input table containing the data to preprocess.
        date_column (str): The name of the column containing date values.
        index_columns (list): A list of column names to use as the index in the pivot table.
        pivot_column (str): The column name to pivot on.
        value_column (str): The column name containing the values for the pivot table.

    Returns:
        Table: A preprocessed table with a pivot table format and renamed group entries.

    Raises:

    """
    # Create a pivot table for each demographic group
    pivot_tb = tb.pivot(
        index=index_columns,
        columns=pivot_column,
        values=value_column,
    ).reset_index()

    pivot_tb = pivot_tb.rename_axis(None, axis=1)
    rename_entries = {
        "18-29": "18-29 years",
        "2-year": "2-year post-secondary education",
        "30-44": "30-44 years",
        "4-year": "4-year post-secondary education",
        "45-64": "45-64 years",
        "65+": "65+ years",
        "High school graduate": "High school graduates",
        "No HS": "No high school education",
        "Post-grad": "Post-graduate education",
    }
    pivot_tb["group"] = pivot_tb["group"].replace(rename_entries)

    return pivot_tb


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

    if perform_merge:
        tb_first = group_sort_filter(tb, column_to_process[0])
        tb_second = group_sort_filter(tb, column_to_process[1])
        tb_combined = pr.merge(tb_first, tb_second, on=["date", "name"], how="outer")
        tb_to_save = tb_combined
    else:
        tb_to_save = group_sort_filter(tb, column_to_process[0])

    tb_to_save = tb_to_save.format(["date", "name"])

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_to_save], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def group_sort_filter(tb, column):
    """
    Processes the Table to group, sort, and filter by performance improvement.

    """
    grouped = tb.groupby(["date", "name"]).agg({column: "max"})
    grouped = grouped.dropna(subset=[column]).reset_index()
    grouped = grouped.sort_values(by="date")
    max_performing = grouped.groupby("date")[column].idxmax()
    best_daily_models = grouped.loc[max_performing]
    best_daily_models = best_daily_models.sort_values(by="date")
    best_daily_models["is_improved"] = best_daily_models[column].gt(
        best_daily_models[column].cummax().shift(fill_value=-float("inf"))
    )
    return best_daily_models[best_daily_models["is_improved"]].drop(columns="is_improved")
