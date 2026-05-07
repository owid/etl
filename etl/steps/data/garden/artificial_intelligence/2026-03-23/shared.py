from owid.catalog import Table


def preprocess_data(tb: Table, index_columns: list, pivot_column: str, value_column: str) -> Table:
    """
    Preprocesses the input data by creating a pivot table and renaming group entries.

    Args:
        tb (Table): The input table containing the data to preprocess.
        index_columns (list): A list of column names to use as the index in the pivot table.
        pivot_column (str): The column name to pivot on.
        value_column (str): The column name containing the values for the pivot table.

    Returns:
        Table: A preprocessed table with a pivot table format and renamed group entries.
    """
    # Create a pivot table for each demographic group
    pivot_tb = tb.pivot(
        index=index_columns,
        columns=pivot_column,
        values=value_column,
    ).reset_index()

    pivot_tb = pivot_tb.rename_axis(None, axis=1)
    rename_entries = {
        "18-29": "Ages 18-29",
        "30-44": "Ages 30-44",
        "45-64": "Ages 45-64",
        "65+": "Ages 65+",
        "Male": "Men",
        "Female": "Women",
        "2-year": "2-year post-secondary education",
        "4-year": "4-year post-secondary education",
        "High school graduate": "High school graduates",
        "No HS": "No high school education",
        "Post-grad": "Post-graduate education",
    }
    pivot_tb["group"] = pivot_tb["group"].replace(rename_entries)

    return pivot_tb
