import pandas as pd


def merge_dfs(df, columns):
    """
    Merge multiple dataframes based on specific columns.

    Args:
        df (DataFrame): The input dataframe.
        columns (list): A list of column names to use for merging.

    Returns:
        DataFrame: The merged dataframe.

    Note:
        - The input dataframe should have the specified columns for merging.
        - The merging is performed based on the columns 'days_since' and 'name'.
        - If the input dataframe has a 'training_data' column, the merging is also performed on that column.
        - The merging is performed using an outer join.

    """
    all_dfs = []

    for col in columns:
        grouped_df = process_df(df, col, col)
        all_dfs.append(grouped_df)

    merge_columns = ["days_since", "name"]
    if "training_data" in df.columns:
        merge_columns.append("training_data")

    merged_df = pd.merge(all_dfs[0], all_dfs[1], on=merge_columns, how="outer")

    if len(all_dfs) > 2:
        for i in range(2, len(all_dfs)):
            merged_df = pd.merge(merged_df, all_dfs[i], on=merge_columns, how="outer")

    return merged_df


def process_df(df, column, dropna_column):
    """
    Process a DataFrame by finding the maximum value for a column in each group.

    Args:
        df (DataFrame): The input DataFrame.
        column (str): The name of the column to find the maximum value.
        dropna_column (str): The name of the column to check for NaN values.

    Returns:
        DataFrame: The processed DataFrame with the maximum values and dropped NaN rows.
    """
    # Ensure there are no extra spaces at the end of column names
    df["name"] = df["name"].str.strip()
    if "training_data" in df.columns:
        # Group the DataFrame by 'days_since', 'name', and find the maximum value for the specified column
        grouped_df = df.groupby(["days_since", "training_data", "name"]).agg({column: "max"})
    else:
        grouped_df = df.groupby(["days_since", "name"]).agg({column: "max"})

    # Reset the index of the grouped DataFrame
    grouped_df = grouped_df.reset_index()

    # Drop rows with NaN values in the specified column
    df_drop = grouped_df.dropna(subset=[dropna_column])

    # Reset the index of the dropped DataFrame
    df_drop.reset_index(drop=True, inplace=True)

    return df_drop


def check_improvement(df, column_name):
    """
    Check improvement in performance in a benchmark column and create a new column with the improvement status.

    Args:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column to check for improvement.

    Returns:
        DataFrame: The updated DataFrame with the new column indicating the improvement status.
    """

    # Create a new column with the same values as the input column
    df[column_name + "_improved"] = "Not applicable"

    # Initialize previous value and days_since with the first row values

    # Check if 'training_data' column exists in the DataFrame
    if "training_data" in df.columns:
        first_non_nan_index = df[column_name].first_valid_index()
        previous_value = df[column_name].iloc[first_non_nan_index]
        previous_days_since = df["days_since"].iloc[first_non_nan_index]

        for i, value in enumerate(df[column_name].iloc[first_non_nan_index + 1 :], start=first_non_nan_index + 1):
            days_since = df["days_since"].iloc[i]
            with_without = df["training_data"].iloc[i]

            # Check if the value is NaN and assign the corresponding improvement status
            if pd.isnull(value):
                df.at[i, column_name + "_improved"] = "Not applicable"

            # Check if the value is greater than the previous value and meets the condition of days_since
            elif pd.notnull(previous_value) and value > previous_value and days_since >= previous_days_since:
                if with_without == "With extra data":
                    df.at[i, column_name + "_improved"] = "State of the art (with extra training data)"
                else:
                    df.at[i, column_name + "_improved"] = "State of the art (without extra training data)"

                # Update the previous value and days_since
                previous_value = value
                previous_days_since = days_since
            else:
                if with_without == "With extra data":
                    df.at[first_non_nan_index, column_name + "_improved"] = "Others (with extra data)"
                else:
                    df.at[first_non_nan_index, column_name + "_improved"] = "Others (without extra data)"

        if df["training_data"].iloc[0] == "Without extra data":
            df.at[0, column_name + "_improved"] = "State of the art (without extra training data)"
        else:
            df.at[0, column_name + "_improved"] = "State of the art (with extra training data)"
    else:
        first_non_nan_index = df[column_name].first_valid_index()
        previous_value = df[column_name].iloc[first_non_nan_index]
        previous_days_since = df["days_since"].iloc[first_non_nan_index]

        # Iterate over the remaining values in the column
        for i, value in enumerate(df[column_name].iloc[first_non_nan_index + 1 :], start=first_non_nan_index + 1):
            days_since = df["days_since"].iloc[i]

            # Check if the value is NaN and assign the corresponding improvement status
            if pd.isnull(value):
                df.at[i, column_name + "_improved"] = "Not applicable"

            elif pd.notnull(previous_value) and value > previous_value and days_since >= previous_days_since:
                df.at[i, column_name + "_improved"] = "State of the art"
                previous_value = value
                previous_days_since = days_since
            else:
                df.at[i, column_name + "_improved"] = "Others"

        # Handle the case of the first value being NaN or set it as the initial improvement status
        df.at[first_non_nan_index, column_name + "_improved"] = "State of the art"

        df[column_name + "_improved"] = df[column_name + "_improved"].astype(str)

    return df
