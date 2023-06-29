import pandas as pd


def select_best_on_date(df, column_year):
    max_df = []
    # Loop through each year
    for year in df[column_year].unique():
        # Filter the DataFrame for the current year
        year_data = df[df[column_year] == year]
        # Check if "training_data" column is present
        if "training_data" in year_data.columns:
            # Loop through each unique training_data value
            for training_data in year_data["training_data"].unique():
                # Filter the DataFrame for the current year and training_data value
                training_data_data = year_data[year_data["training_data"] == training_data]
                # Loop through each column (excluding 'name', 'days_since', and 'training_data')
                for column in training_data_data.columns:
                    if column not in ["name", column_year, "training_data"]:
                        # Check if there is at least one non-NaN value in the column for the current year and training_data
                        if not training_data_data[column].isnull().all():
                            # Find the model with the highest performance and its value for the current column
                            max_value = training_data_data[column].max()
                            # Find the name that corresponds to this maximum value
                            max_name = training_data_data.loc[training_data_data[column].idxmax(), "name"]
                            # Append the year, column (performance), name, training_data, and maximum value to max_df
                            max_df.append(
                                {
                                    column_year: year,
                                    "performance": column,
                                    "name": max_name,
                                    "training_data": training_data,
                                    "accuracy": max_value,
                                }
                            )
        else:
            # Loop through each column (excluding 'name' and 'days_since')
            for column in year_data.columns:
                if column not in ["name", column_year]:
                    # Check if there is at least one non-NaN value in the column for the current year
                    if not year_data[column].isnull().all():
                        # Find the model with the highest performance and its value for the current column
                        max_value = year_data[column].max()
                        # Find the name that corresponds to this maximum value
                        max_name = year_data.loc[year_data[column].idxmax(), "name"]
                        # Append the year, column (performance), name, and maximum value to max_df
                        max_df.append(
                            {column_year: year, "performance": column, "name": max_name, "accuracy": max_value}
                        )

    # Convert the result to DataFrame
    result_df_max = pd.DataFrame(max_df)

    # Pivot the DataFrame based on the available columns
    if "training_data" in result_df_max.columns:
        pivot_df = result_df_max.pivot_table(
            values="accuracy", index=[column_year, "name", "training_data"], columns="performance", aggfunc=max
        )
    else:
        pivot_df = result_df_max.pivot_table(
            values="accuracy", index=[column_year, "name"], columns="performance", aggfunc=max
        )

    # Reset index and remove the name of columns
    pivot_df.reset_index(inplace=True)
    pivot_df.columns.name = None

    return pivot_df
