import pandas as pd


def select_best(df):
    # Create empty dictionaries to store the models with the highest performance and their values for each year
    max_without_extra_training = {}
    max_with_extra_training = {}

    # Loop through each year
    for year in df["year"].unique():
        # Filter the DataFrame for the current year
        year_data = df[df["year"] == year]

        # Check if there is at least one non-NaN value in "Without extra training data" for the current year
        if not year_data["Without extra training data"].isnull().all():
            # Find the model with the highest performance and its value for "Without extra training data"
            max_without_extra_training[year] = (
                year,
                year_data.loc[year_data["Without extra training data"].idxmax(), "name"],
                year_data.loc[year_data["Without extra training data"].idxmax(), "Without extra training data"],
            )

        # Check if there is at least one non-NaN value in "With extra training data" for the current year
        if not year_data["With extra training data"].isnull().all():
            # Find the model with the highest performance and its value for "With extra training data"
            max_with_extra_training[year] = (
                year,
                year_data.loc[year_data["With extra training data"].idxmax(), "name"],
                year_data.loc[year_data["With extra training data"].idxmax(), "With extra training data"],
            )

    # Create a new DataFrame to store the models with the highest performance and their values for each year
    result_df_with = pd.DataFrame(
        max_without_extra_training.values(), columns=["year", "name", "Without extra training data"]
    )

    # Create a new DataFrame to store the models with the highest performance and their values for each year
    result_df_without = pd.DataFrame(
        max_with_extra_training.values(), columns=["year", "name", "With extra training data"]
    )
    merged_df = pd.merge(result_df_with, result_df_without, on=["year", "name"], how="outer")

    return merged_df


def combine_with_without(df):
    df["name"] = df["name"].astype(str)
    # Add a star at the end of the "name" column where "without_extra_training_data" is NaN
    df.loc[df["Without extra training data"].isnull(), "name"] += "*"
    df["name"] = df["name"].str.replace(" ", "")

    # Create a copy of the DataFrame
    result_df = df.copy()

    # Combine "without_extra_training_data" and "with_extra_training_data" into one column
    result_df["performance_data"] = result_df["Without extra training data"].fillna(
        result_df["With extra training data"]
    )

    # Create a new column indicating whether it's training data or not
    result_df["training_data"] = (
        result_df["Without extra training data"]
        .notnull()
        .map({True: "With extra training data", False: "Without extra training data"})
    )

    # Remove the "without_extra_training_data" and "with_extra_training_data" columns
    result_df = result_df.drop(columns=["Without extra training data", "With extra training data"])

    return result_df
