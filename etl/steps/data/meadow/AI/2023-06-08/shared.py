import pandas as pd

from etl.snapshot import Snapshot


def load_data(snap: Snapshot):
    """
    Load the Excel file from the given snapshot.

    Args:
        snap (Snapshot): The snapshot object containing the path to the Excel file.

    Returns:
        pd.ExcelFile: The loaded Excel file as a pandas ExcelFile object, or None if loading failed.
    """

    # Attempt to load the Excel file from the snapshot path.
    try:
        excel_object = pd.ExcelFile(snap.path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Excel file not found at path: {snap.path}")
    except IsADirectoryError:
        raise IsADirectoryError(f"Provided path is a directory, not an Excel file: {snap.path}")
    except Exception as e:
        raise Exception(f"An error occurred while loading the Excel file: {e}")

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object


def process_data(excel_object: pd.ExcelFile):
    """
    Process sheets in the given Excel file and return a combined DataFrame.

    Args:
        excel_object (pd.ExcelFile): The loaded Excel file to process.
    Returns:
        pd.DataFrame: The combined and processed DataFrame from all sheets.
    """
    sheet_names = excel_object.sheet_names
    data_frames = []
    # Iterate through the matched sheet names and process each sheet
    for i, sheet_name in enumerate(sheet_names):
        print(f"Processing sheet: {sheet_name}")
        df = pd.read_excel(excel_object, sheet_name=sheet_name)
        question = df.columns[0]  # Extract the question that was asked
        melted_df = df.melt(
            id_vars=question, var_name="Year", value_name="Value"
        )  # Melt date columns into one called 'year'
        filtered_df = melted_df[~melted_df[question].isin(["Unweighted base", "Base"])]  # Exclude sample sizes
        filtered_df = filtered_df.assign(Value=filtered_df["Value"] * 100, Group=sheet_name)  # Convert to percentages

        data_frames.append(filtered_df)

    # Concatenate all the processed DataFrames
    df_concat = pd.concat(data_frames, axis=0)
    df_concat.reset_index(drop=True, inplace=True)
    return df_concat
