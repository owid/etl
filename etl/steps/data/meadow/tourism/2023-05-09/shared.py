import pandas as pd
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()


def load_data(snap: Snapshot, sheet_name_to_load: str):
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

    if sheet_name_to_load not in excel_object.sheet_names:
        # Raise an exception if the desired sheet is not found
        raise Exception("Sheet not found in the Excel file.")

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object
