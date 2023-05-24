import pandas as pd
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()


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
        raise Exception(f"Unknown error occurred: {e}")

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object
