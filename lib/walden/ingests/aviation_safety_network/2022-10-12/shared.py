import tempfile
from pathlib import Path

import pandas as pd

from owid.walden import Dataset, add_to_catalog, files

CURRENT_DIR = Path(__file__).parent


def add_dataframe_with_metadata_to_catalog(df: pd.DataFrame, metadata: Dataset, upload: bool) -> None:
    """Add a dataframe with metadata to Walden catalog as a csv file, and create the corresponding Walden index file.

    Notes:
     * This function stores a csv file, and hence the 'file_extension' field in the metadata file should be 'csv'.
     * The dataframe is expected to have a dummy index.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to be uploaded.
    metadata : Dataset
        Content of the Walden metadata yaml file (with, for example, the extension of the data file to be created).
    upload : bool
        True to upload data to Walden bucket.

    """
    # Store dataframe in a temporary file.
    with tempfile.NamedTemporaryFile() as _temp_file:
        # Save data in a temporary file.
        df.to_csv(_temp_file.name, index=False)
        # Add file checksum to metadata.
        metadata.md5 = files.checksum(_temp_file.name)
        # Create walden index file and upload to s3 (if upload is True).
        add_to_catalog(metadata, _temp_file.name, upload=upload)
