"""Tools to ingest to Walden and Catalog."""

import tempfile
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from owid.datautils import dataframes

from owid.walden import files

from .catalog import Dataset
from .ui import log


def add_to_catalog(
    metadata: Union[dict, Dataset],
    filename: Optional[Union[str, Path]] = None,
    dataframe: Optional[pd.DataFrame] = None,
    upload: bool = False,
    public: bool = True,
) -> None:
    """Add dataset with metadata to catalog, where the data is either a local file, or a dataframe in memory.

    Additionally, it computes the md5 hash of the file, which is added to the metadata file.

    TODO: Add checks of fields.

    Args:
        metadata (dict): Dictionary with metadata.
        filename (str or None): Path to local data file (if dataframe is not given).
        dataframe (pd.DataFrame or None): Dataframe to upload (if filename is not given).
        upload (bool): True to upload data to Walden bucket.
        public (bool): True to make file public.
    """
    if (filename is not None) and (dataframe is None):
        # checksum happens in here, copy to cache happens here
        dataset = Dataset.copy_and_create(str(filename), metadata)

        if upload:
            # add it to our DigitalOcean Space and set `owid_cache_url`
            dataset.upload(public=public)

        # save the JSON to the local index
        dataset.save()
        log("ADDED TO CATALOG", f"{dataset.relative_base}.json")
    elif (dataframe is not None) and (filename is None):
        # Get output file extension from metadata.
        if isinstance(metadata, dict):
            file_extension = metadata["file_extension"]  # type: ignore
        else:
            file_extension = metadata.file_extension  # type: ignore

        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            # Use the extension specified in the metadata, so that the file is stored in the right format.
            temp_file = Path(temp_dir) / f"temp.{file_extension}"
            dataframes.to_file(dataframe, file_path=temp_file)
            # Add file checksum to metadata.
            metadata.md5 = files.checksum(temp_file)  # type: ignore
            # Run the function again, but now fetching the data from the temporary file instead of the dataframe.
            # This time the function will create the walden index file and upload to s3 (if upload is True).
            add_to_catalog(metadata=metadata, filename=temp_file, upload=upload, public=public)
    else:
        raise ValueError("Use either 'filename' or 'dataframe' argument, but not both.")
