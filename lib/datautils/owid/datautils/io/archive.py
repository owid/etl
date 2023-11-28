"""Input/Output functions for local files."""

import tarfile
import zipfile
from pathlib import Path
from typing import Union

from owid.datautils.decorators import enable_file_download


@enable_file_download(path_arg_name="input_file")
def decompress_file(
    input_file: Union[str, Path],
    output_folder: Union[str, Path],
    overwrite: bool = False,
) -> None:
    """Extract a zip or tar file.

    It can be a local or a remote file.

    Parameters
    ----------
    input_file : Union[str, Path]
        Path to local zip file, or URL of a remote zip file.
    output_folder : Union[str, Path]
        Path to local folder.
    overwrite : bool
        Overwrite decompressed content if it already exists (otherwise raises an error if content already exists).

    """
    if zipfile.is_zipfile(input_file):
        _decompress_zip_file(input_file, output_folder, overwrite)
    elif tarfile.is_tarfile(input_file):
        _decompress_tar_file(input_file, output_folder, overwrite)
    else:
        raise ValueError("File is neither a zip nor a tar file.")


def _decompress_zip_file(
    input_file: Union[str, Path],
    output_folder: Union[str, Path],
    overwrite: bool = False,
) -> None:
    """Unpack zip file."""
    zip_file = zipfile.ZipFile(input_file)

    # If the content to be written in output folder already exists, raise an error,
    # unless 'overwrite' is set to True, in which case the existing file will be overwritten.
    # Path to new file to be created.
    new_file = Path(output_folder) / Path(zip_file.namelist()[0])
    if new_file.exists() and not overwrite:
        raise FileExistsError("Output already exists. Either change output_folder or use overwrite=True.")

    # Unzip the file and save it in the local output folder.
    # Note that, if output_folder path does not exist, the following command will create it.
    zip_file.extractall(output_folder)


def _decompress_tar_file(
    input_file: Union[str, Path],
    output_folder: Union[str, Path],
    overwrite: bool = False,
) -> None:
    """Unpack tar file."""
    with tarfile.open(input_file) as tar_file:
        # If the content to be written in output folder already exists, raise an error,
        # unless 'overwrite' is set to True, in which case the existing file will be overwritten.
        # Path to new file to be created.
        new_file = Path(output_folder) / Path(tar_file.getnames()[0])
        if new_file.exists() and not overwrite:
            raise FileExistsError("Output already exists. Either change output_folder or use" " overwrite=True.")

        # Unzip the file and save it in the local output folder.
        # Note that, if output_folder path does not exist, the following command will create it.
        tar_file.extractall(output_folder)
