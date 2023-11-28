"""DataFrame io operations."""
import inspect
from pathlib import Path
from typing import Any, List, Optional, Union

import pandas as pd

from owid.datautils.decorators import enable_file_download

COMPRESSION_SUPPORTED = ["gz", "bz2", "zip", "xz", "zst", "tar"]


def _has_index(df: pd.DataFrame) -> bool:
    # Copy of dataframes.has_index to avoid circular imports.
    df_has_index = True if df.index.names[0] is not None else False

    return df_has_index


@enable_file_download("file_path")
def from_file(
    file_path: Union[str, Path], file_type: Optional[str] = None, **kwargs: Any
) -> Union[pd.DataFrame, List[pd.DataFrame]]:
    """Load a file as a pandas DataFrame.

    It uses standard pandas function pandas.read_* but adds the ability to read from a URL in some
    cases where pandas does not work.

    The function will infer the extension of `file_path` by simply taking what follows the last ".". For example:
    "file.csv" will be read as a csv file, and "http://my/domain/file.xlsx" will be read as an excel file.

    Reading from compressed files will not work by default, unless you provide a `file_type`.

    Parameters
    ----------
    filepath : str
        Path or url to file.
    file_type : str
        File type of the data file. By default is None, as it is only required when reading compressed files.
        This is typically equivalent to the file extension. However, when reading a
        compressed file, this refers to the actual file that is compressed (not the compressed file extension).
        Reading from compressed files is supported for "csv", "dta" and "json".
    kwargs :
        pandas.read_* arguments.

    Returns
    -------
    pandas.DataFrame:
        Read dataframe.
    """
    # Ensure file_path is a Path object.
    file_path = Path(file_path)

    # Ensure extension is lower case and does not start with '.'.
    extension = file_path.suffix.lstrip(".").lower()

    # If compressed file, raise an exception unless file_type is given
    if extension in COMPRESSION_SUPPORTED:
        if file_type:
            extension = file_type
        else:
            raise ValueError(
                "To be able to read from a compressed file, you need to provide a value" " for `file_type`."
            )

    # Check path is valid
    if not file_path.exists():
        raise FileNotFoundError(f"Cannot find file: {file_path}")

    # Available input methods (some of them may need additional dependencies to work).
    input_methods = {
        "csv": pd.read_csv,
        "dta": pd.read_stata,
        "feather": pd.read_feather,
        "hdf": pd.read_hdf,
        "html": pd.read_html,
        "json": pd.read_json,
        "parquet": pd.read_parquet,
        "pickle": pd.read_pickle,
        "pkl": pd.read_pickle,
        "xlsx": pd.read_excel,
        "xml": pd.read_xml,
    }
    if extension not in input_methods:
        raise ValueError("Failed reading dataframe because of an unknown file extension:" f" {extension}")
    # Select the appropriate reading method.
    read_function = input_methods[extension]

    # Load file using the chosen read function and the appropriate arguments.
    df: pd.DataFrame = read_function(file_path, **kwargs)
    return df


def to_file(df: pd.DataFrame, file_path: Union[str, Path], overwrite: bool = True, **kwargs: Any) -> None:
    """Save dataframe to file.

    This function wraps all pandas df.to_* methods, e.g. df.to_csv() or df.to_parquet(), with the following advantages:
    * The output file will have the format determined by the extension of file_path. Hence, to_file(df, "data.csv") will
    create a csv file, and to_file(df, "data.parquet") will create a parquet file.
    * If file_path is with one or more subfolders that do not exist, the full path will be created.
    * It can overwrite an existing file (if overwrite is True), or raise an error if the file already exists.
    * It will avoid creating an index column if the dataframe has a dummy index (which would be equivalent to doing
    df.to_csv(file_path, index=False)), but it will include the index if the dataframe has one.
    * Any additional keyword argument that would be passed on to the method to write a file can be safely added. For
    example, to_file(df, "data.csv", na_rep="TEST") will replace missing data by "TEST" (analogous to
    df.to_csv("data.csv", na_rep="TEST")).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to be stored in a file.
    file_path : Union[str, Path]
        Path to file to be created.
    overwrite : bool, optional
        True to overwrite file if it already exists. False to raise an error if file already exists.

    """
    # Ensure file_path is a Path object.
    file_path = Path(file_path)

    # Ensure extension is lower case and does not start with '.'.
    extension = file_path.suffix.lstrip(".").lower()

    # Ensure output directory exists.
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True)

    # Avoid overwriting an existing file unless explicitly stated.
    if file_path.is_file() and not overwrite:
        raise FileExistsError("Failed to save dataframe because file exists and 'overwrite' is False.")

    # Available output methods (some of them may need additional dependencies to work).
    output_methods = {
        "csv": df.to_csv,
        "dta": df.to_stata,
        "feather": df.to_feather,
        "hdf": df.to_hdf,
        "html": df.to_html,
        "json": df.to_json,
        "md": df.to_markdown,
        "parquet": df.to_parquet,
        "pickle": df.to_pickle,
        "pkl": df.to_pickle,
        "tex": df.to_latex,
        "txt": df.to_string,
        "xlsx": df.to_excel,
        "xml": df.to_xml,
    }
    if extension not in output_methods:
        raise ValueError(f"Failed saving dataframe because of an unknown file extension: {extension}")
    # Select the appropriate storing method.
    save_function = output_methods[extension]

    # Decide whether dataframe should be stored with or without an index, if:
    # * The storing method allows for an 'index' argument.
    # * The argument "index" is not explicitly given.
    if ("index" in inspect.signature(save_function).parameters) and ("index" not in kwargs):
        # Make 'index' False to avoid storing index if dataframe has a dummy index.
        kwargs["index"] = _has_index(df=df)

    # Save file using the chosen save function and the appropriate arguments.
    save_function(file_path, **kwargs)
