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
    """Load a file as a pandas DataFrame with URL and compression support.

    Enhanced wrapper around pandas `read_*` functions that adds:
    - Automatic format detection from file extension
    - URL download support (via `@enable_file_download` decorator)
    - Compressed file reading (with explicit `file_type`)

    The function infers the file type from the extension after the last dot.
    For example: "file.csv" reads as CSV, "https://example.com/data.xlsx" reads as Excel.

    Args:
        file_path: Local path or URL to the file. Supports local files and HTTP(S) URLs.
        file_type: Explicit file type when reading compressed files (e.g., "csv", "dta", "json").
            Only needed for compressed files. Specifies the format of the compressed content,
            not the compression format itself.
        **kwargs: Additional arguments passed to the underlying `pandas.read_*` function.

    Returns:
        DataFrame loaded from the file. Some formats (like HTML) may return a list of DataFrames.

    Raises:
        ValueError: If file extension is unknown or `file_type` not provided for compressed files.
        FileNotFoundError: If the file path doesn't exist.

    Example:
        Load from local file
        ```python
        from owid.datautils.io.df import from_file

        # CSV file
        df = from_file("data.csv")

        # Excel with specific sheet
        df = from_file("data.xlsx", sheet_name="Sheet1")
        ```

        Load from URL
        ```python
        # HTTP URL (handled automatically by decorator)
        df = from_file("https://example.com/data.csv")
        ```

        Load compressed file
        ```python
        # Compressed CSV (must specify file_type)
        df = from_file("data.csv.gz", file_type="csv")
        ```

    Note:
        Supported formats: csv, dta, feather, hdf, html, json, parquet, pickle, xlsx, xml.
        Compression formats: gz, bz2, zip, xz, zst, tar (with explicit `file_type`).
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
    df: pd.DataFrame = read_function(file_path, **kwargs)  # type: ignore
    return df


def to_file(df: pd.DataFrame, file_path: Union[str, Path], overwrite: bool = True, **kwargs: Any) -> None:
    """Save DataFrame to file with automatic format detection and smart defaults.

    Enhanced wrapper around pandas `to_*` methods that provides:

    - Automatic format selection from file extension
    - Auto-creation of parent directories
    - Intelligent index handling (omits dummy indices)
    - Optional overwrite protection

    Format is determined by file extension: "data.csv" creates CSV,
    "data.parquet" creates Parquet, etc.

    Args:
        df: DataFrame to save.
        file_path: Output file path. Parent directories are created if needed.
        overwrite: If True, overwrite existing files. If False, raise error
            if file already exists.
        **kwargs: Additional arguments passed to the underlying `pandas.to_*` method
            (e.g., `na_rep`, `sep`, `compression`).

    Raises:
        ValueError: If file extension is not supported.
        FileExistsError: If file exists and `overwrite=False`.

    Example:
        Basic usage
        ```python
        from owid.datautils.io.df import to_file
        import pandas as pd

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        # Save as CSV
        to_file(df, "output.csv")

        # Save as Parquet
        to_file(df, "output.parquet")

        # Save with custom parameters
        to_file(df, "output.csv", na_rep="N/A", sep=";")
        ```

        Auto-create directories
        ```python
        # Creates nested/path/ if it doesn't exist
        to_file(df, "nested/path/data.csv")
        ```

        Overwrite protection
        ```python
        # Raises FileExistsError if file exists
        to_file(df, "existing.csv", overwrite=False)
        ```

    Note:
        Supported formats: csv, dta, feather, hdf, html, json, md, parquet,
        pickle, tex, txt, xlsx, xml.

        Index handling: Automatically omits dummy indices (default integer index)
        but preserves meaningful indices. Override with `index=True/False` in kwargs.
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
