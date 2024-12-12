"""Input/Output methods."""

from owid.datautils.io.archive import decompress_file
from owid.datautils.io.df import from_file as df_from_file
from owid.datautils.io.df import to_file as df_to_file
from owid.datautils.io.json import load_json, save_json

__all__ = [
    "decompress_file",
    "load_json",
    "save_json",
    "df_from_file",
    "df_to_file",
]
