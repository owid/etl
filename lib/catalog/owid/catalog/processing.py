"""Common operations performed on tables and variables.

"""
from .tables import (
    ExcelFile,
    concat,
    melt,
    merge,
    pivot,
    read_csv,
    read_excel,
    read_from_dict,
    read_from_records,
    read_json,
)

__all__ = [
    "ExcelFile",
    "concat",
    "melt",
    "merge",
    "pivot",
    "read_csv",
    "read_excel",
    "read_from_dict",
    "read_from_records",
    "read_json",
]
