"""Common operations performed on tables and variables.

"""
from .tables import (
    ExcelFile,
    concat,
    melt,
    merge,
    multi_merge,
    pivot,
    read_csv,
    read_excel,
    read_feather,
    read_from_dict,
    read_from_records,
    read_fwf,
    read_json,
    read_rda,
    read_rds,
    read_stata,
)

__all__ = [
    "ExcelFile",
    "concat",
    "melt",
    "merge",
    "multi_merge",
    "pivot",
    "read_csv",
    "read_feather",
    "read_excel",
    "read_from_dict",
    "read_from_records",
    "read_json",
    "read_fwf",
    "read_stata",
    "read_rda",
    "read_rds",
]
