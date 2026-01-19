# Stub file for backwards compatibility - re-exports from core/tables.py
# New code should import from owid.catalog.core.tables
from owid.catalog.core.tables import (
    # Constants
    METADATA_FIELDS,
    SCHEMA,
    # Type variables
    AnyStr,
    # Helper classes
    ExcelFile,
    SeriesOrVariable,
    # Main class
    Table,
    TableGroupBy,
    TableRolling,
    TableRollingGroupBy,
    VariableGroupBy,
    # Core functions
    align_categoricals,
    check_all_variables_have_metadata,
    combine_tables_datasetmeta,
    combine_tables_description,
    combine_tables_metadata,
    combine_tables_title,
    combine_tables_update_period_days,
    concat,
    copy_metadata,
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
    keep_metadata,
    # Logger
    log,
    melt,
    merge,
    multi_merge,
    pivot,
    # Read functions
    read_csv,
    read_custom,
    read_df,
    read_excel,
    read_feather,
    read_from_df,
    read_from_dict,
    read_from_records,
    read_fwf,
    read_json,
    read_parquet,
    read_rda,
    read_rda_multiple,
    read_rds,
    read_stata,
    # Conversion functions
    to_datetime,
    to_numeric,
    # Processing log functions
    update_processing_logs_when_loading_or_creating_table,
    update_processing_logs_when_saving_table,
    # Variable dimension function
    update_variable_dimensions,
)

__all__ = [
    # Main class
    "Table",
    # Constants
    "METADATA_FIELDS",
    "SCHEMA",
    # Type variables
    "AnyStr",
    "SeriesOrVariable",
    # Logger
    "log",
    # Helper classes
    "ExcelFile",
    "TableGroupBy",
    "TableRolling",
    "TableRollingGroupBy",
    "VariableGroupBy",
    # Core functions
    "align_categoricals",
    "check_all_variables_have_metadata",
    "combine_tables_datasetmeta",
    "combine_tables_description",
    "combine_tables_metadata",
    "combine_tables_title",
    "combine_tables_update_period_days",
    "concat",
    "copy_metadata",
    "get_unique_licenses_from_tables",
    "get_unique_sources_from_tables",
    "keep_metadata",
    "melt",
    "merge",
    "multi_merge",
    "pivot",
    # Read functions
    "read_csv",
    "read_custom",
    "read_df",
    "read_excel",
    "read_feather",
    "read_from_df",
    "read_from_dict",
    "read_from_records",
    "read_fwf",
    "read_json",
    "read_parquet",
    "read_rda",
    "read_rda_multiple",
    "read_rds",
    "read_stata",
    # Conversion functions
    "to_datetime",
    "to_numeric",
    # Processing log functions
    "update_processing_logs_when_loading_or_creating_table",
    "update_processing_logs_when_saving_table",
    # Variable dimension function
    "update_variable_dimensions",
]
