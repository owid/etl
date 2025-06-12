"""Tools for an interactive (ipython) session."""

from typing import List, Optional

import yaml
from owid.catalog import Table

from etl.harmonize import harmonize_ipython

__all__ = ["harmonize_ipython"]


def print_tables_metadata_template(tables: List[Table], fields: Optional[List[str]] = None) -> None:
    # This function is meant to be used when creating code in an interactive window (or a notebook).
    # It prints a template for the metadata of the tables in the list.
    # The template can be copied and pasted into the corresponding yaml file.
    # In the future, we should have an interactive tool to add or edit the content of the metadata yaml files, using
    # AI-generated texts when possible.

    if fields is None:
        fields = ["title", "unit", "short_unit", "description_short"]

    # Initialize output dictionary.
    dict_tables = {}
    for tb in tables:
        dict_variables = {}
        for column in tb.columns:
            dict_values = {}
            for field in fields:
                if field.startswith("presentation"):
                    field = field.replace("presentation.", "")
                    value = getattr(tb[column].metadata.presentation, field) or ""
                    if "presentation" not in dict_values:
                        dict_values["presentation"] = {}
                    dict_values["presentation"][field] = value
                else:
                    value = getattr(tb[column].metadata, field) or ""

                    # Add some simple rules to simplify some common cases.

                    # If title is empty, or if title is underscore (probably because it is taken from the column name),
                    # create a custom title.
                    if (field == "title") and ((value == "") or ("_" in value)):
                        value = column.capitalize().replace("_", " ")

                    # If unit or short_unit is empty, and the column name contains 'pct', set it to '%'.
                    if (value == "") and (field in ["unit", "short_unit"]) and "pct" in column:
                        value = "%"

                    if field == "processing_level":
                        # Assume a minor processing level (it will be manually overwritten, if needed).
                        value = "minor"

                    dict_values[field] = value
            dict_variables[column] = dict_values
        dict_tables[tb.metadata.short_name] = {"variables": dict_variables}
    dict_output = {"tables": dict_tables}

    # print(yaml.dump(dict_output, default_flow_style=False, sort_keys=False))
    print(yaml.dump(dict_output, default_flow_style=False, sort_keys=False, width=float("inf")))
