from typing import Dict

from owid.catalog import Table


def harmonize_dimension(tb: Table, column_name: str, mapping: Dict[str, str]) -> Table:
    """Harmonize a dimension in a table using a mapping.

    tb: Table to harmonize.
    column_name: Column name to harmonize.
    mapping: Mapping to harmonize the column.
    """
    # Assert column_name does not contain any other column but those in mapping
    assert set(tb[column_name].unique()) == set(mapping.keys())

    # Replace values in column_name
    tb[column_name] = tb[column_name].replace(mapping)

    return tb
