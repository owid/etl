from owid.catalog import Table

from etl.snapshot import Snapshot


def add_variable_description_from_producer(tb: Table, dd: Snapshot) -> Table:
    """Add variable description from the data dictionary to each variable."""
    columns = tb.columns.difference(["country", "year"])
    for col in columns:
        tb[col].metadata.description_from_producer = dd.loc[dd.variable_name == col, "definition"].values[0]
    return tb
