from owid.catalog import Table


def add_variable_description_from_producer(tb: Table, dd: Table) -> Table:
    """Add variable description from the data dictionary to each variable.
    Some descriptions reference other variable codes (strings including "_") so we won't use these ones.
    """
    columns = tb.columns.difference(["country", "year"])
    for col in columns:
        description_from_producer = dd.loc[dd.variable_name == col, "definition"].values[0]
        if "_" in description_from_producer:
            tb[col].metadata.description_from_producer = ""
        else:
            tb[col].metadata.description_from_producer = description_from_producer
    return tb
