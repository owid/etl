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


def removing_old_variables(tb: Table, dd: Table, dataset_name: str) -> Table:
    """
    There are several variables in this dataset that are recorded as no longer being used after 2011.

    We will remove these as they will be of limited use to us.
    """
    dd = dd[dd["dataset"] == dataset_name]
    cols_to_drop = dd["variable_name"][dd["definition"].str.contains("not used after ")].to_list()
    # Removing any columns that are in data dictionary but not in the dataset
    cols_to_drop = [col for col in cols_to_drop if col in tb.columns]
    tb = tb.drop(columns=cols_to_drop)

    return tb
