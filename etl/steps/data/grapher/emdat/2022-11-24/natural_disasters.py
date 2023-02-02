from copy import deepcopy

from owid import catalog
from shared import CURRENT_DIR, DISASTER_TYPE_RENAMING

from etl.helpers import PathFinder

N = PathFinder(str(CURRENT_DIR / "natural_disasters"))


def create_wide_tables(table: catalog.Table, is_decade: bool) -> catalog.Table:
    # Create wide dataframes.
    table_wide = table.reset_index().pivot(index=["country", "year"], columns="type")

    if is_decade:
        variable_name_suffix = "_decadal"
        variable_title_suffix = " (decadal)"
    else:
        variable_name_suffix = "_yearly"
        variable_title_suffix = ""

    # Store metadata of original table variables.
    variable_metadata = {}
    for column, subcolumn in table_wide.columns:
        old_metadata = deepcopy(table[column].metadata)
        new_variable = f"{column}_{subcolumn}" + variable_name_suffix
        new_title = f"{old_metadata.title} - {DISASTER_TYPE_RENAMING[subcolumn]}" + variable_title_suffix
        old_metadata.title = new_title
        variable_metadata[new_variable] = old_metadata

    # Flatten column indexes.
    table_wide.columns = [f"{column}_{subcolumn}" + variable_name_suffix for column, subcolumn in table_wide.columns]

    # Assign original variables metadata to new variables in wide table.
    for variable in variable_metadata:
        table_wide[variable].metadata = variable_metadata[variable]

    return table_wide


def run(dest_dir: str) -> None:
    # Load garden tables and remove unnecessary columns.
    table_yearly = N.garden_dataset["natural_disasters_yearly"].drop(columns=["population", "gdp"])
    table_decade = N.garden_dataset["natural_disasters_decadal"].drop(columns=["population", "gdp"])

    # Create wide tables.
    table_yearly_wide = create_wide_tables(table=table_yearly, is_decade=False)
    table_decade_wide = create_wide_tables(table=table_decade, is_decade=True)

    # Create new grapher dataset, add tables, and save dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.add(table_yearly_wide)
    dataset.add(table_decade_wide)
    dataset.save()
