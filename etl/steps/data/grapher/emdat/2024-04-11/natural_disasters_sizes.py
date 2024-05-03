from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def prepare_tables(table: Table, is_decade: bool) -> Table:
    # Create wide tables.
    table_wide = table.reset_index()

    if is_decade:
        variable_name_suffix = "_decadal"
        variable_title_suffix = " (decadal)"
    else:
        variable_name_suffix = "_yearly"
        variable_title_suffix = ""

    # Improve variable names and titles.
    for column in table_wide.drop(columns=["country", "year"], errors="raise").columns:
        table_wide[column].metadata.title += variable_title_suffix
        table_wide = table_wide.rename(columns={column: column + variable_name_suffix}, errors="raise")

    # Set an appropriate index and sort conveniently.
    table_wide = table_wide.format()

    return table_wide


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden tables and remove unnecessary columns.
    ds_garden = paths.load_dataset("natural_disasters")
    tb_yearly = ds_garden["natural_disasters_yearly_sizes"]
    tb_decadal = ds_garden["natural_disasters_decadal_sizes"]

    #
    # Process data.
    #
    # Create wide tables.
    tb_yearly_wide = prepare_tables(table=tb_yearly, is_decade=False)
    tb_decadal_wide = prepare_tables(table=tb_decadal, is_decade=True)

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_yearly_wide, tb_decadal_wide],
        default_metadata=ds_garden.metadata,
        check_variables_metadata=True,
    )

    ds_grapher.metadata.title = "Natural disasters by impact"
    ds_grapher.save()
