"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def drop_all_rows_of_notes(tb: Table) -> Table:
    # Identify rows that start with "Notes" in the first column.
    notes_index = tb[tb.iloc[:, 0].str.startswith("Notes").fillna(False)].index
    # Drop all rows after that.
    if len(notes_index) >= 1:
        tb = tb[: notes_index[0]].reset_index(drop=True)

    return tb


def separate_minerals_and_technology(tb: Table) -> Table:
    tb = tb.copy()
    # Now, the first column contains the name of the mineral (with no data in other columns) followed by the technology where the mineral is relevant.
    # Create a new column for the mineral.
    tb["mineral"] = None
    # Identify the rows for each mineral (which are the ones for which all other columns are empty).
    mineral_mask = (tb["technology"].notnull()) & (tb[tb.columns[1]].isnull())
    tb.loc[mineral_mask, "mineral"] = tb[mineral_mask]["technology"]
    tb["mineral"] = tb["mineral"].ffill()
    # Remove the rows where the minerals were.
    tb = tb[~mineral_mask].reset_index(drop=True)

    return tb


def process_demand_for_key_minerals(data: pr.ExcelFile) -> Table:
    sheet_name = "1 Total demand for key minerals"
    tb = data.parse(sheet_name)

    scenarios = {
        "": "",
        "Stated Policies scenario": "stated_policies",
        "Announced Pledges scenario": "announced_pledges",
        "Net Zero Emissions by 2050 scenario": "net_zero_by_2050",
    }
    scenarios_columns = tb.iloc[2].ffill().fillna("").values
    assert set(scenarios) == set(scenarios_columns), f"Format has changed for sheet: {sheet_name}"

    # Define new columns.
    new_columns = (
        tb.iloc[2].ffill().fillna("").replace(scenarios).values
        + "__"
        + tb.iloc[3].astype("Int64").astype(object).fillna("").astype(str).values
    )
    new_columns[0] = "technology"
    new_columns[1] = f"current{new_columns[1]}"
    tb = tb.rename(columns={tb.columns[i]: column for i, column in enumerate(new_columns)}, errors="raise")

    # By construction, all columns that end in "__" do not correspond to any data, so drop them.
    tb = tb.drop(columns=[column for column in tb.columns if column.endswith("__")])

    # Drop initial rows that corresponded to titles.
    tb = tb.drop([0, 1, 2, 3, 4]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all").dropna(how="all").reset_index(drop=True)

    # Drop rows containing notes at the end of the sheet.
    tb = drop_all_rows_of_notes(tb=tb)

    # Separate minerals and technology in different columns.
    tb = separate_minerals_and_technology(tb=tb)

    # Transform table to end up with another table that has a "year" and "scenario" columns.
    scenarios_names = ["current"] + [scenario for scenario in scenarios.values() if scenario != ""]
    tables = []
    for scenario in scenarios_names:
        _tb = tb[["mineral", "technology"] + [column for column in tb.columns if column.startswith(scenario)]]
        _tb = _tb.rename(columns={column: column.replace(f"{scenario}__", "") for column in _tb.columns})
        _tb = _tb.melt(id_vars=["mineral", "technology"], value_name="demand", var_name="year")
        _tb = _tb.assign(**{"scenario": scenario})
        tables.append(_tb)
    tb = pr.concat(tables, short_name="demand_for_key_minerals")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["mineral", "technology", "year", "scenario"])

    return tb


def process_supply_for_key_minerals(data: pr.ExcelFile) -> Table:
    sheet_name = "2 Total supply for key minerals"
    # Read the sheet, and let all columns be "Unnamed: X" (with X from 0 to the number of columns).
    tb = data.parse(sheet_name)

    # In this sheet there are two tables separated by an empty column.
    # Identify the separating column.
    column_position = int(tb.columns[tb.isnull().all()][0].split(" ")[-1])

    def _prepare_sub_table(_tb: Table) -> Table:
        _tb = _tb.copy()
        new_columns = _tb.iloc[2].astype("Int64").astype(object).fillna("").astype(str).values
        new_columns[0] = "technology"
        _tb = _tb.rename(columns={_tb.columns[i]: column for i, column in enumerate(new_columns)}, errors="raise")
        _tb = _tb.drop([0, 1, 2, 3]).reset_index(drop=True)
        _tb = drop_all_rows_of_notes(tb=_tb)

        # Drop empty columns and rows.
        _tb = _tb.dropna(axis=1, how="all").dropna(how="all").reset_index(drop=True)

        return _tb

    _tb_1 = _prepare_sub_table(tb[tb.columns[:column_position]])
    _tb_2 = _prepare_sub_table(tb[tb.columns[column_position + 1 :]])
    tb = pr.concat([_tb_1, _tb_2])

    # All predictions seems to be of the "Base case" scenario.
    tb["scenario"] = "base_case"

    # Separate minerals and technology in different columns.
    tb = separate_minerals_and_technology(tb=tb)

    # Reformat table to have "year" and "supply" columns.
    tb = tb.melt(id_vars=["mineral", "technology", "scenario"], value_name="supply", var_name="year")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["mineral", "technology", "year", "scenario"])

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("critical_minerals.xlsx")

    # Load data from snapshot.
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Process each table.
    tb_demand_for_key_minerals = process_demand_for_key_minerals(data=data)
    tb_supply = process_supply_for_key_minerals(data=data)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_demand_for_key_minerals, tb_supply],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
