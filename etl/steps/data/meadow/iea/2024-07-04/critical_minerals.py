"""Load data from the individual sheets of IEA's Critical Minerals spreadsheet.

Sheets have different formats, but there are some common elements.

There are four scenarios adopted for projections:
* Stated Policies.
* Announced Pledges.
* Net Zero Emissions by 2050.

Additionally, we assign a scenario "Current" to data that is not based on projections.

Some of the sheets also contain "cases", which are things like "Base case" (the default), "Constrained rare earth elements supply", "Wider use of silicon-rich anodes"...

"""

from typing import Optional

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


def transform_header_rows_into_new_column(tb: Table, header_row_name: str, normal_row_name: str) -> Table:
    tb = tb.copy()
    # Now, the first column contains the name of the mineral (with no data in other columns) followed by the technology where the mineral is relevant.
    # Create a new column for the mineral.
    tb[header_row_name] = None
    # Identify the rows for each mineral (which are the ones for which all other columns are empty).
    mineral_mask = (tb[normal_row_name].notnull()) & (tb[tb.columns[1]].isnull())
    tb.loc[mineral_mask, header_row_name] = tb[mineral_mask][normal_row_name]
    tb[header_row_name] = tb[header_row_name].ffill()
    # Remove the rows where the minerals were.
    tb = tb[~mineral_mask].reset_index(drop=True)

    return tb


def process_demand_for_key_minerals(
    data: pr.ExcelFile, sheet_name: str, header_row_name: Optional[str], normal_row_name: str, tb_short_name: str
) -> Table:
    tb = data.parse(sheet_name)

    scenarios = {
        "": "",
        "Stated Policies scenario": "Stated policies",
        "Announced Pledges scenario": "Announced pledges",
        "Net Zero Emissions by 2050 scenario": "Net zero by 2050",
    }
    scenarios_columns = tb.iloc[2].ffill().fillna("").values
    assert set(scenarios) == set(scenarios_columns), f"Format has changed for sheet: {sheet_name}"

    # Define new columns.
    new_columns = (
        tb.iloc[2].ffill().fillna("").replace(scenarios).values
        + "__"
        + tb.iloc[3].astype("Int64").astype(object).fillna("").astype(str).values
    )
    new_columns[0] = normal_row_name
    new_columns[1] = f"Current{new_columns[1]}"
    tb = tb.rename(columns={tb.columns[i]: column for i, column in enumerate(new_columns)}, errors="raise")

    # By construction, all columns that end in "__" do not correspond to any data, so drop them.
    tb = tb.drop(columns=[column for column in tb.columns if column.endswith("__")])

    # Drop initial rows that corresponded to titles.
    assert tb.loc[4].isnull().all(), "Sheet format may have changed."
    tb = tb.drop([0, 1, 2, 3, 4]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all").dropna(how="all").reset_index(drop=True)

    # Drop rows containing notes at the end of the sheet.
    tb = drop_all_rows_of_notes(tb=tb)

    if header_row_name is not None:
        # Separate minerals and technology in different columns.
        tb = transform_header_rows_into_new_column(
            tb=tb, header_row_name=header_row_name, normal_row_name=normal_row_name
        )
        main_index_columns = [header_row_name, normal_row_name]
    else:
        main_index_columns = [normal_row_name]

    # Transform table to end up with another table that has a "year" and "scenario" columns.
    scenarios_names = ["Current"] + [scenario for scenario in scenarios.values() if scenario != ""]
    tables = []
    for scenario in scenarios_names:
        _tb = tb[main_index_columns + [column for column in tb.columns if column.startswith(scenario)]]
        _tb = _tb.rename(columns={column: column.replace(f"{scenario}__", "") for column in _tb.columns})
        _tb = _tb.melt(id_vars=main_index_columns, value_name="demand", var_name="year")
        _tb = _tb.assign(**{"scenario": scenario})
        tables.append(_tb)
    tb = pr.concat(tables, short_name=tb_short_name)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(main_index_columns + ["year", "scenario"])

    return tb


def process_supply_for_key_minerals(data: pr.ExcelFile, sheet_name: str) -> Table:
    # Read the sheet, and let all columns be "Unnamed: X" (with X from 0 to the number of columns).
    tb = data.parse(sheet_name)

    # In this sheet there are two tables separated by an empty column.
    # Identify the separating column.
    column_position = int(tb.columns[tb.isnull().all()][0].split(" ")[-1])

    def _prepare_sub_table(_tb: Table) -> Table:
        _tb = _tb.copy()
        new_columns = _tb.iloc[2].astype("Int64").astype(object).fillna("").astype(str).values
        new_columns[0] = "country"
        _tb = _tb.rename(columns={_tb.columns[i]: column for i, column in enumerate(new_columns)}, errors="raise")
        assert _tb.loc[3].isnull().all(), "Sheet format may have changed."
        _tb = _tb.drop([0, 1, 2, 3]).reset_index(drop=True)
        _tb = drop_all_rows_of_notes(tb=_tb)

        # Drop empty columns and rows.
        _tb = _tb.dropna(axis=1, how="all").dropna(how="all").reset_index(drop=True)

        return _tb

    _tb_1 = _prepare_sub_table(tb[tb.columns[:column_position]])
    _tb_2 = _prepare_sub_table(tb[tb.columns[column_position + 1 :]])
    tb = pr.concat([_tb_1, _tb_2])

    # All predictions correspond to the "Base case" scenario (as stated in the title).
    tb["case"] = "Base case"

    # Separate minerals and technology in different columns.
    tb = transform_header_rows_into_new_column(tb=tb, header_row_name="mineral_process", normal_row_name="country")

    # Separate mineral and process.
    tb["mineral"] = tb["mineral_process"].str.split(" - ").str[0]
    tb["process"] = tb["mineral_process"].str.split(" - ").str[1]
    tb = tb.drop(columns="mineral_process")

    # Reformat table to have "year" and "supply" columns.
    tb = tb.melt(id_vars=["mineral", "process", "country", "case"], value_name="supply", var_name="year")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "mineral", "process", "case"], short_name="supply_for_key_minerals")

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
    tb_demand_for_key_minerals = process_demand_for_key_minerals(
        data=data,
        sheet_name="1 Total demand for key minerals",
        header_row_name="mineral",
        normal_row_name="technology",
        tb_short_name="demand_for_key_minerals",
    )
    tb_supply = process_supply_for_key_minerals(data=data, sheet_name="2 Total supply for key minerals")
    tb_demand_for_clean_energy_technologies = process_demand_for_key_minerals(
        data=data,
        sheet_name="3.1 Cleantech demand by tech",
        header_row_name="mineral",
        normal_row_name="technology",
        tb_short_name="demand_for_clean_energy_technologies",
    )
    tb_demand_for_clean_energy_technologies_by_mineral = process_demand_for_key_minerals(
        data=data,
        sheet_name="3.2 Cleantech demand by mineral",
        header_row_name=None,
        normal_row_name="mineral",
        tb_short_name="demand_for_clean_energy_technologies_by_mineral",
    )
    tb_demand_for_solar_pv = process_demand_for_key_minerals(
        data=data,
        sheet_name="4.1 Solar PV",
        header_row_name="case",
        normal_row_name="mineral",
        tb_short_name="demand_for_solar_pv",
    )
    tb_demand_for_wind = process_demand_for_key_minerals(
        data=data,
        sheet_name="4.2 Wind",
        header_row_name="case",
        normal_row_name="mineral",
        tb_short_name="demand_for_wind",
    )
    tb_demand_for_ev = process_demand_for_key_minerals(
        data=data, sheet_name="4.3 EV", header_row_name="case", normal_row_name="mineral", tb_short_name="demand_for_ev"
    )
    tb_demand_for_battery_storage = process_demand_for_key_minerals(
        data=data,
        sheet_name="4.4 Battery storage",
        header_row_name="case",
        normal_row_name="mineral",
        tb_short_name="demand_for_battery_storage",
    )
    tb_demand_for_electricity_networks = process_demand_for_key_minerals(
        data=data,
        sheet_name="4.5 Electricity networks",
        header_row_name="case",
        normal_row_name="mineral",
        tb_short_name="demand_for_electricity_networks",
    )
    tb_demand_for_hydrogen = process_demand_for_key_minerals(
        data=data,
        sheet_name="4.6 Hydrogen",
        header_row_name="case",
        normal_row_name="mineral",
        tb_short_name="demand_for_hydrogen",
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[
            tb_demand_for_key_minerals,
            tb_supply,
            tb_demand_for_clean_energy_technologies,
            tb_demand_for_clean_energy_technologies_by_mineral,
            tb_demand_for_solar_pv,
            tb_demand_for_wind,
            tb_demand_for_ev,
            tb_demand_for_battery_storage,
            tb_demand_for_electricity_networks,
            tb_demand_for_hydrogen,
        ],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
