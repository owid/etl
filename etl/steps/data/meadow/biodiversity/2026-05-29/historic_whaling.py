"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("historic_whaling_rocha_et_al.xlsx")

    table_names = snap.ExcelFile().sheet_names

    assert table_names == [
        "Table 1 – Total by Species",
        "Table 2 – N. Hemisphere Decades",
        "Table 3 – S. Hemisphere Decades",
        "Table 4 – N. Hemisphere Annual",
        "Table 5 – S. Hemisphere Annual",
    ], f"Expected table names do not match. Found: {table_names}"

    # Load data for Decade and annual data
    tb_n_dec = snap.read_excel(sheet_name="Table 2 – N. Hemisphere Decades", header=2)
    tb_n_dec["hemisphere"] = "Northern Hemisphere"
    tb_s_dec = snap.read_excel(sheet_name="Table 3 – S. Hemisphere Decades", header=2)
    tb_s_dec["hemisphere"] = "Southern Hemisphere"

    tb_decades = pr.concat([tb_n_dec, tb_s_dec], ignore_index=True)

    tb_n_ann = snap.read_excel(sheet_name="Table 4 – N. Hemisphere Annual", header=2)
    tb_n_ann["hemisphere"] = "Northern Hemisphere"
    tb_s_ann = snap.read_excel(sheet_name="Table 5 – S. Hemisphere Annual", header=2)
    tb_s_ann["hemisphere"] = "Southern Hemisphere"
    tb_annual = pr.concat([tb_n_ann, tb_s_ann], ignore_index=True)

    tb_decades = tb_decades.format(["species", "hemisphere"], short_name="historic_whaling_decades")
    tb_annual = tb_annual.format(["year", "hemisphere"], short_name="historic_whaling_annual")

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb_decades, tb_annual]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
