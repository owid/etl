"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "year": "year",
    "month": "month",
    "average": "concentration",
    # The following column is loaded only to perform a sanity check.
    "decimal": "decimal",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("ghg_concentration")
    tb_co2 = ds_meadow["co2_concentration_monthly"].reset_index()
    tb_ch4 = ds_meadow["ch4_concentration_monthly"].reset_index()
    tb_n2o = ds_meadow["n2o_concentration_monthly"].reset_index()

    #
    # Process data.
    #
    # Add a column for the gas name, and concatenate all tables.
    tb = pr.concat(
        [tb_co2.assign(gas="co2"), tb_ch4.assign(gas="ch4"), tb_n2o.assign(gas="n2o")],
        ignore_index=True,
        short_name=paths.short_name,
    )

    # Select necessary columns and rename them.
    tb = tb[list(COLUMNS) + ["gas"]].rename(columns=COLUMNS, errors="raise")

    # There is a "decimal" column for the year as a decimal number, that only has 12 possible values, corresponding to
    # the middle of each month, so we will assume the 15th of each month.
    error = "Date format has changed."
    assert len(set(tb["decimal"].astype(str).str.split(".").str[1])) == 12, error
    assert set(tb["month"]) == set(range(1, 13)), error
    tb["date"] = tb["year"].astype(str) + "-" + tb["month"].astype(str).str.zfill(2) + "-15"

    # Add a location column.
    tb["location"] = "World"

    # Remove unnecessary columns.
    tb = tb.drop(columns=["year", "month", "decimal"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "date", "gas"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
