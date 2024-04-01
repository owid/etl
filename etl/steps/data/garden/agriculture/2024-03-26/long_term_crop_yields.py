"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# FAOSTAT source name (used to be able to put this source first in the list of sources of each variable).
FAOSTAT_SOURCE_NAME = "Food and Agriculture Organization of the United Nations"

# FAOSTAT element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"


def prepare_faostat_data(tb_qcl: Table) -> Table:
    # Select the relevant metric in FAOSTAT dataset.
    tb_qcl = tb_qcl[tb_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD].reset_index(drop=True)

    # Sanity check.
    error = "Units of yield may have changed in FAOSTAT QCL."
    assert set(tb_qcl["unit"]) == {"tonnes per hectare"}, error

    # Transpose FAOSTAT data.
    tb_qcl = tb_qcl.pivot(index=["country", "year"], columns=["item"], values=["value"], join_column_levels_with="_")
    tb_qcl = tb_qcl.rename(
        columns={
            column: column.replace("value_", "") + "_yield"
            for column in tb_qcl.columns
            if column not in ["country", "year"]
        },
        errors="raise",
    ).underscore()

    return tb_qcl


def run_sanity_checks_on_inputs(tb_qcl: Table, tb_us: Table, tb_uk: Table, tb_wheat: Table) -> None:
    error = "Columns in US long-term corn yields were expected to be found in FAOSTAT QCL."
    assert set(tb_us.columns) <= set(tb_qcl.columns), error
    error = "Columns in UK long-term yields were expected to be found in FAOSTAT QCL."
    assert set(tb_uk.columns) <= set(tb_qcl.columns), error
    error = "UK long-term yields were expected to start earlier than FAOSTAT QCL."
    assert set(tb_qcl[tb_qcl["country"] == "United Kingdom"]["year"]) <= set(tb_uk["year"]), error
    error = "Columns in long-term wheat yields were expected to be found in FAOSTAT QCL."
    assert set(tb_wheat.columns) <= set(tb_qcl.columns)
    error = "Long-term wheat yields were expected to start earlier than FAOSTAT QCL."
    assert set(tb_qcl["year"]) <= set(tb_wheat["year"])


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl["faostat_qcl"].reset_index()

    # Load the UK long-term yields dataset and read its main table.
    ds_uk = paths.load_dataset("uk_long_term_yields")
    tb_uk = ds_uk["uk_long_term_yields"].reset_index()

    # Load the long-term US corn yields dataset and read its main table.
    ds_us = paths.load_dataset("us_corn_yields")
    tb_us = ds_us["us_corn_yields"].reset_index()

    # Load the long-term wheat yields dataset and read its main table.
    ds_wheat = paths.load_dataset("long_term_wheat_yields")
    tb_wheat = ds_wheat["long_term_wheat_yields"].reset_index()

    #
    # Process data.
    #
    # Prepare FAOSTAT QCL data.
    tb_qcl = prepare_faostat_data(tb_qcl=tb_qcl)

    # Rename US corn variable to be consistent with FAOSTAT QCL.
    tb_us = tb_us.rename(columns={"corn_yield": "maize_yield"}, errors="raise")

    # Sanity checks.
    run_sanity_checks_on_inputs(tb_qcl=tb_qcl, tb_us=tb_us, tb_uk=tb_uk, tb_wheat=tb_wheat)

    # Tables tb_uk and tb_wheat share column "wheat_yield" for the UK.
    # We should keep the former, since it includes much earlier data.

    # Combine the long-term wheat yields table with FAOSTAT QCL (prioritizing the former).
    tb = combine_two_overlapping_dataframes(
        df1=tb_wheat, df2=tb_qcl, index_columns=["country", "year"], keep_column_order=True
    )

    # Combine the UK long-term yields with the previous table (prioritizing the former).
    tb = combine_two_overlapping_dataframes(
        df1=tb_uk, df2=tb, index_columns=["country", "year"], keep_column_order=True
    )

    # Combine the US long-term corn yields with the previous table (prioritizing the former).
    tb = combine_two_overlapping_dataframes(
        df1=tb_us, df2=tb, index_columns=["country", "year"], keep_column_order=True
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
