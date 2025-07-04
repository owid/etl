"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# FAOSTAT source name (used to be able to put this source first in the list of sources of each variable).
FAOSTAT_SOURCE_NAME = "Food and Agriculture Organization of the United Nations"

# FAOSTAT element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005412"


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


def create_table_of_rolling_averages(tb: Table) -> Table:
    # NOTE: Given that different countries have different sampling, I'll just create these variables for the UK and the World (since that's the only data we need for now). If we need smooth curves in the future for other countries, we can generalize this function.
    countries = ["United Kingdom", "World"]
    columns = ["country", "year"] + [
        crop + "_yield" for crop in ["barley", "oats", "potatoes", "pulses", "rye", "sugar_beet", "wheat"]
    ]
    tb = tb[(tb["country"].isin(countries))][columns].reset_index(drop=True)
    # To do that, combine pre-1961 data as-is, with a 10-year rolling average of FAOSTAT data (from 1961 onwards).
    tb_historical = tb[tb["year"] < 1961].reset_index(drop=True)
    tb_modern = tb[tb["year"] >= 1961].reset_index(drop=True)
    # Get data columns.
    data_columns = [column for column in tb.columns if column not in ["country", "year"]]
    # Apply rolling average only to data columns for modern period.
    _tables = []
    for country in countries:
        _tb_modern = tb[(tb["year"] >= 1961) & (tb["country"] == country)].reset_index(drop=True)
        _tb_modern[data_columns] = _tb_modern[data_columns].rolling(window=10, min_periods=1).mean()
        _tables.append(_tb_modern)
    tb_modern = pr.concat(_tables)
    # Combine both periods back together
    tb_combined = pr.concat([tb_historical, tb_modern], ignore_index=True)
    # Rename columns conveniently.
    tb_combined = tb_combined.rename(columns={column: column + "_smoothed" for column in data_columns}, errors="raise")
    # Improve table format.
    tb_combined = tb_combined.format(short_name=paths.short_name + "_smoothed")

    # Improve metadata.
    for column in tb_combined.columns:
        tb_combined[
            column
        ].metadata.description_processing = "- The historical data (before 1961) has been combined with a 10-year rolling average of the FAOSTAT data (from 1961 onwards), to reduce short-term volatility and make long-term trends more visible."

    return tb_combined


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl")

    # Load the UK long-term yields dataset and read its main table.
    ds_uk = paths.load_dataset("uk_long_term_yields")
    tb_uk = ds_uk.read("uk_long_term_yields")

    # Load the long-term US corn yields dataset and read its main table.
    ds_us = paths.load_dataset("us_corn_yields")
    tb_us = ds_us.read("us_corn_yields")

    # Load the long-term wheat yields dataset and read its main table.
    ds_wheat = paths.load_dataset("long_term_wheat_yields")
    tb_wheat = ds_wheat.read("long_term_wheat_yields")

    #
    # Process data.
    #
    # It's better to not combine the UK long-run series with the long-term wheat yields series.
    # The latter only adds a few points to the UK, and the first one (in 1850, from Bayliss-Smith & Wanmali (1984)) is at odds with the closest estimates from Broadberry et al. (2015).
    # NOTE: In the future, consider fixing this discrepancy in either the uk_long_term_yields or the long_term_wheat_yields steps (or merge them into one).
    tb_wheat = tb_wheat[tb_wheat["country"] != "United Kingdom"].reset_index(drop=True)

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

    # Create another table of smooth yield series.
    # NOTE: For now I'll do it only for the UK and the World, and for certain crops (required for a specific chart).
    tb_smoothed = create_table_of_rolling_averages(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_smoothed])
    ds_garden.save()
