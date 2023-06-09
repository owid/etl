"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import numpy as np
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# FAOSTAT source name (used to be able to put this source first in the list of sources of each variable).
FAOSTAT_SOURCE_NAME = "Food and Agriculture Organization of the United Nations"

# FAOSTAT element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"


def prepare_faostat_data(tb_qcl: Table) -> Table:
    # Select the relevant metric in FAOSTAT dataset.
    tb_qcl = tb_qcl[tb_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD].reset_index(drop=True)

    # Store FAOSTAT QCL metadata (it will be used later after transforming the table).
    metadata_qcl = tb_qcl.metadata

    # Sanity check.
    error = "Units of yield may have changed in FAOSTAT QCL."
    assert set(tb_qcl["unit"]) == {"tonnes per hectare"}, error

    # Transpose FAOSTAT data.
    tb_qcl = tb_qcl.pivot(index=["country", "year"], columns=["item"], values=["value"])
    tb_qcl.columns = [f"{underscore(column[1])}_yield" for column in tb_qcl.columns]
    tb_qcl = tb_qcl.reset_index()
    tb_qcl.metadata = metadata_qcl

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


def combine_variables_metadata(combined_table: Table, individual_tables: List[Table]) -> Table:
    # Assign sources and licenses of the variables of each individual table to the variables in the combined table.
    combined_table = combined_table.copy()
    for column in combined_table.columns:
        # Initialize sources and licenses for the current variable.
        sources = []
        licenses = []
        for table in individual_tables:
            if column in table.columns:
                # If the current variable was in this table, assign its sources and licenses to the current variable.
                for source in table.metadata.dataset.sources:
                    if source.name not in [known_source.name for known_source in sources]:
                        sources.append(source)
                for license in table.metadata.dataset.licenses:
                    if license.name not in [known_license.name for known_license in licenses]:
                        licenses.append(license)

        # Given that FAOSTAT is the main source of data, place this source first in the list of sources.
        sources_without_faostat = [source for source in sources if source.name != FAOSTAT_SOURCE_NAME]
        # Sort the rest of the sources in alphabetical order.
        sources_sorting = np.argsort([source.name for source in sources_without_faostat])
        sources = [source for source in sources if source.name == FAOSTAT_SOURCE_NAME] + np.array(
            sources_without_faostat
        )[sources_sorting].tolist()

        combined_table[column].metadata.sources = sources
        combined_table[column].metadata.licenses = licenses

        # Generate a title for this variable using the column name.
        title = column.capitalize().replace("_", " ").replace("  ", " ").replace("n e c", "n.e.c.")
        combined_table[column].metadata.title = title

        # Define units.
        combined_table[column].metadata.unit = "tonnes per hectare"
        combined_table[column].metadata.short_unit = "t/ha"

    return combined_table


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset and read its main table.
    ds_qcl = cast(Dataset, paths.load_dependency("faostat_qcl"))
    tb_qcl = ds_qcl["faostat_qcl"].reset_index()

    # Load the UK long-term yields dataset and read its main table.
    ds_uk = cast(Dataset, paths.load_dependency("uk_long_term_yields"))
    tb_uk = ds_uk["uk_long_term_yields"].reset_index()

    # Load the long-term US corn yields dataset and read its main table.
    ds_us = cast(Dataset, paths.load_dependency("us_corn_yields"))
    tb_us = ds_us["us_corn_yields"].reset_index()

    # Load the long-term wheat yields dataset and read its main table.
    ds_wheat = cast(Dataset, paths.load_dependency("long_term_wheat_yields"))
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

    # Combine variables metadata and adjust table metadata.
    tb = combine_variables_metadata(combined_table=tb, individual_tables=[tb_uk, tb_us, tb_wheat, tb_qcl])
    tb.metadata.short_name = paths.short_name

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    # Combine sources and licenses from all involved datasets.
    ds_garden.metadata.sources = sum([ds.metadata.sources for ds in [ds_uk, ds_us, ds_wheat, ds_qcl]], [])
    ds_garden.metadata.licenses = sum([ds.metadata.licenses for ds in [ds_uk, ds_us, ds_wheat, ds_qcl]], [])

    # Save changes in the new garden dataset.
    ds_garden.save()
