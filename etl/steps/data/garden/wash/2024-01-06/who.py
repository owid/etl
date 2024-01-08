"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("who")

    tables = []
    table_names = ds_meadow.table_names
    for table_name in table_names:
        # Read table from meadow dataset.
        tb = ds_meadow[table_name].reset_index()
        # Clean values.
        tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
        tb = clean_values(tb)
        tb = drop_region_columns(tb)
        tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_values(tb: Table) -> Table:
    """
    Some values are strings that begin with either < or >. These are not valid numbers, so we need to clean them by removing those non-numeric characters.
    Additionally, NAs are represented with - in the data, we need to replace those with NaNs.

    """
    # Replace - with NaNs.
    tb = tb.replace("-", np.nan)
    # Remove < and > from strings.
    # tb = tb.replace(r"[<>]", "", regex=True)
    return tb


def drop_region_columns(tb: Table) -> Table:
    """
    Drop columns that contain region information.

    """
    columns_to_drop = [col for col in tb.columns if "region" in col.lower()]
    tb = tb.drop(columns_to_drop, axis=1)
    return tb
