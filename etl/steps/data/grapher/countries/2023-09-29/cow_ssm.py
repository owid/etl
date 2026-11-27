"""Load a garden dataset and create a grapher dataset."""

from copy import deepcopy

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("cow_ssm")

    # Read table from garden dataset.
    tb_regions = ds_garden["cow_ssm_regions"]
    tb_countries = ds_garden["cow_ssm_countries"]

    #
    # Process data.
    #
    tb_regions = tb_regions.rename_index_names({"region": "country"})
    tb_countries = tb_countries.reset_index().rename(columns={"id": "is_present"})
    tb_countries["is_present"] = 1
    tb_countries["is_present"].metadata = tb_regions["number_countries"].metadata

    # Fill zeroes
    column_index = ["year", "country"]
    tb_countries = fill_gaps_with_zeroes(tb_countries, columns=column_index)
    # Store the sovereign state flag as No/Yes, so the Swimlane chart can show it.
    # NOTE: On each update, check that the flag is still 0/1. The recoding fails on any other value.
    unknown = set(tb_countries["is_present"].dropna().unique()) - {0, 1}
    assert not unknown, f"`is_present` has values other than 0 and 1: {unknown}"
    # Copy it: `is_present` shares its metadata object with `number_countries`, which must stay numeric.
    metadata = deepcopy(tb_countries["is_present"].metadata)
    tb_countries["is_present"] = tb_countries["is_present"].map({0: "No", 1: "Yes"}).astype("category")
    tb_countries["is_present"].metadata = metadata
    tb_countries["is_present"].metadata.type = "string"
    tb_countries["is_present"].metadata.sort = ["No", "Yes"]

    tb_countries = tb_countries.set_index(column_index, verify_integrity=True)
    #
    # Save outputs.
    #
    tables = [
        tb_regions,
        tb_countries,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def fill_gaps_with_zeroes(
    tb: Table, columns: list[str], cols_use_range: list[str] | None = None, use_nan: bool = False
) -> Table:
    """Fill missing values with zeroes.

    Makes sure all combinations of `columns` are present. If not present in the original table, then it is added with zero value.
    """
    # Build grid with all possible values
    values_possible = []
    for col in columns:
        if cols_use_range and col in cols_use_range:
            value_range = np.arange(tb[col].min(), tb[col].max() + 1)
            values_possible.append(value_range)
        else:
            values_possible.append(set(tb[col]))

    # Reindex
    new_idx = pd.MultiIndex.from_product(values_possible, names=columns)
    tb = tb.set_index(columns).reindex(new_idx).reset_index()

    # Fill zeroes
    if not use_nan:
        columns_fill = [col for col in tb.columns if col not in columns]
        tb[columns_fill] = tb[columns_fill].fillna(0)
    return tb
