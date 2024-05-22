"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
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
    ds_meadow = paths.load_dataset("eiu")

    # Read table from meadow dataset.
    tb = ds_meadow["eiu"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove years with interpolated data (2007 and 2009 are interpolated by Gapminder)
    tb = tb[~tb["year"].isin([2007, 2009])]

    # Drop rank column
    tb = tb.drop(columns=["rank_eiu"])
    tb = cast(Table, tb)

    tb = add_regime_identifier(tb)

    # Format
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regime_identifier(tb: Table) -> Table:
    """Create regime identifier."""
    # `regime_eiu`: Categorise democracy_eiu into 4 groups
    bins = [
        -0.01,
        4,
        6,
        8,
        10,
    ]
    labels = [
        0,
        1,
        2,
        3,
    ]
    tb["regime_eiu"] = pd.cut(tb["democracy_eiu"], bins=bins, labels=labels)

    # Add metadata
    tb["regime_eiu"] = tb["regime_eiu"].copy_metadata(tb["democracy_eiu"])
    return tb
