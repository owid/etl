"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_old_data(tb_old: Table) -> Table:
    """Extract 2007-2011 rows from the 2018 USDA report, deriving caged hens from the cage-free percentage."""
    tb_old = tb_old[tb_old["year"] < 2012].copy()
    tb_old["caged"] = tb_old["cage_free"] / (tb_old["cage_free_pct"] / 100) - tb_old["cage_free"]
    for col in ["caged", "cage_free", "organic_cage_free", "non_organic_cage_free"]:
        tb_old[col] = tb_old[col] * 1e6
    return tb_old[["year", "caged", "cage_free", "organic_cage_free", "non_organic_cage_free"]]


def run() -> None:
    #
    # Load inputs.
    #
    # Load current USDA meadow dataset (2012+).
    ds_meadow = paths.load_dataset("us_egg_production", version="2026-04-16")
    tb = ds_meadow.read("us_egg_production")

    # Load older USDA meadow dataset for 2007-2011.
    ds_meadow_old = paths.load_dataset("us_egg_production_2007_2018")
    tb_old = ds_meadow_old.read("us_egg_production_2007_2018")

    #
    # Process data.
    #
    # Convert millions of hens to hens.
    hen_columns = [col for col in tb.columns if col != "year"]
    for col in hen_columns:
        tb[col] = tb[col] * 1e6

    # Combine with 2007-2011 data from the older USDA report.
    tb = pr.concat([prepare_old_data(tb_old), tb], ignore_index=True, short_name=paths.short_name)

    # Add country column.
    tb["country"] = "United States"

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save new garden dataset.
    ds_garden.save()
