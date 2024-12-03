"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define tables to be loaded.
TABLES = ["country", "region", "global"]

# Define scenarios and new names
SCENARIOS = {
    "historical": "Historical",
    "current_forecast": "Current forecast + historical growth",
    "2pct": "2% growth",
    "2pct_gini1": "2% growth + Gini reduction 1%",
    "2pct_gini2": "2% growth + Gini reduction 2%",
    "4pct": "4% growth",
    "6pct": "6% growth",
    "8pct": "8% growth",
}

# Define index columns
INDEX_COLUMNS = ["country", "year", "povertyline", "scenario"]

# Define indicator columns
INDICATOR_COLUMNS = ["fgt0", "poorpop"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("poverty_projections")

    # Read table from meadow dataset.
    # Define empty table list to store tables.
    tables = []
    for table in TABLES:
        tb = ds_meadow.read(table)

        # Append table to list.
        tables.append(tb)

    #
    # Process data.
    #
    # Concatenate tables
    tb = pr.concat(tables, ignore_index=True)

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Rename scenario column
    tb["scenario"] = map_series(
        series=tb["scenario"],
        mapping=SCENARIOS,
    )

    # Recover origins
    tb["scenario"] = tb["scenario"].copy_metadata(tb["country"])

    tb = tb.format(INDEX_COLUMNS, short_name="poverty_projections")

    # Keep only relevant columns
    tb = tb[INDICATOR_COLUMNS]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
