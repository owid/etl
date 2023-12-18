"""Extract data from UNEP's Global trends in renewable energy investment.

Since the data is given as an image of a table (not suitable for OCR) the data has been manually copied into a file,
next to this script.

The data is copied from "FIGURE 42. GLOBAL TRENDS IN RENEWABLE ENERGY INVESTMENT 2020 DATA TABLE, $BN" (Page 62).
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)

EXTRACTED_DATA_FILE = paths.directory / "renewable_energy_investments.data.csv"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("global_trends_in_renewable_energy_investment.pdf")

    # Load file with manually extracted data.
    tb = pr.read_csv(EXTRACTED_DATA_FILE, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

    #
    # Prepare data.
    #
    # Transpose data to have a column per energy source.
    tb = tb.melt(id_vars="sector", var_name="year", value_name="investment").pivot(
        index="year", columns=["sector"], join_column_levels_with="_"
    )
    tb = tb.rename(columns={column: column.replace("investment_", "") for column in tb.columns}, errors="raise")

    # Add column for region.
    tb = tb.assign(**{"country": "World"})

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Update table short name.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
