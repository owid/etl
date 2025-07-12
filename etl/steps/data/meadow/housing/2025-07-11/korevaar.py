"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COL_RENAME = {
    "Unnamed: 0": "year",
    "BelRR": "belgium_real_rent",
    "AmsRR": "amsterdam_real_rent",
    "LonRR": "london_real_rent",
    "ParRR": "paris_real_rent",
    "BelRW": "belgium_real_wage",
    "AmsRW": "amsterdam_real_wage",
    "LonRW": "london_real_wage",
    "ParRW": "paris_real_wage",
    "BelAff": "belgium_affordability",
    "AmsAff": "amsterdam_affordability",
    "LonAff": "london_affordability",
    "ParAff": "paris_affordability",
    "AntR": "antwerp_nom_rent",
    "BrgR": "bruges_nom_rent",
    "BruR": "brussels_nom_rent",
    "GheR": "ghent_nom_rent",
    "BelP": "belgium_cpi",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("korevaar.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    tb = tb.rename(columns=COL_RENAME)

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
