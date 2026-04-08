"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load inputs.
    snap = paths.load_snapshot("ghsl_countries.xlsx")

    # Load data from snapshot - using Country_STATS_L1 sheet (with UC, UCL, RUR aggregation)
    tb = snap.read(safe_types=False, sheet_name="Country_STATS_L1")

    # Remove rows where all values are NaNs.
    tb = tb.dropna(how="all")

    # Rename columns to be more interpretable.
    tb = tb.rename(
        columns={
            "UNLocName": "country",
            "Year": "year",
            "DEGURBA": "urbanization_level",
            "AREA_km2": "area",
            "POP": "population",
            "BU_km2": "built_up_area",
        }
    )

    # Drop UNLocID column as we don't need it.
    tb = tb.drop(columns=["UNLocID"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "urbanization_level"])

    # Save outputs.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()
