"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to select from sheet and how to rename them.
COLUMNS = {
    "Accidents \n(excl. suicide, sabotage, hijackings etc.) Year": "year",
    "Accidents ": "accidents_excluding_hijacking_etc",
    "Fatalities": "fatalities_excluding_hijacking_etc",
    "Accidents \n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_including_hijacking_etc",
    "Fatalities.1": "fatalities_including_hijacking_etc",
    "Accidents with passenger flights \n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_with_passenger_flights_including_hijacking_etc",
    "Fatalities.2": "fatalities_with_passenger_flights_including_hijacking_etc",
    "Accidents with passenger + cargo flights\n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_with_passenger_and_cargo_flights_including_hijacking_etc",
    "Fatalities.3": "fatalities_with_passenger_and_cargo_flights_including_hijacking_etc",
    # 'World air traffic (departures)': '',
    # '1 accident \nper x flights': '',
    # 'fatal accidents \nper mln flights': '',
    # '5-year \nmoving avg': '',
    "Corporate jets (civilian) Accidents ": "accidents_with_corporate_jets",
    "Fatalities.4": "fatalities_with_corporate_jets",
    # 'moving 5 year average # of accidents': '',
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = paths.load_snapshot("aviation_statistics.csv")
    snap_by_period = paths.load_snapshot("aviation_statistics_by_period.csv")
    snap_by_nature = paths.load_snapshot("aviation_statistics_by_nature.csv")

    # Load data from snapshots.
    tb = snap.read(safe_types=False)
    tb_by_period = snap_by_period.read()
    tb_by_nature = snap_by_nature.read()

    #
    # Process data.
    #
    # Select necessary columns and rename them appropriately.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")
    tb_by_period = tb_by_period.rename(columns={"Year": "year"}, errors="raise")
    tb_by_nature = tb_by_nature.rename(columns={"Year": "year"}, errors="raise")

    # Drop last row (which should be the only one without a year), which gives a grand total.
    tb = tb.dropna(subset="year").reset_index(drop=True).astype({"year": int})

    # Combine all tables.
    tb_combined = pr.multi_merge([tb, tb_by_period, tb_by_nature], how="outer", on=["year"])

    # Add a country column (that only contains "World").
    tb_combined["country"] = "World"

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_meadow.save()
