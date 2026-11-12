"""Load a meadow dataset and create a garden dataset."""

import re

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Entity name for the data. Table RAS0101 covers Great Britain (England, Scotland and Wales), not the whole United
# Kingdom: Northern Ireland collects its road casualty statistics separately. "Great Britain" is not in our regions
# dataset, so the name is set here rather than harmonized (harmonizing it to "United Kingdom" would mislabel the data).
COUNTRY = "Great Britain"

# Patterns that identify the column of deaths for each road user type in the meadow table (whose column names are
# the producer's headers in snake case, e.g. "pedestrians_killed").
# NOTE: These were written without access to the file. If the step fails here, compare them with the
#  column names listed in the error message and adjust.
ROAD_USER_PATTERNS = {
    "pedestrian_deaths": r"pedestrian",
    "pedal_cyclist_deaths": r"pedal_cycl",
    "motorcyclist_deaths": r"motor_?cycl",
    "car_occupant_deaths": r"(^|_)car_(occupant|user)",
}
# Pattern for the column of deaths among all road users.
TOTAL_PATTERN = r"^(killed|(all|total)\w*_killed|killed_\w*(all|total)\w*)$"

# Plausible range for the total number of road deaths in Great Britain in any year since 1926.
TOTAL_DEATHS_RANGE = (1_000, 10_000)


def find_column(tb: Table, pattern: str) -> str:
    """Return the only column of deaths (not "killed or seriously injured") whose name matches the given pattern."""
    candidates = [
        column
        for column in tb.columns
        if re.search(pattern, column) and ("killed" in column) and ("serious" not in column)
    ]
    assert len(candidates) == 1, (
        f"Expected one column of deaths matching '{pattern}', found {candidates}. Columns: {tb.columns.tolist()}"
    )
    return candidates[0]


def sanity_check_inputs(tb: Table) -> None:
    assert tb["year"].is_unique, "Duplicate years in the meadow table."
    assert tb["year"].min() <= 1930, f"Data should start in the 1920s, but it starts in {tb['year'].min()}."
    killed_columns = [column for column in tb.columns if "killed" in column]
    assert killed_columns, f"No columns of deaths found. Columns: {tb.columns.tolist()}"
    assert (tb[killed_columns].fillna(0) >= 0).all().all(), "Negative number of deaths in the meadow table."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    low, high = TOTAL_DEATHS_RANGE
    assert tb["road_deaths"].between(low, high).all(), (
        f"Total road deaths outside the plausible range {TOTAL_DEATHS_RANGE}, or missing: "
        f"{tb.loc[~tb['road_deaths'].between(low, high), ['year', 'road_deaths']].to_dict('records')}"
    )
    assert (tb["vehicle_occupant_deaths"].dropna() >= 0).all(), (
        "Deaths among pedestrians, pedal cyclists and motorcyclists exceed total road deaths in some year."
    )
    # Car occupants are part of the remainder group, so they can't outnumber it.
    rows_with_cars = tb["car_occupant_deaths"].notna()
    assert (tb.loc[rows_with_cars, "car_occupant_deaths"] <= tb.loc[rows_with_cars, "vehicle_occupant_deaths"]).all(), (
        "Car occupant deaths exceed deaths among all vehicle occupants and other road users in some year."
    )


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("road_casualties")

    # Read table from meadow dataset.
    tb = ds_meadow.read("road_casualties")
    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Select deaths by road user type, and the total.
    columns = {find_column(tb, pattern): name for name, pattern in ROAD_USER_PATTERNS.items()}
    columns[find_column(tb, TOTAL_PATTERN)] = "road_deaths"
    tb = tb[["year"] + list(columns)].rename(columns=columns, errors="raise")

    # Add deaths among all other road users: drivers and passengers of cars and of all other vehicles except
    # motorcycles and pedal cycles, plus other road users like horse riders. Computed as a remainder so that, with
    # pedestrians, pedal cyclists and motorcyclists, the groups add up to the total.
    tb["vehicle_occupant_deaths"] = (
        tb["road_deaths"] - tb["pedestrian_deaths"] - tb["pedal_cyclist_deaths"] - tb["motorcyclist_deaths"]
    )

    # Add the entity (see the note on COUNTRY above).
    tb["country"] = COUNTRY

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
