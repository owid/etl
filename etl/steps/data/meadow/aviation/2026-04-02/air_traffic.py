"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS = {
    "Year": "year",
    "PLF": "passenger_load_factor",
    "RPKs (mils)": "revenue_passenger_kilometers",
    "ASKs (mils)": "available_seat_kilometers",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("air_traffic.csv")
    tb = snap.read(encoding="utf-16", sep="\t")

    #
    # Process data.
    #
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS)

    # Strip % sign from passenger load factor and convert to float.
    tb["passenger_load_factor"] = tb["passenger_load_factor"].str.rstrip("%").astype(float)
    # Strip comma formatting from RPKs and ASKs (e.g. '6,600' → 6600) and convert to float.
    for col in ["revenue_passenger_kilometers", "available_seat_kilometers"]:
        tb[col] = tb[col].str.replace(",", "", regex=False).astype(float)

    tb["country"] = "World"
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
