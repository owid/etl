"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Carriers approved to operate paid driverless services in California. If a new carrier is
# approved, the TCPID→carrier map in the snapshot script must be extended and this set updated.
KNOWN_CARRIERS = {"Waymo", "Cruise"}

# First month covered by the CPUC deployment reports. If the earliest date moves forward,
# a previous report stopped being scraped — a coverage regression, not a real change.
FIRST_MONTH = "2022-03-31"

# Miles to kilometers conversion factor.
MILES_TO_KM = 1.60934

NUMERIC_COLS = ["totaltrips", "totalpassengerscarried", "totalpmt"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("robotaxis")

    # Read table from meadow dataset.
    tb = ds_meadow.read("robotaxis")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Sum across all carriers for each date
    tb = tb.groupby("date")[NUMERIC_COLS].sum().reset_index()

    # Convert passenger miles traveled to kilometers
    tb["totalpmt"] = tb["totalpmt"] * MILES_TO_KM

    tb["country"] = "California"

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    expected_cols = {"carrier_name", "date"} | set(NUMERIC_COLS)
    missing_cols = expected_cols - set(tb.columns)
    assert not missing_cols, f"Missing expected columns in meadow table: {missing_cols}"

    unknown_carriers = set(tb["carrier_name"]) - KNOWN_CARRIERS
    assert not unknown_carriers, (
        f"Unknown carriers in the data: {unknown_carriers}. "
        "Extend the TCPID→carrier map in the snapshot script and this step's KNOWN_CARRIERS."
    )

    negative = {col: tb[col].min() for col in NUMERIC_COLS if (tb[col] < 0).any()}
    assert not negative, f"Negative values found: {negative}"

    assert not tb.duplicated(subset=["carrier_name", "date"]).any(), "Duplicate (carrier, month) rows in meadow table."

    assert str(tb["date"].min().date()) == FIRST_MONTH, (
        f"Earliest month is {tb['date'].min().date()}, expected {FIRST_MONTH} — "
        "a previous report may no longer be scraped."
    )


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "date"]).any(), "Duplicate (country, month) rows in output."

    # Reports cover contiguous months — a gap means a quarterly report went missing from the scrape.
    gaps = tb["date"].sort_values().diff().dt.days.dropna()
    assert gaps.max() <= 31, f"Gap of {gaps.max()} days between consecutive months — a report is missing."

    assert tb[NUMERIC_COLS].isna().sum().sum() == 0, "Unexpected NaN values in output indicators."
    assert (tb[NUMERIC_COLS] >= 0).all().all(), "Negative values in output indicators."
