"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("child_work_incidence_us.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    # The source is US-only; add the country column explicitly.
    tb["country"] = "United States"
    tb["country"] = tb["country"].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
