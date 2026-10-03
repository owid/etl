"""Load a snapshot and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("long_run_stunting.xlsx")

    # Read table from snapshot.
    tb = snap.read_excel(sheet_name="Data")

    #
    # Process data.
    #
    # Rename columns.
    tb = tb.rename(columns={"country_code": "country", "b_decade": "year"})

    # change year to midpoint of decade
    tb["year"] = tb["year"] + 5

    tb = tb[["country", "year", "stunting_rate"]]

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save garden dataset.
    ds_garden.save()
