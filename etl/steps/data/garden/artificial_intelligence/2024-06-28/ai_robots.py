"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_robots.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb.rename(columns={"Geographic area": "country", "Year": "year"})
    tb["Number of robots (in thousands)"] = tb["Number of robots (in thousands)"] * 1000
    tb = tb.rename(columns={"Number of robots (in thousands)": "number_of_robots"})
    tb = tb.pivot(index=["year", "country"], columns="Indicator", values="number_of_robots").reset_index()

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
