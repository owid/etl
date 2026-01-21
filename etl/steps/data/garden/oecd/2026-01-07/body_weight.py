"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("body_weight")

    # Read table from meadow dataset.
    tb = ds_meadow.read("body_weight")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)
    assert len(tb["unit_of_measure"].unique() == 1)
    assert len(tb["age"].unique() == 1)
    tb = tb.drop(columns=["unit_of_measure", "measurement_method", "age"])

    # Improve table format.
    tb = tb.format(["country", "year", "measure", "sex"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
