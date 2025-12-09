"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("historical_inequality_van_zanden_et_al")

    # Read table from meadow dataset.
    tb = ds_meadow.read("historical_inequality_van_zanden_et_al")

    #
    # Process data.
    #

    # Drop ccode
    tb = tb.drop(columns=["ccode"], errors="raise")

    # Rename country.name to country and value to gini
    tb = tb.rename(columns={"country_name": "country", "value": "gini"}, errors="raise")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Divide gini values by 100 to express them as coefficients rather than percentages.
    tb["gini"] /= 100

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
