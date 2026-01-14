"""Garden step for plastic waste data with country harmonization."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cottom_plastic_waste")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("cottom_plastic_waste")
    #
    # Process data.
    #
    # Harmonize country names for national data
    tb = paths.regions.harmonize_names(tb)

    # Set index and format tables
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
