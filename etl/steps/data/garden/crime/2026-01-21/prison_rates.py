"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("prison_rates")

    # Read table from meadow dataset.
    tb = ds_meadow.read("prison_rates")
    # Don't keep this
    tb = tb.dropna(subset="year")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_united_kingdom(tb: Table) -> Table:
    """
    Calculate data for the UK based on the component countries.
    """
    tb_uk = tb[tb["country"].isin(["England & Wales", "Northern Ireland", "Scotland"])]
