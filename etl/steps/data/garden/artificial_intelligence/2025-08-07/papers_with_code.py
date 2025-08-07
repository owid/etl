"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("papers_with_code")

    # Read table from meadow dataset.
    tb = ds_meadow.read("papers_with_code")

    #
    # Process data.
    #
    tb = tb.rename(columns={"model_name": "country", "paper_date": "date"})
    # Improve table format.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
