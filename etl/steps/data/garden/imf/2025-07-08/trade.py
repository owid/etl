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
    ds_meadow = paths.load_dataset("trade")

    # Read table from meadow dataset.
    tb = ds_meadow.read("trade")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.pivot(
        index=["country", "year", "counterpart_country"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
