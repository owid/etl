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
    ds_meadow = paths.load_dataset("rinderpest")

    # Read table from meadow dataset.
    tb = ds_meadow.read("rinderpest")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.drop(columns="listing_date")
    tb["last_recorded_rinderpest"] = (
        tb["last_recorded_rinderpest"]
        .str.replace(r"\s*\(imported\)", "", regex=True)
        # Sao Tome and Principe is recorded as 1950s, so we replace it with 1959
        # to match the other countries.
        # Kosovo is listed as the 1890s so we replace it with 1899 to match the other countries.
        # We also replace the values for Liechtenstein of 19th century with 1899 to match the other countries.
        .replace({"1950s": "1959", "1890s": "1899", "19th century": "1899", "Never reported": "0"})
    )

    # Improve table format.
    tb = tb.format(["country"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
