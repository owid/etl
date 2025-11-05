"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("semiconductors_cset.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("semiconductors_cset")

    # Read table from meadow dataset.
    tb = ds_meadow.read("semiconductors_cset")

    #
    # Process data.
    #
    # Harmonize country names using OWID standard
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    tb = tb.rename(columns={"country": "country_name"})

    # Set index
    tb = tb.format(["provider", "provided_name", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("semiconductors_cset.end")
