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
    ds_meadow = paths.load_dataset("geoboundaries_cgaz")

    # Read table from meadow dataset.
    tb = ds_meadow.read("geoboundaries_cgaz")

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "shapename": "territory_name",
            "shapegroup": "territory_code",
            "shapetype": "territory_type",
        }
    )

    # Harmonize country names.
    tb["country"] = tb["territory_name"].copy()
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Improve table format.
    tb = tb.format(["country", "territory_name"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
