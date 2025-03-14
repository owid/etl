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
    ds_phr = paths.load_dataset("polio_deaths", namespace="public_health_reports")
    ds_cdc = paths.load_dataset("polio_deaths", namespace="cdc")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_phr = ds_phr.read("polio_deaths")
    tb_cdc = ds_cdc.read("polio_deaths")
    tb_pop = ds_population.read("population")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
