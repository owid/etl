"""Load a meadow dataset and create a garden dataset."""

from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its table.
    ds_meadow = paths.load_dataset("wdi_prices")
    tb_meadow = ds_meadow.read("wdi_prices")

    #
    # Process data.
    #
    # Harmonize country names
    tb = geo.harmonize_countries(df=tb_meadow, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
