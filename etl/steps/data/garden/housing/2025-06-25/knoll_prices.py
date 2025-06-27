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
    ds_meadow = paths.load_dataset("knoll_prices")

    # Read table from meadow dataset.
    tb = ds_meadow.read("knoll_prices")

    # Calculate real house prices from nominal house prices and CPI
    # This calculation is identical to the calculation used in the original paper which can be found here: https://www.aeaweb.org/articles?id=10.1257/aer.20150501
    tb["hpreal"] = tb["hpnom"] / tb["cpi"] * 100

    tb = tb[["country", "year", "hpreal"]]

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
