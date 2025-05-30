"""Load Statistical Review of World Energy and create variables of reserves-to-production ratios of fossil fuels."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# We use the values from the "Approximate conversion factors" sheet in the statistical review excel data file.
BILLION_BARRELS_TO_TONNES = 1e9 * 0.1364
MILLION_TONNES_TO_TONNES = 1e6
TRILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e12
BILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e9


def prepare_statistical_review_data(tb_review: Table) -> Table:
    tb_review = tb_review.copy()

    # Check that the units are the expected ones.
    assert tb_review["coal_reserves_mt"].metadata.unit == "million tonnes"
    assert tb_review["coal_production_mt"].metadata.unit == "million tonnes"
    assert tb_review["oil_reserves_bbl"].metadata.unit == "billion barrels"
    assert tb_review["oil_production_mt"].metadata.unit == "million tonnes"
    assert tb_review["gas_reserves_tcm"].metadata.unit == "trillion cubic meters"
    assert tb_review["gas_production_bcm"].metadata.unit == "billion cubic meters"

    # Prepare Statistical Review data.
    columns = {
        "country": "country",
        "year": "year",
        "coal_reserves_mt": "coal_reserves",
        "coal_production_mt": "coal_production",
        "oil_reserves_bbl": "oil_reserves",
        "oil_production_mt": "oil_production",
        "gas_reserves_tcm": "gas_reserves",
        "gas_production_bcm": "gas_production",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns, errors="raise")

    # Select only global data.
    tb_review = tb_review[tb_review["country"] == "World"].reset_index(drop=True)

    # Convert to tonnes.
    tb_review["coal_reserves"] *= MILLION_TONNES_TO_TONNES
    tb_review["coal_production"] *= MILLION_TONNES_TO_TONNES
    tb_review["oil_production"] *= MILLION_TONNES_TO_TONNES
    tb_review["oil_reserves"] *= BILLION_BARRELS_TO_TONNES
    tb_review["gas_reserves"] *= TRILLION_CUBIC_METERS_TO_CUBIC_METERS
    tb_review["gas_production"] *= BILLION_CUBIC_METERS_TO_CUBIC_METERS

    # Create columns for reserves-production ratio (measured in years of fossil fuels left).
    tb_review["coal_left"] = tb_review["coal_reserves"] / tb_review["coal_production"]
    tb_review["oil_left"] = tb_review["oil_reserves"] / tb_review["oil_production"]
    tb_review["gas_left"] = tb_review["gas_reserves"] / tb_review["gas_production"]

    # Format table conveniently.
    tb_review = tb_review.dropna(how="all").format(sort_columns=True, short_name=paths.short_name)

    return tb_review


def run() -> None:
    #
    # Load data.
    #
    # Load Statistical Review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy")

    #
    # Process data.
    #
    # Prepare Statistical Review data.
    tb = prepare_statistical_review_data(tb_review=tb_review)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
