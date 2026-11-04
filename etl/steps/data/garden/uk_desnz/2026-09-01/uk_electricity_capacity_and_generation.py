from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Conversion factor from gigawatt-hours to terawatt-hours.
GWH_TO_TWH = 1e-3


def sanity_check_data(tb: Table) -> None:
    error = "Expected a complete annual series from 1920 to 2020."
    assert sorted(tb["year"]) == list(range(1920, 2021)), error

    error = "Negative generation found."
    assert (tb.drop(columns=["country", "year"]).fillna(0) >= 0).all().all(), error

    # The reported total should equal the sum of the fuel categories.
    components = [column for column in tb.columns if column not in ["country", "year", "total_generation"]]
    residual = (tb[components].sum(axis=1) - tb["total_generation"]).abs()
    error = "Generation by fuel does not add up to the reported total."
    assert (residual < 0.005).all(), error


def run() -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("uk_electricity_capacity_and_generation")
    tb = ds_meadow.read("uk_electricity_capacity_and_generation")

    #
    # Process data.
    #
    # Convert units from GWh to TWh.
    for column in tb.drop(columns="year").columns:
        tb[column] *= GWH_TO_TWH

    # Add a country column.
    tb["country"] = "United Kingdom"

    # Sanity checks.
    sanity_check_data(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
