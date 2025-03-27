"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define unit and short unit mappings for indicators
UNITS = {
    "Share of population living in Low Elevation Coastal Zones (<5m) (%)": "%",
    "Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day": "%",
    "Average download speed": "Mbps",
    "Annual mean temperature in the decade": "°C",
    "Annual precipitation in the decade": "mm",
    "CO2 emissions per capita": "tonnes",
    "Greenhouse gas emissions per capita": "t",
    "Share of energy emissions in total emissions": "%",
    "Share of residential emissions in total emissions": "%",
    "Share of industrial emissions in total emissions": "%",
    "Share of transport emissions in total emissions": "%",
    "Share of waste emissions in total emissions": "%",
    "Share of agricultural emissions in total emissions": "%",
}


SHORT_UNITS = {
    "Share of population living in Low Elevation Coastal Zones (<5m) (%)": "%",
    "Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day": "%",
    "Average download speed": "Mbps",
    "Annual mean temperature in the decade": "°C",
    "Annual precipitation in the decade": "mm",
    "CO2 emissions per capita": "t",
    "Greenhouse gas emissions per capita": "t",
    "Share of energy emissions in total emissions": "%",
    "Share of residential emissions in total emissions": "%",
    "Share of industrial emissions in total emissions": "%",
    "Share of transport emissions in total emissions": "%",
    "Share of waste emissions in total emissions": "%",
    "Share of agricultural emissions in total emissions": "%",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ghsl_stats_in_the_city")

    # Read table from meadow dataset.
    tb = ds_meadow.read("ghsl_stats_in_the_city")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb["country_city"] = tb["city"] + " (" + tb["country"] + ")"
    tb = tb.drop(columns=["country", "city"])
    tb = tb.rename(columns={"country_city": "country"})

    # Map units and short units to the table
    tb["unit"] = tb["indicator"].map(UNITS)
    tb["short_unit"] = tb["indicator"].map(SHORT_UNITS)

    # Improve table format.
    tb = tb.format(["country", "year", "indicator", "unit", "short_unit"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
