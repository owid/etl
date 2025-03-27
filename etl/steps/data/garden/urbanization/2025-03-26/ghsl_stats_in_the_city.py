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
    "Number of hospitals": "hospitals",
    "Number of pharmacies": "pharmacies",
    "Number of hospitals per capita": "hospitals per 1,000 people",
    "Number of pharmacies per capita": "pharmacies per 1,000 people",
    "Number of hospitals per urban centre area": "hospitals per km²",
    "Number of pharmacies per urban centre area": "pharmacies per km²",
    "Share of urban centre population within 1 km of a hospital": "%",
    "Share of urban centre population within 1 km of a pharmacy": "%",
    "Population within 1 km of a hospital": "people",
    "Population within 1 km of a pharmacy": "people",
    "Average download speed": "Mbps",
    "Life expectancy": "years",
    "Annual mean temperature in the decade": "°C",
    "Annual precipitation in the decade": "mm",
}

SHORT_UNITS = {
    "Share of population living in Low Elevation Coastal Zones (<5m) (%)": "%",
    "Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day": "%",
    "Number of hospitals": "",
    "Number of pharmacies": "",
    "Number of hospitals per capita": "",
    "Number of pharmacies per capita": "",
    "Number of hospitals per urban centre area": "hospitals/km²",
    "Number of pharmacies per urban centre area": "pharmacies/km²",
    "Share of urban centre population within 1 km of a hospital": "%",
    "Share of urban centre population within 1 km of a pharmacy": "%",
    "Population within 1 km of a hospital": "",
    "Population within 1 km of a pharmacy": "",
    "Average download speed": "Mbps",
    "Life expectancy": "",
    "Annual mean temperature in the decade": "°C",
    "Annual precipitation in the decade": "mm",
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
    tb = tb.drop("city", axis=1)

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
