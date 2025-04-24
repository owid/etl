"""Load a meadow dataset and create a garden dataset."""

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
    "Tightly built city areas": "%",
    "Buildings surrounded by green areas": "%",
    "Dense single-story lightweight housing": "%",
    "Large low-rise buildings with paved surroundings": "%",
    "Scattered buildings in natural settings": "%",
    "Industrial zones": "%",
    "Forests, parks, and greenery": "%",
    "Rocky or sandy land": "%",
    "Water bodies": "%",
    "Unknown": "%",
    "Average daily photovoltaic potential": "kilowatt-hours per kilowatt-peak",
    "Share of population living in the high green area": "%",
    "Road network density": "m/m2²",
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
    "Tightly built city areas": "%",
    "Buildings surrounded by green areas": "%",
    "Dense single-story lightweight housing": "%",
    "Large low-rise buildings with paved surroundings": "%",
    "Scattered buildings in natural settings": "%",
    "Industrial zones": "%",
    "Forests, parks, and greenery": "%",
    "Rocky or sandy land": "%",
    "Water bodies": "%",
    "Unknown": "%",
    "Average daily photovoltaic potential": "kWh/kWp",
    "Share of population living in the high green area": "%",
    "Road network density": "m/m²",
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
    tb = tb.drop(columns=["city"])

    # Map units and short units to the table
    tb["unit"] = tb["indicator"].map(UNITS)
    tb["short_unit"] = tb["indicator"].map(SHORT_UNITS)
    print(tb["indicator"].unique())

    # Improve table format.
    tb = tb.format(["country", "year", "indicator", "unit", "short_unit"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
