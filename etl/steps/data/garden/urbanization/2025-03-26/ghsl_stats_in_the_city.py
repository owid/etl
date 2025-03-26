"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define unit and short unit mappings for indicators
UNITS = {
    "Share of population living in Low Elevation Coastal Zones (<10m) (%)": "%",
    "Share of green area in built-up area (%)": "%",
    "Share of adult population (15-64 years) (%)": "%",
    "Share of female population (%)": "%",
    "Share of male population (%)": "%",
    "Share of old population (> 65 years) (%)": "%",
    "Share of young population (0-14 years) (%)": "%",
    "Coldwave occurrence (events per year)": "events per year",
    "Drought occurrence (events per year)": "events per year",
    "Earthquake occurrence (events per year)": "events per year",
    "Extreme wind occurrence (events per year)": "events per year",
    "Flood occurrence (events per year)": "events per year",
    "Heatwave occurrence (events per year)": "events per year",
    "Landslide occurrence (events per year)": "events per year",
    "Tropical Cyclone occurrence (events per year)": "events per year",
    "Tsunami occurrence (events per year)": "events per year",
    "Volcano occurrence (events per year)": "events per year",
}

SHORT_UNITS = {
    "Share of population living in Low Elevation Coastal Zones (<10m) (%)": "%",
    "Share of green area in built-up area (%)": "%",
    "Share of adult population (15-64 years) (%)": "%",
    "Share of female population (%)": "%",
    "Share of male population (%)": "%",
    "Share of old population (> 65 years) (%)": "%",
    "Share of young population (0-14 years) (%)": "%",
    "Coldwave occurrence (events per year)": "events/year",
    "Drought occurrence (events per year)": "events/year",
    "Earthquake occurrence (events per year)": "events/year",
    "Extreme wind occurrence (events per year)": "events/year",
    "Flood occurrence (events per year)": "events/year",
    "Heatwave occurrence (events per year)": "events/year",
    "Landslide occurrence (events per year)": "events/year",
    "Tropical Cyclone occurrence (events per year)": "events/year",
    "Tsunami occurrence (events per year)": "events/year",
    "Volcano occurrence (events per year)": "events/year",
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
    # Map units and short units to the table
    tb["unit"] = tb["indicator"].map(UNITS)
    tb["short_unit"] = tb["indicator"].map(SHORT_UNITS)

    tb = tb.drop("city", axis=1)

    # Improve table format.
    tb = tb.format(["country", "year", "indicator", "unit", "short_unit"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
