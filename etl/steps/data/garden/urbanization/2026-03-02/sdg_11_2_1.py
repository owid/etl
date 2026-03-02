"""Garden step for SDG 11.2.1 - Access to Public Transport.

Creates two tables:
1. City-level data: Individual cities with their public transport access metrics
2. Country-level data: Aggregated country averages across all cities
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sdg_11_2_1")

    # Read table from meadow dataset.
    tb = ds_meadow["sdg_11_2_1"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names for both city and country tables
    tb = paths.regions.harmonize_names(tb)

    #
    # Create city-level table
    #
    tb_cities = tb.copy()

    # Create city_country column for uniqueness in the grapher
    # This ensures cities with same/similar names in different countries are distinct
    # Convert to string first to handle Categorical types
    tb_cities["city_country"] = tb_cities["city"].astype(str) + ", " + tb_cities["country"].astype(str)

    # Select relevant columns for city-level table
    tb_cities = tb_cities[
        [
            "country",
            "city_country",
            "year",
            "public_transport_access",
        ]
    ]

    # Format the city-level table
    tb_cities = tb_cities.format(["country", "city_country", "year"], short_name="sdg_11_2_1_city")

    #
    # Create country-level table
    #
    # Calculate country-level aggregates (simple mean across cities)
    tb_countries = (
        tb.groupby(["country", "year"], observed=True)
        .agg(
            {
                "public_transport_access": "mean",
            }
        )
        .reset_index()
    )

    # Format the country-level table
    tb_countries = tb_countries.format(["country", "year"], short_name="sdg_11_2_1_country")

    #
    # Save outputs.
    #
    # Create a new garden dataset with both tables
    ds_garden = paths.create_dataset(tables=[tb_cities, tb_countries])

    # Save changes in the new garden dataset.
    ds_garden.save()
