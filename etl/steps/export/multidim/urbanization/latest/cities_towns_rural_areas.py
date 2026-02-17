"""MDIM step for urbanization data - cities, towns, and rural areas."""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Common configuration for all charts
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "originUrl": "ourworldindata.org/urbanization",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "add-country",
}


def run() -> None:
    """
    Main function to process urbanization data and create multidimensional data views.
    """
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file
    config = paths.load_collection_config()

    # Load grapher datasets
    ds_grapher = paths.load_dataset("ghsl_countries")
    tb = ds_grapher.read("ghsl_countries", load_data=False)
    tb_dominant = ds_grapher.read("ghsl_countries_dominant_type", load_data=False)

    #
    # Process data - create two separate collections
    #

    # Collection 1: Main population/density data with location types
    c1 = paths.create_collection(
        config=config,
        tb=[tb],  # Include both tables to allow combined views later
        indicator_names=["value"],
        dimensions={
            "location_type": "*",
            "metric": [
                "population",
                "popshare",
                "density",
                "popshare_change",
            ],
            "data_type": "*",
        },
        common_view_config=MULTIDIM_CONFIG,
    )
    # Add grouped view for location types (stacked area chart)
    c1.group_views(
        groups=[
            {
                "dimension": "location_type",
                "choice_new_slug": "location_type_stacked",
                "choices": ["urban_centre", "urban_cluster", "rural_total"],
                "view_config": {
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["StackedArea"],
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                },
            }
        ]
    )

    # Collection 2: Dominant type data (has metric='dominant_type', data_type, and location_type dimensions)
    c2 = paths.create_collection(
        config=config,
        tb=tb_dominant,
        indicator_names=["value"],
        dimensions=["data_type", "location_type", "metric"],
        common_view_config=MULTIDIM_CONFIG,
    )

    # Merge the two collections
    c = combine_collections(
        [c1, c2],
        collection_name=paths.short_name,
        config=paths.load_collection_config(),
    )

    # Update view configurations and add colors
    for view in c.views:
        # Set stacked area chart config for grouped location views
        if view.dimensions.get("location_type") == "location_type_stacked":
            view.config = view.config.copy()
            metric = view.dimensions.get("metric")
            data_type = view.dimensions.get("data_type")

            # Add metadata for stacked views
            metadata = create_stacked_metadata(metric, data_type)
            view.metadata = metadata
            view.config["title"] = metadata["title"]
            view.config["subtitle"] = metadata["description_short"]

            # Only set stack mode if metric dimension exists
            if metric:
                # Use line charts for density and popshare_change (not stackable)
                if metric in ["density", "popshare_change"]:
                    view.config["chartTypes"] = ["LineChart"]
                    view.config["hasMapTab"] = False
                    view.config["tab"] = "chart"
                elif metric == "popshare":
                    view.config["stackMode"] = "relative"
                    view.config["yAxis"] = {"min": 0, "max": 100}
                else:
                    view.config["stackMode"] = "absolute"

        # Map-only view for dominant settlement type
        if view.dimensions.get("metric") == "dominant_type":
            view.config = view.config.copy()
            view.config["chartTypes"] = []
            view.config["tab"] = "map"

    #
    # Save outputs.
    #
    c.save()


def create_stacked_metadata(metric, data_type):
    """Create metadata for stacked location_type views."""
    # Define titles
    titles = {
        ("population", "estimates"): "Population in cities, towns and suburbs, and rural areas",
        ("population", "projections"): "Projected population in cities, towns and suburbs, and rural areas",
        ("popshare", "estimates"): "Share of population in cities, towns and suburbs, and rural areas",
        ("popshare", "projections"): "Projected share of population in cities, towns and suburbs, and rural areas",
        ("density", "estimates"): "Population density in cities, towns and suburbs, and rural areas",
        ("density", "projections"): "Projected population density in cities, towns and suburbs, and rural areas",
        (
            "popshare_change",
            "estimates",
        ): "Change in the share of population in cities, towns and suburbs, and rural areas",
        (
            "popshare_change",
            "projections",
        ): "Projected change in the share of population in cities, towns and suburbs, and rural areas",
    }

    # Define description_short
    descriptions = {
        (
            "population",
            "estimates",
        ): "Estimated number of people living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "population",
            "projections",
        ): "Projected number of people living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "estimates",
        ): "Estimated share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "projections",
        ): "Projected share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "density",
            "estimates",
        ): "Estimated population density in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "density",
            "projections",
        ): "Projected population density in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare_change",
            "estimates",
        ): "Change in the share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba), measured in percentage points over 5-year intervals. Positive values indicate a growing share; negative values indicate a declining share.",
        (
            "popshare_change",
            "projections",
        ): "Projected change in the share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba), measured in percentage points over 5-year intervals. Positive values indicate a growing share; negative values indicate a declining share.",
    }

    # Define description_key (varies by metric)
    base_description_key = [
        "The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) classifies areas as [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), or [rural areas](#dod:rural-areas-degurba) based on population density and settlement size rather than administrative boundaries. Developed by six international organizations and endorsed by the UN Statistical Commission in 2020, it provides consistent definitions for comparing urbanization across countries.",
        "Cities are defined as densely populated areas with a minimum density of 1,500 people per square kilometre and at least 50,000 people in total.",
        "Towns and suburbs are settlements outside cities where people live at moderate density. They have at least 300 people per square kilometre and at least 5,000 people in total. This includes smaller towns, suburban areas, and peri-urban areas around cities.",
        "Rural areas are places with less than 300 people per km² or a total population of less than 5,000.",
        "The classification uses 1 km² grid cells, combining satellite imagery with census data to map where people actually live.",
    ]

    # Add interpretation for popshare_change
    if metric == "popshare_change":
        interpretation_text = "For cities and towns, positive values indicate urbanization (a growing share of the population living in these areas), while negative values indicate the opposite. For rural areas, positive values indicate a growing rural share, while negative values indicate declining rural population share (urbanization)."
        description_key = base_description_key + [interpretation_text]
    else:
        description_key = base_description_key

    # Add common caveats
    description_key += [
        "Different countries use different definitions and criteria to define urban and rural areas, such as population size, population density, infrastructure, employment patterns, or official city status. The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) applies a single global standard using population density grids, meaning its classifications won't always match official city boundaries and therefore urbanization rates may differ from country-reported figures.",
        "For small countries, values can change sharply when an entire area shifts from one classification to another.",
    ]

    return {
        "title": titles.get((metric, data_type), ""),
        "description_short": descriptions.get((metric, data_type), ""),
        "description_key": description_key,
    }
