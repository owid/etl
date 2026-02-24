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
    # Add grouped views for location types (only for population and popshare)
    c1.group_views(
        groups=[
            # Stacked area chart: cities, towns, rural
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
                    "selectedFacetStrategy": "entity",
                },
            },
            # Line chart: urban areas (total) vs rural
            {
                "dimension": "location_type",
                "choice_new_slug": "urban_vs_rural",
                "choices": ["urban_total", "rural_total"],
                "view_config": {
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                    "selectedFacetStrategy": "entity",
                },
            },
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

    # Group estimates and projections together for all views
    c.group_views(
        groups=[
            {
                "dimension": "data_type",
                "choice_new_slug": "estimates_and_projections",
                "choices": ["projections", "estimates"],
                "view_config": {
                    "hasMapTab": True,
                    "tab": "map",
                    "map": {
                        "time": 2020
                    },  # Show 2020 by default since it's the most recent year with estimates and the start of projections
                    "chartTypes": ["LineChart"],
                    "hideAnnotationFieldsInTitle": {"time": True},
                },
            }
        ]
    )

    # Remove map tab from grouped location_type views (location_type_stacked and urban_vs_rural)
    for view in c.views:
        location_type = view.dimensions.get("location_type")
        if location_type in ["location_type_stacked", "urban_vs_rural"]:
            view.config = view.config.copy() if view.config else {}
            view.config["hasMapTab"] = False
            view.config["tab"] = "chart"

    # Update view configurations and add colors
    for view in c.views:
        data_type = view.dimensions.get("data_type")
        location_type = view.dimensions.get("location_type")
        metric = view.dimensions.get("metric")

        # Add metadata for all views with grouped data_type (estimates_and_projections)
        # These views inherit metadata from the indicator but need updated description_key
        if data_type == "estimates_and_projections" and not view.metadata:
            # Skip grouped location views (handled separately below)
            if location_type not in ["location_type_stacked", "urban_vs_rural"]:
                metadata = create_individual_view_metadata(location_type, metric, data_type)
                if metadata:
                    view.metadata = metadata
                    view.config = view.config.copy() if view.config else {}
                    view.config["title"] = metadata.get("title", "")
                    view.config["subtitle"] = metadata.get("description_short", "")

        # Set stacked area chart config for grouped location views
        if location_type == "location_type_stacked":
            view.config = view.config.copy() if view.config else {}

            # Add metadata for stacked views
            metadata = create_stacked_metadata(metric, data_type)
            view.metadata = metadata
            view.config["title"] = metadata["title"]
            view.config["subtitle"] = metadata["description_short"]

            # Only set stack mode if metric dimension exists
            if metric:
                if metric == "popshare":
                    view.config["stackMode"] = "relative"
                    view.config["yAxis"] = {"min": 0, "max": 100}
                else:
                    view.config["stackMode"] = "absolute"

        # Set config for urban vs rural line chart views
        if location_type == "urban_vs_rural":
            view.config = view.config.copy() if view.config else {}

            # Add metadata for urban vs rural views
            metadata = create_urban_vs_rural_metadata(metric, data_type)
            view.metadata = metadata
            view.config["title"] = metadata["title"]
            view.config["subtitle"] = metadata["description_short"]

            # Special config for popshare (0-100%)
            if metric == "popshare":
                view.config["yAxis"] = {"min": 0, "max": 100}

        # Map-only view for dominant settlement type
        if metric == "dominant_type":
            view.config = view.config.copy() if view.config else {}
            view.config["chartTypes"] = []
            view.config["tab"] = "map"

    # Remove grouped location views for popshare_change and density
    # (these metrics don't make sense in stacked area or grouped views)
    c.drop_views(
        dimensions={
            "location_type": ["location_type_stacked", "urban_vs_rural"],
            "metric": ["popshare_change", "density"],
        }
    )

    # Uncomment if we want to include both estimates and projections in separate views
    c.drop_views(
        dimensions={
            "data_type": ["estimates", "projections"],
        }
    )
    #
    # Save outputs.
    #
    c.save()


def _individual_title(location_type: str, metric: str, data_type: str) -> str:
    """Generate a chart title for individual (non-grouped) views."""
    loc_names = {
        "urban_centre": "cities",
        "urban_cluster": "towns and suburbs",
        "rural_total": "rural areas",
        "urban_total": "urban areas",
        "by_dominant_type": "dominant settlement type",
    }
    base = loc_names.get(location_type, location_type)

    if metric == "population":
        return f"Population in {base}"
    if metric == "popshare":
        return f"Share of population living in {base}"
    if metric == "density":
        return f"Population density in {base}"
    if metric == "popshare_change":
        if location_type == "rural_total":
            return "Annual change in the rural population share"
        return f"Annual change in the population share living in {base}"
    if metric == "dominant_type":
        return "Dominant settlement type"

    # fallback for any other metric names
    return f"{metric.replace('_', ' ').title()} in {base}"


def create_stacked_metadata(metric, data_type):
    """Create metadata for stacked location_type views."""
    # Define titles
    titles = {
        ("population", "estimates"): "Population in cities, towns and suburbs, and rural areas",
        ("population", "projections"): "Projected population in cities, towns and suburbs, and rural areas",
        ("population", "estimates_and_projections"): "Population in cities, towns and suburbs, and rural areas",
        ("popshare", "estimates"): "Share of population in cities, towns and suburbs, and rural areas",
        ("popshare", "projections"): "Projected share of population in cities, towns and suburbs, and rural areas",
        ("popshare", "estimates_and_projections"): "Share of population in cities, towns and suburbs, and rural areas",
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
            "population",
            "estimates_and_projections",
        ): "Number of people living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "estimates",
        ): "Estimated share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "projections",
        ): "Projected share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "estimates_and_projections",
        ): "Share of population living in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
    }

    # Define description_key - same for all data types
    description_key = [
        "The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) classifies areas as [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), or [rural areas](#dod:rural-areas-degurba) based on population density and settlement size rather than administrative boundaries. Developed by six international organizations and endorsed by the UN Statistical Commission in 2020, it provides consistent definitions for comparing urbanization across countries.",
        "Cities are defined as densely populated areas with a minimum density of 1,500 people per square kilometre and at least 50,000 people in total.",
        "Towns and suburbs are settlements outside cities where people live at moderate density. They have at least 300 people per square kilometre and at least 5,000 people in total. This includes smaller towns, suburban areas, and peri-urban areas around cities.",
        "Rural areas are places with less than 300 people per km² or a total population of less than 5,000.",
        "The classification uses 1 km² grid cells, combining satellite imagery with census data to map where people actually live.",
    ]

    # Add data source information based on data_type
    if data_type == "estimates":
        description_key.append(
            "For the years 1950–1975, there are no detailed maps showing where people lived within countries. So instead of using grid-level or satellite data, the estimates are reconstructed using national statistics from the UN. From 1975 onwards, population is mapped to 1 km² grid cells by combining census data with satellite imagery of built-up areas from the [Global Human Settlement Layer](https://human-settlement.emergency.copernicus.eu/)."
        )
    elif data_type == "projections":
        description_key.append(
            "From 2020 onwards, population distribution and built-up areas are projected at 1 km² resolution using the [Cities and Rural Integrated Spatial Projections spatial model](https://www.researchgate.net/publication/384062691_Calibration_of_the_CRISP_model_A_global_assessment_of_local_built-up_area_presence). The projections follow the UN World Population Prospects 2024 medium scenario, and country totals are aligned with UN projections."
        )
    elif data_type == "estimates_and_projections":
        description_key.append(
            "For the years 1950–1975, there are no detailed maps showing where people lived within countries. So instead of using grid-level or satellite data, the estimates are reconstructed using national statistics from the UN. From 1975 onwards, population is mapped to 1 km² grid cells by combining census data with satellite imagery of built-up areas from the [Global Human Settlement Layer](https://human-settlement.emergency.copernicus.eu/). From 2020 onwards, population distribution and built-up areas are projected at 1 km² resolution using the [Cities and Rural Integrated Spatial Projections spatial model](https://www.researchgate.net/publication/384062691_Calibration_of_the_CRISP_model_A_global_assessment_of_local_built-up_area_presence). The projections follow the UN World Population Prospects 2024 medium scenario, and country totals are aligned with UN projections."
        )

    description_key.extend(
        [
            "Different countries use different definitions and criteria to define urban and rural areas, such as population size, population density, infrastructure, employment patterns, or official city status. The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) applies a single global standard using population density grids, meaning its classifications won't always match official city boundaries and therefore urbanization rates may differ from country-reported figures.",
            "For small countries, values can change sharply when an entire area shifts from one classification to another.",
        ]
    )

    return {
        "title": titles.get((metric, data_type), ""),
        "description_short": descriptions.get((metric, data_type), ""),
        "description_key": description_key,
    }


def create_urban_vs_rural_metadata(metric, data_type):
    """Create metadata for urban vs rural views."""
    # Define titles
    titles = {
        ("population", "estimates"): "Population in urban and rural areas",
        ("population", "projections"): "Projected population in urban and rural areas",
        ("population", "estimates_and_projections"): "Population in urban and rural areas",
        ("popshare", "estimates"): "Share of population in urban and rural areas",
        ("popshare", "projections"): "Projected share of population in urban and rural areas",
        ("popshare", "estimates_and_projections"): "Share of population in urban and rural areas",
    }

    # Define description_short
    descriptions = {
        (
            "population",
            "estimates",
        ): "Estimated number of people living in urban areas ([cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba) combined) and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "population",
            "projections",
        ): "Projected number of people living in urban areas ([cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba) combined) and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "population",
            "estimates_and_projections",
        ): "Number of people living in urban areas ([cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba) combined) and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "estimates",
        ): "Estimated share of population living in urban areas ([cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba) combined) and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "projections",
        ): "Projected share of population living in urban areas ([cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba) combined) and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
        (
            "popshare",
            "estimates_and_projections",
        ): "Share of population living in urban areas ([cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba) combined) and [rural areas](#dod:rural-areas-degurba). Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries.",
    }

    # Define description_key - same for all data types
    description_key = [
        "The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) classifies areas as [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), or [rural areas](#dod:rural-areas-degurba) based on population density and settlement size rather than administrative boundaries. Developed by six international organizations and endorsed by the UN Statistical Commission in 2020, it provides consistent definitions for comparing urbanization across countries.",
        "Urban areas combine cities (at least 1,500 people per km², 50,000 total) and towns and suburbs (at least 300 people per km², 5,000 total).",
        "Rural areas are places with less than 300 people per km² or a total population of less than 5,000.",
        "The classification uses 1 km² grid cells, combining satellite imagery with census data to map where people actually live.",
    ]

    # Add data source information based on data_type
    if data_type == "estimates":
        description_key.append(
            "For the years 1950–1975, there are no detailed maps showing where people lived within countries. So instead of using grid-level or satellite data, the estimates are reconstructed using national statistics from the UN. From 1975 onwards, population is mapped to 1 km² grid cells by combining census data with satellite imagery of built-up areas from the [Global Human Settlement Layer](https://human-settlement.emergency.copernicus.eu/)."
        )
    elif data_type == "projections":
        description_key.append(
            "From 2020 onwards, population distribution and built-up areas are projected at 1 km² resolution using the [Cities and Rural Integrated Spatial Projections spatial model](https://www.researchgate.net/publication/384062691_Calibration_of_the_CRISP_model_A_global_assessment_of_local_built-up_area_presence). The projections follow the UN World Population Prospects 2024 medium scenario, and country totals are aligned with UN projections."
        )
    elif data_type == "estimates_and_projections":
        description_key.append(
            "For the years 1950–1975, there are no detailed maps showing where people lived within countries. So instead of using grid-level or satellite data, the estimates are reconstructed using national statistics from the UN. From 1975 onwards, population is mapped to 1 km² grid cells by combining census data with satellite imagery of built-up areas from the [Global Human Settlement Layer](https://human-settlement.emergency.copernicus.eu/). From 2020 onwards, population distribution and built-up areas are projected at 1 km² resolution using the [Cities and Rural Integrated Spatial Projections spatial model](https://www.researchgate.net/publication/384062691_Calibration_of_the_CRISP_model_A_global_assessment_of_local_built-up_area_presence). The projections follow the UN World Population Prospects 2024 medium scenario, and country totals are aligned with UN projections."
        )

    description_key.extend(
        [
            "Different countries use different definitions and criteria to define urban and rural areas, such as population size, population density, infrastructure, employment patterns, or official city status. The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) applies a single global standard using population density grids, meaning its classifications won't always match official city boundaries and therefore urbanization rates may differ from country-reported figures.",
            "For small countries, values can change sharply when an entire area shifts from one classification to another.",
        ]
    )

    return {
        "title": titles.get((metric, data_type), ""),
        "description_short": descriptions.get((metric, data_type), ""),
        "description_key": description_key,
    }


def create_individual_view_metadata(location_type, metric, data_type):
    """Create metadata for individual (non-grouped) views with data_type='estimates_and_projections'."""
    if data_type != "estimates_and_projections":
        return None

    # Create description_short based on location_type and metric
    location_descriptions = {
        "urban_centre": "in [cities](#dod:cities-degurba)",
        "urban_cluster": "in [towns and suburbs](#dod:towns-degurba)",
        "rural_total": "in [rural areas](#dod:rural-areas-degurba)",
        "urban_total": "in urban areas (the sum of [cities](#dod:cities-degurba) and [towns and suburbs](#dod:towns-degurba))",
        "by_dominant_type": "",  # Dominant type has its own special description
    }

    metric_descriptions = {
        "population": "Number of people living",
        "popshare": "Share of population living",
        "density": "Number of people per square kilometre",
        "popshare_change": "Annual rate of change in the population share living",
        "dominant_type": "Shows whether most people live in [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), or [rural areas](#dod:rural-areas-degurba)",
    }

    # Build description_short
    if metric == "dominant_type":
        description_short = (
            metric_descriptions[metric]
            + ". Settlement types are identified using satellite imagery and population data, applying the same density and size thresholds across all countries."
        )
    else:
        metric_text = metric_descriptions.get(metric, "")
        location_text = location_descriptions.get(location_type, "")

        description_short = f"{metric_text} {location_text}"

        if metric == "popshare_change":
            if location_type == "rural_total":
                description_short += ", measured in percentage points. Positive values indicate a growing share; negative values indicate a declining share (urbanization)"
            else:
                description_short += ", measured in percentage points. Positive values indicate a growing share; negative values indicate a declining share"

        description_short += ", identified using satellite imagery and population data, applying the same density and size thresholds across all countries."

    title = _individual_title(location_type, metric, data_type)

    # Base description_key with DEGURBA overview
    description_key = [
        "The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) classifies areas as [cities](#dod:cities-degurba), [towns and suburbs](#dod:towns-degurba), or [rural areas](#dod:rural-areas-degurba) based on population density and settlement size rather than administrative boundaries. Developed by six international organizations and endorsed by the UN Statistical Commission in 2020, it provides consistent definitions for comparing urbanization across countries.",
    ]

    # Add location-specific definitions
    if location_type == "urban_centre":
        description_key.append(
            "Cities are defined as densely populated areas with a minimum density of 1,500 people per square kilometre and at least 50,000 people in total."
        )
    elif location_type == "urban_cluster":
        description_key.append(
            "Towns and suburbs are settlements outside cities where people live at moderate density. They have at least 300 people per square kilometre and at least 5,000 people in total. This includes smaller towns, suburban areas, and peri-urban areas around cities."
        )
    elif location_type == "rural_total":
        description_key.append(
            "Rural areas are places with less than 300 people per km² or a total population of less than 5,000."
        )
    elif location_type == "urban_total":
        description_key.append(
            "Urban areas include both cities (at least 1,500 people per km², 50,000 total) and towns and suburbs (at least 300 people per km², 5,000 total)."
        )
    elif location_type == "by_dominant_type":
        description_key.append(
            "Cities are defined as densely populated areas with a minimum density of 1,500 people per square kilometre and at least 50,000 people in total. Towns and suburbs are settlements outside cities where people live at moderate density. They have at least 300 people per square kilometre and at least 5,000 people in total. Rural areas are everything else."
        )

    # Add data source (classification method)
    description_key.append(
        "The classification uses 1 km² grid cells, combining satellite imagery with census data to map where people actually live."
    )

    # Add methodology for combined estimates and projections
    description_key.append(
        "For the years 1950–1975, there are no detailed maps showing where people lived within countries. So instead of using grid-level or satellite data, the estimates are reconstructed using national statistics from the UN. From 1975 onwards, population is mapped to 1 km² grid cells by combining census data with satellite imagery of built-up areas from the [Global Human Settlement Layer](https://human-settlement.emergency.copernicus.eu/). From 2020 onwards, population distribution and built-up areas are projected at 1 km² resolution using the [Cities and Rural Integrated Spatial Projections spatial model](https://www.researchgate.net/publication/384062691_Calibration_of_the_CRISP_model_A_global_assessment_of_local_built-up_area_presence). The projections follow the UN World Population Prospects 2024 medium scenario, and country totals are aligned with UN projections."
    )

    # Add metric-specific explanation
    if metric == "popshare_change":
        if location_type == "rural_total":
            description_key.append(
                "To measure how quickly urbanization is happening, we calculate the average annual rate at which the share of people living in rural areas has grown or shrunk over a 5-year period. A positive value means the rural share grew, while a negative value means it fell (indicating urbanization)."
            )
        else:
            area_name = (
                "cities"
                if location_type == "urban_centre"
                else "towns and suburbs"
                if location_type == "urban_cluster"
                else "urban areas"
            )
            description_key.append(
                f"To measure how quickly urbanization is happening, we calculate the average annual rate at which the share of people living in {area_name} has grown or shrunk over a 5-year period. A positive value means a larger share of the population was living in {area_name} by the end of the period — in other words, urbanization was happening. A negative value means the opposite: the share living in {area_name} shrank."
            )

    # Add caveats
    description_key.extend(
        [
            "Different countries use different definitions and criteria to define urban and rural areas, such as population size, population density, infrastructure, employment patterns, or official city status. The [Degree of Urbanization](https://human-settlement.emergency.copernicus.eu/degurba.php) applies a single global standard using population density grids, meaning its classifications won't always match official city boundaries and therefore urbanization rates may differ from country-reported figures.",
            "For small countries, values can change sharply when an entire area shifts from one classification to another.",
        ]
    )

    return {
        "title": title,
        "description_short": description_short,
        "description_key": description_key,
    }
