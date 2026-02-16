"""MDIM step for urbanization data - cities, towns, and rural areas."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Color constants for location types
COLOR_CITIES = "#4C6A9C"
COLOR_TOWNS = "#883039"
COLOR_RURAL = "#578145"

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "chartTypes": ["StackedDiscreteBar"],
    "originUrl": "ourworldindata.org/urbanization",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "add-country",
}

# Define dimensions for all variables
DIMENSIONS_DETAILS = {
    # Population - absolute
    "population_urban_centre_estimates": {
        "location_type": "cities",
        "metric": "population",
        "data_type": "estimates",
        "original_short_name": "urbanization_population",
    },
    "population_urban_cluster_estimates": {
        "location_type": "towns",
        "metric": "population",
        "data_type": "estimates",
        "original_short_name": "urbanization_population",
    },
    "population_rural_total_estimates": {
        "location_type": "rural_areas",
        "metric": "population",
        "data_type": "estimates",
        "original_short_name": "urbanization_population",
    },
    "population_urban_centre_projections": {
        "location_type": "cities",
        "metric": "population",
        "data_type": "projections",
        "original_short_name": "urbanization_population",
    },
    "population_urban_cluster_projections": {
        "location_type": "towns",
        "metric": "population",
        "data_type": "projections",
        "original_short_name": "urbanization_population",
    },
    "population_rural_total_projections": {
        "location_type": "rural_areas",
        "metric": "population",
        "data_type": "projections",
        "original_short_name": "urbanization_population",
    },
    # Population share - relative
    "popshare_urban_centre_estimates": {
        "location_type": "cities",
        "metric": "popshare",
        "data_type": "estimates",
        "original_short_name": "urbanization_population",
    },
    "popshare_urban_cluster_estimates": {
        "location_type": "towns",
        "metric": "popshare",
        "data_type": "estimates",
        "original_short_name": "urbanization_population",
    },
    "popshare_rural_total_estimates": {
        "location_type": "rural_areas",
        "metric": "popshare",
        "data_type": "estimates",
        "original_short_name": "urbanization_population",
    },
    "popshare_urban_centre_projections": {
        "location_type": "cities",
        "metric": "popshare",
        "data_type": "projections",
        "original_short_name": "urbanization_population",
    },
    "popshare_urban_cluster_projections": {
        "location_type": "towns",
        "metric": "popshare",
        "data_type": "projections",
        "original_short_name": "urbanization_population",
    },
    "popshare_rural_total_projections": {
        "location_type": "rural_areas",
        "metric": "popshare",
        "data_type": "projections",
        "original_short_name": "urbanization_population",
    },
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

    # Load grapher dataset
    ds_grapher = paths.load_dataset("ghsl_countries")
    tb = ds_grapher.read("ghsl_countries", reset_index=False)

    #
    # Process data.
    #
    # Add dimension metadata to columns based on their measure type
    for col, details in DIMENSIONS_DETAILS.items():
        if col in tb.columns:
            # Set dimensions (all keys except original_short_name)
            tb[col].m.dimensions = {k: v for k, v in details.items() if k != "original_short_name"}
            # Set original_short_name if present
            if "original_short_name" in details:
                tb[col].m.original_short_name = details["original_short_name"]

    # Create collection - this will automatically generate views from dimensions
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["urbanization_population"],
        dimensions=["location_type", "metric", "data_type"],
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add stacked chart configuration for combined views
    for view in c.views:
        # Update title and subtitle based on view dimensions
        location_type = view.dimensions.get("location_type")
        metric = view.dimensions.get("metric")
        data_type = view.dimensions.get("data_type")

        # Create a copy of the config to avoid shared references
        view.config = view.config.copy()

        # Generate dynamic title and subtitle
        view.config["title"] = generate_title(location_type, metric, data_type)
        view.config["subtitle"] = generate_subtitle(location_type, metric, data_type)

        # Set up stacked charts when showing all location types
        if location_type == "all":
            view.config["chartTypes"] = ["StackedArea"]
            if metric == "popshare":
                view.config["stackMode"] = "relative"
                view.config["yAxis"] = {"min": 0, "max": 100}
            else:
                view.config["stackMode"] = "absolute"

        # Set display properties and colors
        edit_indicator_displays(view)

        # Update metadata
        view.metadata = {
            "description_short": view.config["subtitle"],
            "presentation": {
                "title_public": view.config["title"],
            },
        }

    #
    # Save outputs.
    #
    c.save()


def generate_title(location_type, metric, data_type):
    """Generate dynamic title based on dimensions."""
    # Location type mapping
    location_map = {
        "cities": "cities",
        "towns": "towns",
        "rural_areas": "rural areas",
        "all": "cities, towns, and rural areas",
    }

    # Metric mapping
    if metric == "population":
        metric_text = "Population in"
    elif metric == "popshare":
        metric_text = "Share of population in"
    else:
        metric_text = "Population in"

    # Data type suffix
    if data_type == "projections":
        data_suffix = " (projections)"
    elif data_type == "estimates":
        data_suffix = ""
    else:
        data_suffix = ""

    location_text = location_map.get(location_type, "")

    return f"{metric_text} {location_text}{data_suffix}"


def generate_subtitle(location_type, metric, data_type):
    """Generate dynamic subtitle based on dimensions."""
    # Location type mapping with definitions
    location_map = {
        "cities": "[cities (urban centres)](#dod:urban-centre)",
        "towns": "[towns (urban clusters)](#dod:urban-cluster)",
        "rural_areas": "[rural areas](#dod:rural-area)",
        "all": "[cities](#dod:urban-centre), [towns](#dod:urban-cluster), and [rural areas](#dod:rural-area)",
    }

    location_text = location_map.get(location_type, "")

    if metric == "population":
        return f"Total population living in {location_text}."
    elif metric == "popshare":
        return f"Share of total population living in {location_text}."

    return ""


def edit_indicator_displays(view):
    """Edit display names and colors for the views."""
    # Location type configuration
    LOCATION_CONFIG = {
        "cities": {
            "name": "Cities",
            "color": COLOR_CITIES,
            "patterns": ["urban_centre"],
        },
        "towns": {
            "name": "Towns",
            "color": COLOR_TOWNS,
            "patterns": ["urban_cluster"],
        },
        "rural_areas": {
            "name": "Rural areas",
            "color": COLOR_RURAL,
            "patterns": ["rural_total"],
        },
    }

    # Apply display properties to indicators
    for indicator in view.indicators.y:
        for config in LOCATION_CONFIG.values():
            if any(pattern in indicator.catalogPath for pattern in config["patterns"]):
                indicator.display = {"name": config["name"], "color": config["color"]}
                break

    # Sort indicators: cities → towns → rural areas
    def get_location_index(ind):
        if "urban_centre" in ind.catalogPath:
            return 0
        elif "urban_cluster" in ind.catalogPath:
            return 1
        elif "rural_total" in ind.catalogPath:
            return 2
        return 3

    view.indicators.y = sorted(view.indicators.y, key=get_location_index)
