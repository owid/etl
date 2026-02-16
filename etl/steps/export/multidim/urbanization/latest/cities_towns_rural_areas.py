"""MDIM step for urbanization data - cities, towns, and rural areas."""

from etl.collection import combine_collections
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

    # Add grouped view for location types (stacked area chart)
    c.group_views(
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

    # Update view configurations and add colors
    for view in c.views:
        # Set stacked area chart config for grouped location views
        if view.dimensions.get("location_type") == "location_type_stacked":
            view.config = view.config.copy()
            metric = view.dimensions.get("metric")
            # Only set stack mode if metric dimension exists
            if metric:
                if metric == "popshare":
                    view.config["stackMode"] = "relative"
                    view.config["yAxis"] = {"min": 0, "max": 100}
                else:
                    view.config["stackMode"] = "absolute"

        # Map-only view for dominant settlement type
        if view.dimensions.get("metric") == "dominant_type":
            view.config = view.config.copy()
            view.config["chartTypes"] = []
            view.config["tab"] = "map"

        # Apply colors and display names
        edit_indicator_displays(view)

    #
    # Save outputs.
    #
    c.save()


def edit_indicator_displays(view):
    """Edit display names and colors for the views."""

    # Location type configuration
    LOCATION_CONFIG = {
        "urban_centre": {
            "color": COLOR_CITIES,
            "patterns": ["urban_centre"],
            "sort_order": 0,
        },
        "urban_cluster": {
            "color": COLOR_TOWNS,
            "patterns": ["urban_cluster"],
            "sort_order": 1,
        },
        "rural_total": {
            "color": COLOR_RURAL,
            "patterns": ["rural_total"],
        },
    }

    # Apply display properties to indicators
    for indicator in view.indicators.y:
        for key, config in LOCATION_CONFIG.items():
            if any(pattern in indicator.catalogPath for pattern in config["patterns"]):
                indicator.display = {"color": config["color"]}
                break
