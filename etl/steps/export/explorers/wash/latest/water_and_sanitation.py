"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import copy

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def get_color_scheme(resource, level_of_use_access, relative_to_population):
    """Determine color scheme based on dimensions."""

    # For share of population
    if relative_to_population == "share_of_population":
        if resource == "handwashing_facilities":
            if level_of_use_access == "basic":
                return "GnBu"
            elif level_of_use_access == "limited":
                return "YlGnBu"
            elif level_of_use_access == "no_facilities":
                return "YlOrRd"
        else:  # drinking_water or sanitation
            if level_of_use_access in ["safely_managed", "basic", "improved"]:
                return "YlGnBu"
            elif level_of_use_access == "limited":
                return "YlOrBr"
            else:  # unimproved or no_facilities
                return "YlOrRd"

    # For number of people
    else:
        if resource == "sanitation":
            if level_of_use_access in ["safely_managed", "basic", "improved"]:
                return "Greens"
            elif level_of_use_access == "limited":
                return "YlOrBr"
            else:  # unimproved or no_facilities
                return "YlOrRd"
        elif resource == "handwashing_facilities":
            if level_of_use_access == "basic":
                return "Purples"
            elif level_of_use_access == "limited":
                return "YlOrBr"
            else:  # no_facilities
                return "YlOrRd"
        else:  # drinking_water
            if level_of_use_access in ["safely_managed", "basic", "improved"]:
                return "PuBuGn"
            elif level_of_use_access == "limited":
                return "YlOrBr"
            else:  # unimproved or no_facilities
                return "YlOrRd"


def get_numeric_bins(resource, level_of_use_access, relative_to_population, residence=None):
    """Determine numeric bins based on dimensions."""

    if relative_to_population == "number_of_people":
        return "100000,,;1000000,,;10000000,,;100000000,,;1000000000"
    else:  # share_of_population
        # Special case for sanitation + unimproved
        if resource == "sanitation" and level_of_use_access == "unimproved":
            if residence == "total":
                return "10,,;20,,;40,,;60,,;80,,;100"
            else:  # urban or rural
                return "5,,;10,,;20,,;40,,;60,,;80"
        # Special case for drinking_water + rural + no_facilities
        elif resource == "drinking_water" and residence == "rural" and level_of_use_access == "no_facilities":
            return "5,,;10,,;20,,;40,,;50,,;60"
        # General cases
        elif resource == "sanitation" and level_of_use_access == "limited":
            return "10,,;20,,;30,,;40,,;50,,;60"
        elif level_of_use_access in ["safely_managed", "basic", "improved"]:
            return "20,,;40,,;60,,;80,,;100"
        elif level_of_use_access == "limited":
            return "5,,;10,,;20,,;30,,;40"
        elif level_of_use_access == "unimproved":
            return "5,,;10,,;20,,;40,,;60,,;80"
        else:  # no_facilities
            return "2.5,,;5,,;10,,;25,,;50"


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_explorer_config()

    # Update the color properties programmatically
    views = []
    for view in config["views"]:
        new_view = copy.deepcopy(view)
        dimensions = view["dimensions"]

        resource = dimensions["resource"]
        residence = dimensions["residence"]
        level_of_use_access = dimensions["level_of_use_access"]
        relative_to_population = dimensions["relative_to_population"]

        # Set color scheme
        color_scheme = get_color_scheme(resource, level_of_use_access, relative_to_population)
        new_view["indicators"]["y"][0]["display"]["colorScaleScheme"] = color_scheme

        # Set numeric bins
        numeric_bins = get_numeric_bins(resource, level_of_use_access, relative_to_population, residence)
        new_view["indicators"]["y"][0]["display"]["colorScaleNumericBins"] = numeric_bins

        # Set equal size bins based on relative_to_population
        equal_size_bins = relative_to_population == "number_of_people"
        new_view["indicators"]["y"][0]["display"]["colorScaleEqualSizeBins"] = equal_size_bins

        # Set min value to 0 (consistent across all views)
        new_view["indicators"]["y"][0]["display"]["colorScaleNumericMinValue"] = 0

        views.append(new_view)

    # Replace the views in the config
    config["views"] = views

    # Create explorer
    explorer = paths.create_explorer(config=config, explorer_name="water-and-sanitation")

    explorer.save()
