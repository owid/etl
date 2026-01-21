"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load dataset without including data
    ds = paths.load_dataset("household")
    tb = ds.read("household", load_data=False)

    # Load grapher config from YAML
    config = paths.load_collection_config()

    # Create collection
    c = paths.create_collection(
        config=config,
        short_name="water-and-sanitation",
        explorer=True,
    )

    map_bins = {
        "regular_perc": "10,,;20,,;30,,;40,,;50,,;60,,;70,,;80,,;90,,;100",
        "focus_on_lower_perc": "5,,;10,,;15,,;20,,;25,,;30,,;35,,;40,,;45,,;0",
    }

    # Edit display
    for view in c.views:
        assert view.indicators.y is not None
        if len(view.indicators.y) == 1:
            col_name = view.indicators.y[0].catalogPath.split("#")[1]
            view.config["title"] = tb[col_name].metadata.title
            view.config["subtitle"] = tb[col_name].metadata.description_short
        for y in view.indicators.y:
            set_map_bins(y, view, map_bins)
    c.save()


def set_map_bins(y, view, map_bins):
    y.update_display(
        {
            "colorScaleNumericMinValue": 0,
        }
    )
    if y.display.get("unit") == "%":
        if check_lower_perc_columns(view):
            y.update_display({"colorScaleNumericBins": map_bins["focus_on_lower_perc"]})
        else:
            y.update_display({"colorScaleNumericBins": map_bins["regular_perc"]})


def check_lower_perc_columns(view):
    dim = view.dimensions
    resources = ["drinking_water", "sanitation"]
    levels = ["limited", "unimproved", "no_facilities"]
    return dim.get("resource") in resources and dim.get("level_of_use_access") in levels
