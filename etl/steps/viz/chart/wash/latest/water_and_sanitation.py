"""Load the WASH grapher dataset and create the water and sanitation multidim (shown as a data explorer)."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Map bins for indicators measured as a share of the population.
MAP_BINS = {
    "regular_perc": "10,,;20,,;30,,;40,,;50,,;60,,;70,,;80,,;90,,;100",
    "focus_on_lower_perc": "5,,;10,,;15,,;20,,;25,,;30,,;35,,;40,,;45,,;0",
}


def run() -> None:
    # Load the grapher dataset's metadata (no data needed).
    ds = paths.load_dataset("household")
    tb = ds.read("household", load_data=False)

    # Load the chart config.
    config = paths.load_config()

    # Create the multidim.
    c = paths.create_chart(config=config)

    # Take each view's title and subtitle from its indicator's metadata, and set map bins.
    for view in c.views:
        assert view.indicators.y is not None
        if len(view.indicators.y) == 1:
            col_name = view.indicators.y[0].catalogPath.split("#")[1]
            view.config = view.config or {}
            view.config["title"] = tb[col_name].metadata.title
            view.config["subtitle"] = tb[col_name].metadata.description_short
        for y in view.indicators.y:
            set_map_bins(y, view)

    c.save()


def set_map_bins(y, view) -> None:
    y.update_display({"colorScaleNumericMinValue": 0})
    if y.display.get("unit") == "%":
        if is_low_share_view(view):
            y.update_display({"colorScaleNumericBins": MAP_BINS["focus_on_lower_perc"]})
        else:
            y.update_display({"colorScaleNumericBins": MAP_BINS["regular_perc"]})


def is_low_share_view(view) -> bool:
    """Views of the lower service levels of water and sanitation, where shares are usually small."""
    dim = view.dimensions
    return dim.get("resource") in ["drinking_water", "sanitation"] and dim.get("level_of_use_access") in [
        "limited",
        "unimproved",
        "no_facilities",
    ]
