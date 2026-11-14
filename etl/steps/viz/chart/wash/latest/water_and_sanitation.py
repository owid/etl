"""Load the WASH grapher dataset and create the water and sanitation multidim (shown as a data explorer)."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load the grapher dataset's metadata (no data needed).
    ds = paths.load_dataset("household")
    tb = ds.read("household", load_data=False)

    # Load the chart config.
    config = paths.load_config()

    # Create the multidim.
    c = paths.create_chart(config=config)

    # Take each view's title and subtitle from its indicator's metadata.
    for view in c.views:
        assert view.indicators.y is not None
        if len(view.indicators.y) == 1:
            col_name = view.indicators.y[0].catalogPath.split("#")[1]
            view.config = view.config or {}
            view.config["title"] = tb[col_name].metadata.title
            view.config["subtitle"] = tb[col_name].metadata.description_short

    c.save()
