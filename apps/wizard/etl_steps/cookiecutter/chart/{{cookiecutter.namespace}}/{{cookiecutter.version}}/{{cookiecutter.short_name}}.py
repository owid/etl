"""Create a chart (or MDIM) from a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default chart config
    config = paths.load_config()

    # Load grapher dataset.
    ds = paths.load_dataset("{{cookiecutter.short_name}}")
    tb = ds.read("{{cookiecutter.short_name}}", load_data=False)

    #
    # (optional) Adjust dimensions if needed
    #

    #
    # Create chart object
    #
    c = paths.create_chart(
        config=config,
        short_name="{{cookiecutter.short_name}}",
        tb=tb,
        # indicator_names=[],
        # dimensions={},
    )


    #
    # (optional) Edit views
    #
    for view in c.views:
        # if view.dimensions["sex"] == "male":
        #     view.config["title"] = "Something else"
        pass

    #
    # Save the chart.
    #
    c.save()
