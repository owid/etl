"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its table.
    ds_garden = paths.load_dataset("child_work_incidence_us")
    tb = ds_garden.read("child_work_incidence_us")

    #
    # Process data.
    #
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
