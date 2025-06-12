"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("income_groups")

    # Read table of income groups (a dynamic classification that changes over the years).
    tb_dynamic = ds_garden.read("income_groups")

    # Read table of latest income groups (a static classification).
    tb_static = ds_garden.read("income_groups_latest")

    #
    # Process data.
    #
    # Combine both tables.
    tb = tb_dynamic.merge(tb_static, on=["country"], how="outer", suffixes=("", "_latest"))

    # Sanity check.
    error = "Classification in the latest year of data should coincide with 'classification_latest'."
    assert (
        tb[tb["year"] == tb["year"].max()]["classification"]
        == tb[tb["year"] == tb["year"].max()]["classification_latest"]
    ).all(), error

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
