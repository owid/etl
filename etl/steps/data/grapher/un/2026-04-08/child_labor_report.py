"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("child_labor_report")

    # Read tables from garden dataset.
    tb = ds_garden.read("child_labor", reset_index=False)
    tb_sector = ds_garden.read("sector", reset_index=False)
    tb_by_sex = ds_garden.read("child_labor_by_sex")

    #
    # Process data.
    #
    # Use sex as the chart entity (replacing the World-only country column), so charts
    # can show one row per sex (e.g. a dumbbell comparing child labor with and without
    # household chores).
    assert (tb_by_sex["country"] == "World").all(), "Expected only World rows in child_labor_by_sex."
    tb_by_sex = tb_by_sex.drop(columns=["country"]).rename(columns={"sex": "country"})
    tb_by_sex["country"] = tb_by_sex["country"].replace({"boys": "Boys", "girls": "Girls"})
    tb_by_sex = tb_by_sex.format(["country", "year", "age"], short_name="child_labor_by_sex")

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_sector, tb_by_sex], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
