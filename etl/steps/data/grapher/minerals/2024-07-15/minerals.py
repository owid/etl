"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("minerals")
    tb = ds_garden.read_table("minerals")

    #
    # Process data.
    #
    # Pivot table to have production for each commodity.
    tb = tb.pivot(
        index=["country", "year"],
        columns="commodity",
        values=["imports", "exports", "production"],
        join_column_levels_with="_",
    )

    # Improve metadata of new columns.
    for column in tb.drop(columns=["country", "year"]).columns:
        metric = column.split("_")[0].capitalize()
        commodity = " ".join(column.split("_")[1:]).capitalize()
        title = f"{metric} of {commodity}"
        tb[column].metadata.title = title
        tb[column].metadata.unit = "tonnes"
        tb[column].metadata.short_unit = "t"

    # Format table conveniently.
    tb = tb.format(keys=["country", "year"])

    # Fix format.
    tb = tb.astype("Float64")

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
