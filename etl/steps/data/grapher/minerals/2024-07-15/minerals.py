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
        columns=["commodity", "sub_commodity", "source"],
        values=["imports", "exports", "production", "reserves", "unit_value"],
        join_column_levels_with="|",
    )

    # Improve metadata of new columns.
    for column in tb.drop(columns=["country", "year"]).columns:
        metric, commodity, sub_commodity, source = column.split("|")
        metric = metric.replace("_", " ").capitalize()
        commodity = commodity.capitalize()
        sub_commodity = sub_commodity.lower()
        title_public = f"{metric} of {commodity} ({sub_commodity}), according to {source}"
        tb[column].metadata.title = column
        tb[column].metadata.presentation.title_public = title_public
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
