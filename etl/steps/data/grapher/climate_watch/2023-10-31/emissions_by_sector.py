"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Convert million tonnes to tonnes.
MT_TO_T = 1e6


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("emissions_by_sector")

    #
    # Process data.
    #
    # Process each table in the dataset.
    tables = []
    for table_name in ds_garden.table_names:
        tb = ds_garden[table_name].copy()

        # Drop unnecessary columns.
        tb = tb.drop(columns=["population"])

        # For convenience, change units from "million tonnes" to "tonnes" and multiply all variables by a million.
        # Doing this, grapher will know when to use the word "million" and when to use "billion".
        for column in tb.columns:
            if tb[column].metadata.unit == "million tonnes":
                tb[column].metadata.unit = "tonnes"
                tb[column].metadata.short_unit = "t"
                tb[column] *= MT_TO_T
                tb[column].metadata.description_short = tb[column].metadata.description_short.replace(
                    "million tonnes", "tonnes"
                )

        # Add current table to the list.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, default_metadata=ds_garden.metadata, tables=tables, check_variables_metadata=True
    )
    ds_grapher.save()
