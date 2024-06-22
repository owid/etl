"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("education_opri")

    # Read table from garden dataset.
    tb = ds_garden["education_opri"].reset_index()

    #
    # Process data.
    #
    # Pivot the table to have the indicators as columns to add descriptions from producer

    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")
    for column in tb_pivoted.columns:
        long_definition = tb["long_description"].loc[tb["indicator_label_en"] == column].iloc[0]
        tb_pivoted[column].metadata.description_from_producer = long_definition
        tb_pivoted[column].metadata.title = column

    tb_pivoted = tb_pivoted.reset_index()
    tb_pivoted = tb_pivoted.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_pivoted], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
