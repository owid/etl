"""Load a garden dataset and create a grapher dataset."""

from etl.data_helpers.misc import export_table_to_gsheet
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("long_run_child_mortality")

    # Read table from garden dataset.
    tb = ds_garden["long_run_child_mortality"]
    tb_sel = ds_garden["long_run_child_mortality_selected"]
    # Export the table to a Google Sheet.
    sheet_url, sheet_id = export_table_to_gsheet(
        table=tb_sel,
        sheet_title="Long Run Child Mortality Estimates",
        update_existing=True,
        metadata_variables=["child_mortality_rate"],
    )
    print(f"Google Sheet exported successfully. URL: {sheet_url}, ID: {sheet_id}")
    # Process data.
    # Dropping columns we only want in the GSheet and not in grapher
    tb = tb.drop(columns=["source_url"])
    tb_sel = tb_sel.drop(columns=["source_url"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb, tb_sel], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
