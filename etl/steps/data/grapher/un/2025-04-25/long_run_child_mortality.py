"""Load a garden dataset and create a grapher dataset."""

from structlog import get_logger

from etl.data_helpers.misc import export_table_to_gsheet, get_team_folder_id
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("long_run_child_mortality")

    # Read table from garden dataset.
    tb = ds_garden["long_run_child_mortality"]
    tb_sel = ds_garden["long_run_child_mortality_selected"]
    tb_sel['child_mortality_rate'] = tb_sel['child_mortality_rate'].round(2)
    # Export the table to a Google Sheet
    # Can only be run locally, not in prod or staging
    team_folder_id = get_team_folder_id()
    sheet_url, sheet_id = export_table_to_gsheet(
        table=tb_sel,
        sheet_title="Long-run child mortality rate",
        update_existing=True,
        folder_id=team_folder_id,
        metadata_variables=["child_mortality_rate"],
        role="reader",
        general_access="anyone",
    )

    log.info(f"Google Sheet exported successfully. URL: {sheet_url}, ID: {sheet_id}")
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
