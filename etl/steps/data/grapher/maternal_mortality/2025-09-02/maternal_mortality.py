"""Load a garden dataset and create a grapher dataset."""

from structlog import get_logger

from etl.data_helpers.misc import export_table_to_gsheet, get_team_folder_id
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("maternal_mortality")

    # Read table from garden dataset.
    tb = ds_garden.read("maternal_mortality", reset_index=True)
    # Export the table to a Google Sheet
    # Can only be run locally, not in prod or staging
    tb_sel = tb[["country", "year", "mmr", "source"]].copy()
    team_folder_id = get_team_folder_id()
    sheet_url, sheet_id = export_table_to_gsheet(
        table=tb_sel,
        sheet_title="Long-run maternal mortality ratio",
        update_existing=True,
        folder_id=team_folder_id,
        metadata_variables=["mmr"],
        role="reader",
        general_access="anyone",
    )

    log.info(f"Google Sheet exported successfully. URL: {sheet_url}, ID: {sheet_id}")

    tb = tb.drop(columns=["source"])
    tb = tb.set_index(["country", "year"])
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
