"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset
from etl.data_helpers.misc import export_table_to_gsheet, get_team_folder_id
from structlog import get_logger

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("life_expectancy")

    # Read table from garden dataset.
    tables = list(ds_garden)
    tb_gsheet = ds_garden["life_expectancy_at_birth"].reset_index()
    tb_gsheet = tb_gsheet[["country", "year", "life_expectancy_0", "source"]].rename(
        columns={"life_expectancy_0": "life_expectancy_at_birth"}
    )
    #
    # Process data.
    team_folder_id = get_team_folder_id()
    sheet_url, sheet_id = export_table_to_gsheet(
        table=tb_gsheet,  # type: ignore
        sheet_title="Long-run life expectancy at birth",
        folder_id=team_folder_id,
        metadata_variables=["life_expectancy_at_birth"],
        role="reader",
        general_access="anyone",
    )
    log.info(f"Google Sheet exported successfully. URL: {sheet_url}, ID: {sheet_id}")
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
