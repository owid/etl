"""Load a garden dataset and create a grapher dataset."""

from etl.data_helpers.misc import export_table_to_gsheet, get_team_folder_id
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("historic_literacy_omm")

    # Read table from garden dataset.
    tb = ds_garden.read("historic_literacy_omm", reset_index=False)

    tb_sel = tb[["source_url", "source"]].copy()

    team_folder_id = get_team_folder_id()
    sheet_url, sheet_id = export_table_to_gsheet(
        table=tb_sel,
        sheet_title="Long-run literacy data",
        update_existing=True,
        folder_id=team_folder_id,
        metadata_variables=["literacy_rate"],
        role="reader",
        general_access="anyone",
    )

    paths.log.info(f"Google Sheet exported successfully. URL: {sheet_url}, ID: {sheet_id}")

    # Dropping columns we only want in the GSheet and not in grapher
    tb = tb.drop(columns=["source_url", "source"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
