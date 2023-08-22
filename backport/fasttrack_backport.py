import datetime as dt
import os
from typing import Optional, cast

import click
import gspread
import pandas as pd
import structlog
from google.oauth2.service_account import Credentials
from owid.catalog.utils import underscore

from backport.backport import PotentialBackport
from etl.backport_helpers import create_dataset
from etl.db import get_engine
from etl.metadata_export import metadata_export
from fasttrack.sheets import SHEET_TO_GID

log = structlog.get_logger()


@click.command()
@click.option("--dataset-id", type=int, required=True)
@click.option(
    "--short-name", type=str, required=False, help="New short name to use, underscored dataset name by default"
)
@click.option(
    "--backport/--no-backport",
    default=True,
    type=bool,
    help="Backport dataset before migrating",
)
@click.option(
    "--recreate/--no-recreate",
    default=False,
    type=bool,
    help="Recreate the spreadsheet if it already exists",
)
def cli(
    dataset_id: int,
    short_name: Optional[str] = None,
    backport: bool = True,
    recreate: bool = False,
) -> None:
    """Create Fast-track ready spreadsheet from an existing dataset.

    ## Installation

    1. Add Google Sheets API and Google Drive API to your project in the Google Cloud Platform Console.
    2. Download the credentials as a JSON file and save it in the same directory as this notebook.
    3. Point env variable GOOGLE_APPLICATION_CREDENTIALS to the credentials file.
    4. Share [Fast-track template](https://docs.google.com/spreadsheets/d/1j_mclAffQ2_jpbVEmI3VOiWRBeclBAIr-U7NpGAdV9A/edit#gid=1898134479) with the service account email address (e.g. 937270026338-compute@developer.gserviceaccount.com)

    Example usage:
        ENV=.env.prod backport-fasttrack --dataset-id 5546 --short-name democracy_lexical_index --no-backport
    """
    return migrate(dataset_id=dataset_id, short_name=short_name, backport=backport, recreate=recreate)


def _create_client():
    # Define the scope
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]

    # Add your service account file
    creds = Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scope)

    return gspread.authorize(creds)


def _create_temp_dataset(pb: PotentialBackport, short_name):
    if not short_name:
        short_name = underscore(pb.ds.name)
    short_name = cast(str, short_name)

    ds = create_dataset("/tmp/migrate", short_name=pb.short_name, new_short_name=short_name)
    ds.metadata.version = "latest"
    ds.metadata.short_name = short_name
    return ds


def _fill_data(spreadsheet, ds):
    assert len(ds.table_names) == 1
    df = ds[ds.table_names[0]]

    df = df.reset_index().drop(columns=["entity_id", "entity_code"]).rename(columns={"entity_name": "country"})

    # fill missing values with empty string
    for col in df.select_dtypes(["category"]).columns:
        df[col] = df[col].cat.add_categories([""])
    for col in df.columns:
        if str(df[col].dtype).startswith("UInt") and df[col].isna().any():
            df[col] = df[col].astype(float)
    df = df.fillna("").astype(str)

    # Convert the DataFrame to a list of lists and include the header
    values = [df.columns.values.tolist()] + df.values.tolist()

    wks = spreadsheet.get_worksheet_by_id(SHEET_TO_GID["data"])
    wks.update("A1", values)


def _fill_variables_meta(spreadsheet, meta, short_name):
    # extract source
    sources = meta["dataset"]["sources"]
    assert len(sources) == 1
    source = sources[0]
    source["short_name"] = underscore(source["name"])

    # add source to the spreadsheet
    wks = spreadsheet.get_worksheet_by_id(SHEET_TO_GID["sources_meta"])
    keys = wks.col_values(1)
    values = pd.Series(source).reindex(keys).fillna("").astype(str).to_frame().values.tolist()
    wks.update("B1", values)

    # extract dataset
    dataset = meta["dataset"]
    dataset["sources"] = source["short_name"]
    dataset["version"] = "latest"
    dataset["short_name"] = short_name
    dataset["description"] = source["description"]
    dataset["updated"] = str(dt.datetime.utcnow())

    # add dataset to the spreadsheet
    wks = spreadsheet.get_worksheet_by_id(SHEET_TO_GID["dataset_meta"])
    keys = wks.col_values(1)
    values = pd.Series(dataset).reindex(keys).fillna("").astype(str).to_frame().values.tolist()
    wks.update("B1", values)

    # extract variables
    table_names = list(meta["tables"].keys())
    assert len(table_names) == 1
    table_name = table_names[0]
    variables = meta["tables"][table_name]["variables"]

    # create dataframe from variables
    for short_name, vals in variables.items():
        vals["short_name"] = short_name
        # vals["sources"] = source["short_name"]
        for k, v in vals.pop("display", {}).items():
            # skip includeInTable=True which is the default
            if k == "includeInTable" and v:
                continue
            vals[f"display.{k}"] = v

    vars_df = pd.DataFrame(variables.values())

    wks = spreadsheet.get_worksheet_by_id(SHEET_TO_GID["variables_meta"])

    vars_df = vars_df.reindex(columns=wks.row_values(1)).fillna("")

    # update values
    values = [vars_df.columns.values.tolist()] + vars_df.values.tolist()
    wks.update("A1", values)


def _copy_template(client, spreadsheet_title):
    spreadsheet = client.open("Fast-track template")

    copied_spreadsheet = client.copy(spreadsheet.id, title=spreadsheet_title)

    # delete raw_data sheet
    copied_spreadsheet.del_worksheet(spreadsheet.get_worksheet_by_id(SHEET_TO_GID["raw_data"]))

    # Share the copied spreadsheet with your team
    copied_spreadsheet.share("team@ourworldindata.org", perm_type="user", role="writer")


def migrate(
    dataset_id: int,
    short_name: Optional[str] = None,
    backport: bool = True,
    recreate: bool = False,
) -> None:
    lg = log.bind(dataset_id=dataset_id)

    lg.info("migrate.start")

    client = _create_client()

    engine = get_engine()

    # load metadata from MySQL
    pb = PotentialBackport(dataset_id)
    pb.load(engine)

    # copy template and use new title
    spreadsheet_title = f"Fast-track: {pb.ds.name}"

    if recreate:
        try:
            spreadsheet = client.open(spreadsheet_title)
            client.del_spreadsheet(spreadsheet.id)
        except gspread.exceptions.SpreadsheetNotFound:
            pass

    try:
        spreadsheet = client.open(spreadsheet_title)
    except gspread.exceptions.SpreadsheetNotFound:
        _copy_template(client, spreadsheet_title)
        spreadsheet = client.open(spreadsheet_title)

    log.info("migrate.spreadsheet", url=spreadsheet.url)

    if backport:
        lg.info("migrate.backport_dataset")
        # backport to refresh snapshots in S3
        if pb.needs_update():
            pb.upload(upload=True, dry_run=False, engine=engine)
        # run ETL on backport
        else:
            from etl.command import main

            main(
                [pb.short_name],
                backport=True,
            )

    spreadsheet = client.open(spreadsheet_title)

    # create temp dataset from backported one
    ds = _create_temp_dataset(pb, short_name)

    meta = metadata_export(ds)

    log.info("migrate.metadata")
    _fill_variables_meta(spreadsheet, meta, short_name)

    log.info("migrate.data")
    _fill_data(spreadsheet, ds)

    lg.info(f"1. Open spreadsheet at {spreadsheet.url}")
    lg.info("2. Add spreadsheet to Google Drive with: File -> Add a shortcut to Drive")
    lg.info("3. Import spreadsheet with Fast-track on http://etl-prod-1:8082/")
    lg.info("4. Run walkthrough charts to migrate charts to the new dataset")
    lg.info("4. Delete old dataset")

    lg.info("migrate.finish")


if __name__ == "__main__":
    cli()
