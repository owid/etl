import datetime as dt
import json
import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import click
import numpy as np
import pandas as pd
import structlog
from owid.catalog import Dataset
from owid.catalog.utils import underscore, validate_underscore
from owid.datautils import dataframes
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po
from pywebio import start_server
from rich import print
from sqlmodel import Session

from etl import config
from etl import grapher_model as gm
from etl.command import main as etl_main
from etl.db import get_engine
from etl.paths import BASE_DIR, REFERENCE_DATASET, SNAPSHOTS_DIR
from etl.snapshot import Snapshot, SnapshotMeta
from walkthrough import utils as walkthrough_utils

from . import sheets
from .yaml_meta import YAMLDatasetMeta, YAMLMeta, YAMLSourceMeta, YAMLVariableMeta

config.enable_bugsnag()

log = structlog.get_logger()

CURRENT_DIR = Path(__file__).parent
DAG_FASTTRACK_PATH = BASE_DIR / "dag_files/dag_fasttrack.yml"
DUMMY_DATA = {
    "sheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vS3kx1WYuCWfWVQYuu3H0J0E2epv3jX9-odkzvhuHj-XenGGFvTCKIOngNpbkxkmw8iv4ZS5eNF4hS0/pub?output=csv",
}


@click.command()
@click.option(
    "--dummy-data",
    is_flag=True,
    help="Prefill form with dummy data, useful for development",
)
@click.option(
    "--auto-open/--skip-auto-open",
    is_flag=True,
    default=True,
    help="Open browser automatically",
)
def cli(dummy_data: bool, auto_open: bool) -> None:
    print("Fasttrack has been opened at http://localhost:8082/")

    start_server(
        lambda: app(dummy_data=dummy_data),
        port=8082,
        debug=True,
        auto_open_webbrowser=auto_open,
    )


def app(dummy_data: bool) -> None:
    dummies = DUMMY_DATA if dummy_data else {}

    with open(CURRENT_DIR / "intro.md", "r") as f:
        po.put_markdown(f.read())

    with open(CURRENT_DIR / "instructions.md", "r") as f:
        walkthrough_utils.put_widget(
            title=po.put_html("<b>Instructions</b>"),
            contents=[po.put_markdown(f.read())],
        )

    data, meta, sheets_url, form = _load_data_and_meta(dummies)

    meta_ds = meta.dataset

    dag_content = _add_to_dag(meta_ds, form.is_private)

    step_path, metadata_path = _generate_etl_files(meta)

    po.put_markdown("""## Uploading Snapshot...""")
    snapshot_path = _upload_snapshot(data, meta, sheets_url, form.is_private)
    po.put_success("Upload successful!")

    po.put_markdown("""## Running ETL and importing to MySQL...""")
    try:
        step = f"grapher/{meta_ds.path}"
        log.info("fasttrack.etl", step=step)
        etl_main(
            steps=[step],
            grapher=True,
            private=form.is_private,
            force=True,
            workers=1,
        )
    except Exception as e:
        _bail([e])
        return

    po.put_success(
        po.put_markdown(
            f"""
    Import to MySQL successful!

    [Link]({os.environ.get("ADMIN_HOST", "")}/admin/datasets/{_dataset_id(meta_ds)}) to dataset in admin
    ~[Link](TODO) to commit in ETL~
    """
        )
    )

    po.put_markdown(
        """
## Generated files
        """
    )
    walkthrough_utils.preview_file(metadata_path, language="yaml")
    walkthrough_utils.preview_file(step_path, language="python")
    walkthrough_utils.preview_file(snapshot_path, language="yaml")
    walkthrough_utils.preview_dag(dag_content, dag_name="dag_fasttrack.yml")


class Options(Enum):

    INFER_METADATA = "Infer missing metadata (instead of raising an error)"
    IS_PRIVATE = "Make dataset private"


class FasttrackForm(BaseModel):

    new_sheets_url: str
    existing_sheets_url: Optional[str]
    infer_metadata: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["is_private"] = Options.IS_PRIVATE.value in options
        data["infer_metadata"] = Options.INFER_METADATA.value in options
        super().__init__(**data)


def _load_data_and_meta(dummies: dict[str, str]) -> Tuple[pd.DataFrame, YAMLMeta, str, FasttrackForm]:
    existing_sheets = [
        {"label": "Choose previously uploaded dataset", "value": "unselected"}
    ] + _load_existing_sheets_from_snapshots()

    sheets_url = None
    selected_sheet = None

    # endless loop that breaks if everything passed validatin
    while True:
        sheets_url = sheets_url or dummies.get("sheet_url", "")

        form_dict = dict(
            pi.input_group(
                "Import from Google Sheets",
                [
                    pi.input(
                        "New Google Sheets URL",
                        value=sheets_url if selected_sheet is None else "",
                        name="new_sheets_url",
                        help_text="Click on `File -> Share -> Publish to Web` and share the entire document as csv. Copy the link above.",
                    ),
                    pi.select(
                        "OR Existing Google Sheets",
                        existing_sheets,
                        value=selected_sheet,
                        name="existing_sheets_url",
                        help_text="Selected sheet will be used if you don't specify Google Sheets URL",
                    ),
                    pi.checkbox(
                        "Additional Options",
                        options=[Options.INFER_METADATA.value, Options.IS_PRIVATE.value],  # type: ignore
                        name="options",
                        value=[Options.INFER_METADATA.value, Options.IS_PRIVATE.value],
                    ),
                ],
            )
        )

        form = FasttrackForm(**form_dict)

        # use selected sheet if URL is not available
        if not form.new_sheets_url:
            if form.existing_sheets_url == "unselected":
                _bail([sheets.ValidationError("Please either set URL or pick from existing Google Sheets")])
                continue
        else:
            if form.existing_sheets_url != "unselected":
                _bail([sheets.ValidationError("You cannot set both URL and pick from existing Google Sheets")])
                continue

        selected_sheet = form.existing_sheets_url
        sheets_url = form.new_sheets_url or selected_sheet

        assert sheets_url

        po.put_markdown(
            """
        ## Importing data from Google Sheets...

        Note that Google Sheets refreshes its published version every 5 minutes, so you may need to wait a bit after you update your data.
        """
        )

        try:
            google_sheets = sheets.import_google_sheets(sheets_url)
            # TODO: it would make sense to repeat the import until we're sure that it has been updated
            # we wouldn't risk importing data that is not up to date then
            # the question is how much can we trust the timestamp in the published version
            po.put_success(
                f"Data imported (sheet refreshed {_last_updated_before_minutes(google_sheets['dataset_meta'])} minutes ago)"
            )
            meta = sheets.parse_metadata_from_sheets(
                google_sheets["dataset_meta"], google_sheets["variables_meta"], google_sheets["sources_meta"]
            )
            data = sheets.parse_data_from_sheets(google_sheets["data"])
        except sheets.ValidationError as e:
            _bail([e])
            continue

        # try to infer as much missing metadata as possible
        if form.infer_metadata:
            data, meta.tables[meta.dataset.short_name].variables = _infer_metadata(
                data, meta.tables[meta.dataset.short_name].variables
            )
            # add unknown source if we don't have any
            if not meta.dataset.sources:
                meta.dataset.sources = [
                    YAMLSourceMeta(
                        name="Unknown",
                        published_by="Unknown",
                        publication_year=dt.date.today().year,
                        date_accessed=dt.date.today(),
                    )
                ]

        # validation
        success = _validate_data(data, meta.tables[meta.dataset.short_name].variables)

        if not success:
            continue

        data, unknown_countries = _harmonize_countries(data)
        if unknown_countries:
            po.put_error("Unknown countries:\n\t" + "\n\t".join(unknown_countries))

            if dummies:
                unknown_countries_select = "drop unknown countries"
            else:
                unknown_countries_select = pi.select(
                    "Unknown countries found, what do you want to do?",
                    ["restart import", "drop unknown countries", "keep unknown countries"],
                    help_text="You can fix those countries in the spreadsheet and restart import",
                )

            if unknown_countries_select == "restart import":
                continue

            elif unknown_countries_select == "drop unknown countries":
                data = data.loc[~data.index.get_level_values("country").isin(unknown_countries)]

            elif unknown_countries_select == "keep unknown countries":
                pass

        po.put_success("Countries have been harmonized")

        break

    return data, meta, sheets_url, form


def _dataset_id(meta_ds: YAMLDatasetMeta) -> int:
    with Session(get_engine()) as session:
        ds = gm.Dataset.load_with_path(
            session, namespace=meta_ds.namespace, short_name=meta_ds.short_name, version=str(meta_ds.version)
        )
        assert ds.id
        return ds.id


def _generate_etl_files(meta: YAMLMeta) -> Tuple[Path, Path]:
    DATASET_DIR = walkthrough_utils.generate_step(
        CURRENT_DIR / "grapher_cookiecutter/", dict(**meta.dataset.dict(), channel="grapher")
    )
    step_path = DATASET_DIR / (meta.dataset.short_name + ".py")
    metadata_path = DATASET_DIR / (meta.dataset.short_name + ".meta.yml")
    with open(metadata_path, "w") as f:
        f.write(meta.to_yaml())

    return step_path, metadata_path


def _last_updated_before_minutes(dataset_meta: pd.DataFrame) -> int:
    td = pd.Timestamp.utcnow() - pd.to_datetime(dataset_meta.set_index(0)[1].loc["updated"], dayfirst=True, utc=True)
    return int(td.total_seconds() / 60)


def _load_existing_sheets_from_snapshots() -> List[Dict[str, str]]:
    # get all fasttrack snapshots
    metas = [SnapshotMeta.load_from_yaml(path) for path in (SNAPSHOTS_DIR / "fasttrack").rglob("*.dvc")]

    # sort them by date accessed
    metas.sort(key=lambda meta: meta.date_accessed, reverse=True)

    # extract their name and url
    return [{"label": meta.name, "value": meta.url} for meta in metas]


def _infer_metadata(
    data: pd.DataFrame, meta_variables: Dict[str, YAMLVariableMeta]
) -> Tuple[pd.DataFrame, Dict[str, YAMLVariableMeta]]:
    # underscore variable names from data sheet, this doesn't raise warnings
    for col in data.columns:
        data = data.rename(columns={col: underscore(col)})

    # underscore short names from metadata, raise warning if they don't match
    for short_name in list(meta_variables.keys()):
        try:
            validate_underscore(short_name, "Variables")
        except NameError:
            new_short_name = underscore(short_name)
            po.put_warning(
                po.put_markdown(
                    f"`{short_name}` isn't in [snake_case](https://en.wikipedia.org/wiki/Snake_case) format and was renamed to `{new_short_name}`. Please update it in your sheet `variables_meta`."
                )
            )
            meta_variables[new_short_name] = meta_variables.pop(short_name)

    # add missing variable metadata
    # for col in data.columns:
    #     # use underscored column name as variable short name and full name as title
    #     try:
    #         validate_underscore(col, "Variables")
    #         short_name = col
    #         title = col
    #     except NameError:
    #         short_name = underscore(col)
    #         title = col

    #     if short_name not in meta_variables:
    #         meta_variables[short_name] = YAMLVariableMeta(title=title, short_unit="", unit="", description="")

    #     data = data.rename(columns={col: short_name})

    return data, meta_variables


def _validate_data(df: pd.DataFrame, meta_variables: Dict[str, YAMLVariableMeta]) -> bool:
    po.put_markdown("""## Validating data and metadata...""")
    errors = []

    # check column names
    for col in df.columns:
        try:
            validate_underscore(col, "Variables")
        except NameError as e:
            errors.append(sheets.ValidationError(e))

    # missing columns in metadata
    for col in set(df.columns) - set(meta_variables.keys()):
        errors.append(sheets.ValidationError(f"Variable {col} is not defined in metadata"))

    # extra columns in metadata
    for col in set(meta_variables.keys()) - set(df.columns):
        errors.append(sheets.ValidationError(f"Variable {col} in metadata is not in the data"))

    # missing titles
    for col in df.columns:
        if col in meta_variables and not meta_variables[col].title:
            errors.append(sheets.ValidationError(f"Variable {col} is missing title (you can use its short name)"))

    # no inf values
    for col in df.select_dtypes("number").columns:
        if col in df.columns and np.isinf(df[col].abs().max()):
            errors.append(sheets.ValidationError(f"Variable {col} has inf values"))

    if errors:
        _bail(errors)
        return False
    else:
        po.put_success("Data and metadata is valid")
        return True


def _harmonize_countries(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Check if all countries are harmonized."""
    po.put_markdown("""## Harmonizing countries...""")

    alias_to_country = (
        Dataset(REFERENCE_DATASET)["countries_regions"][["name", "aliases"]]
        .assign(aliases=lambda df: df.aliases.map(lambda s: json.loads(s) if isinstance(s, str) else None))
        .explode("aliases")
        .set_index("aliases")["name"]
    )

    df = df.reset_index()

    unknown_countries = []

    for country in set(df.country):
        # country is in reference dataset
        if country in alias_to_country.values:
            continue

        # there is an alias for this country
        elif country in alias_to_country.index:
            df.country = df.country.replace({country: alias_to_country[country]})
            po.put_warning(f"Country `{country}` harmonized to `{alias_to_country.loc[country]}`")

        # unknown country
        else:
            unknown_countries.append(country)

    df.set_index(["country", "year"], inplace=True)

    return df, unknown_countries


def _add_to_dag(ds_meta: YAMLDatasetMeta, is_private: bool) -> str:
    public_data_step = f"data://grapher/{ds_meta.path}"
    private_data_step = f"data-private://grapher/{ds_meta.path}"

    # add steps to dag, replace public by private and vice versa if needed
    if is_private:
        to_remove = public_data_step
        to_add = {private_data_step: [f"snapshot-private://{ds_meta.path}.csv"]}
    else:
        to_remove = private_data_step
        to_add = {public_data_step: [f"snapshot://{ds_meta.path}.csv"]}

    walkthrough_utils.remove_from_dag(
        to_remove,
        DAG_FASTTRACK_PATH,
    )
    return walkthrough_utils.add_to_dag(
        to_add,
        DAG_FASTTRACK_PATH,
    )


def _upload_snapshot(data: pd.DataFrame, meta: YAMLMeta, sheets_url: str, is_private: bool) -> Path:
    snap_meta = SnapshotMeta(
        namespace=meta.dataset.namespace,
        short_name=meta.dataset.short_name,
        name=meta.dataset.title,
        version=str(meta.dataset.version),
        file_extension="csv",
        description=meta.dataset.description,
        source_name="Google Sheet",
        url=sheets_url,
        is_public=not is_private,
        date_accessed=dt.date.today(),
    )
    snap_meta.save()

    snap = Snapshot(snap_meta.uri)
    dataframes.to_file(data, file_path=snap.path)
    snap.dvc_add(upload=True)

    return snap.metadata_path


def _bail(errors: Sequence[Exception]) -> None:
    for e in errors:
        po.put_error(str(e))
    po.put_info("Please fix these errors and try again.")


if __name__ == "__main__":
    cli()
