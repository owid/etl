import datetime as dt
import difflib
import functools
import os
import urllib.error
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import bugsnag
import click
import numpy as np
import pandas as pd
import pywebio
import structlog
from cryptography.fernet import Fernet
from git.repo import Repo
from owid.catalog import Dataset
from owid.catalog.utils import underscore, validate_underscore
from owid.datautils import dataframes
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po
from pywebio import start_server
from pywebio.session import run_js
from rich import print
from rich.console import Console
from sqlmodel import Session

from etl import config
from etl import grapher_model as gm
from etl.command import main as etl_main
from etl.compare import diff_print
from etl.db import get_engine
from etl.files import apply_black_formatter_to_files
from etl.paths import (
    BASE_DIR,
    DAG_DIR,
    LATEST_REGIONS_DATASET_PATH,
    SNAPSHOTS_DIR,
    STEP_DIR,
)
from etl.snapshot import Snapshot, SnapshotMeta
from walkthrough import utils as walkthrough_utils

from . import csv, sheets
from .yaml_meta import YAMLDatasetMeta, YAMLMeta, YAMLSourceMeta, YAMLVariableMeta

config.enable_bugsnag()

log = structlog.get_logger()

DEFAULT_FASTTRACK_PORT = int(os.environ.get("FASTTRACK_PORT", 8082))
CURRENT_DIR = Path(__file__).parent
DAG_FASTTRACK_PATH = DAG_DIR / "fasttrack.yml"
DUMMY_DATA = {}

with open("fasttrack/styles.css", "r") as f:
    pywebio.config(css_style=f.read())


@click.command()
@click.option(
    "--commit",
    is_flag=True,
    help="Commit changes to git repository",
)
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
@click.option(
    "--port",
    default=DEFAULT_FASTTRACK_PORT,
    help="Port to run the server on",
)
def cli(commit: bool, dummy_data: bool, auto_open: bool, port: int) -> None:
    print(f"Fasttrack has been opened at http://localhost:{port}/")

    start_server(
        lambda: app(dummy_data=dummy_data, commit=commit),
        port=port,
        debug=True,
        auto_open_webbrowser=auto_open,
    )


class FasttrackImport:
    def __init__(
        self,
        data: pd.DataFrame,
        meta: YAMLMeta,
        sheets_url: str,
        is_private: bool,
        partial_snapshot_meta: sheets.PartialSnapshotMeta,
    ):
        self.data = data
        self.meta = meta
        self.sheets_url = sheets_url
        self.is_private = is_private
        self.partial_snapshot_meta = partial_snapshot_meta

    @property
    def dataset_dir(self) -> Path:
        return STEP_DIR / "data" / "grapher" / self.meta.dataset.namespace / str(self.meta.dataset.version)

    @property
    def step_path(self) -> Path:
        return self.dataset_dir / (self.meta.dataset.short_name + ".py")

    @property
    def metadata_path(self) -> Path:
        return self.dataset_dir / (self.meta.dataset.short_name + ".meta.yml")

    @property
    def snapshot(self) -> Snapshot:
        return Snapshot(f"{self.meta.dataset.namespace}/{self.meta.dataset.version}/{self.meta.dataset.short_name}.csv")

    def snapshot_exists(self) -> bool:
        try:
            self.snapshot
            return True
        except FileNotFoundError:
            return False

    def save_metadata(self) -> None:
        with open(self.metadata_path, "w") as f:
            f.write(self.meta.to_yaml())

    def upload_snapshot(self) -> Path:
        # since sheets url is accessible with link, we have to encrypt it when storing in metadata
        sheets_url = _encrypt(self.sheets_url) if self.is_private else self.sheets_url

        source_name = "Google Sheet" if self.sheets_url != "local_csv" else "Local CSV"

        snap_meta = SnapshotMeta(
            namespace=self.meta.dataset.namespace,
            short_name=self.meta.dataset.short_name,
            name=self.meta.dataset.title,
            version=str(self.meta.dataset.version),
            file_extension="csv",
            description=self.meta.dataset.description,
            url=self.partial_snapshot_meta.url,
            source_name=source_name,
            source_published_by=source_name,
            source_data_url=sheets_url,
            is_public=not self.is_private,
            date_accessed=dt.date.today(),
            publication_year=self.partial_snapshot_meta.publication_year,
            license_url=self.partial_snapshot_meta.license_url,
            license_name=self.partial_snapshot_meta.license_name,
        )
        snap_meta.save()

        snap = Snapshot(snap_meta.uri)
        dataframes.to_file(self.data, file_path=snap.path)
        snap.dvc_add(upload=True)

        return snap.metadata_path


def catch_exceptions(func):
    """Send exceptions to bugsnag and re-raise them."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            bugsnag.notify(e)
            raise e

    return wrapper


@catch_exceptions
def app(dummy_data: bool, commit: bool) -> None:
    dummies = DUMMY_DATA if dummy_data else {}

    with open(CURRENT_DIR / "intro.md", "r") as f:
        po.put_markdown(f.read())

    po.put_warning("This tool is still in beta. Please report any issues to @Mojmir")

    with open(CURRENT_DIR / "instructions_sheets.md", "r") as f:
        walkthrough_utils.put_widget(
            title=po.put_html("<b>Instructions for importing Google Sheet</b>"),
            contents=[po.put_markdown(f.read())],
        )

    with open(CURRENT_DIR / "instructions_csv.md", "r") as f:
        walkthrough_utils.put_widget(
            title=po.put_html("<b>Instructions for importing Local CSV</b>"),
            contents=[po.put_markdown(f.read())],
        )

    with open(CURRENT_DIR / "instructions_large_csv.md", "r") as f:
        walkthrough_utils.put_widget(
            title=po.put_html("<b>Instructions for importing large CSV file</b>"),
            contents=[po.put_markdown(f.read())],
        )

    data, meta, sheets_url, form, snapshot_dict = _load_data_and_meta(dummies)

    fast_import = FasttrackImport(data, meta, sheets_url, form.is_private, snapshot_dict)

    # diff with existing dataset
    if fast_import.snapshot_exists() and fast_import.metadata_path.exists():
        po.put_markdown("""## Data differences from existing dataset...""")
        _data_diff(fast_import, data)

        po.put_markdown("""## Metadata differences from existing dataset...""")
        _metadata_diff(fast_import, meta)

        # if data_is_different or metadata_is_different:
        do_continue = _ask_to_continue()
        if not do_continue:
            return

    # add dataset to dag
    dag_content = _add_to_dag(meta.dataset, form.is_private)

    # create step and metadata file
    walkthrough_utils.generate_step(
        CURRENT_DIR / "grapher_cookiecutter/", dict(**meta.dataset.dict(), channel="grapher")
    )
    fast_import.save_metadata()

    po.put_markdown(
        """
    ## Uploading Snapshot...

    This may take up to a minute, please be patient. Performance fix is in the works.

    """
    )
    snapshot_path = fast_import.upload_snapshot()
    po.put_success("Upload successful!")

    po.put_markdown("""## Running ETL and upserting to GrapherDB...""")
    step = f"grapher/{meta.dataset.path}"
    log.info("fasttrack.etl", step=step)
    etl_main(
        dag_path=DAG_FASTTRACK_PATH,
        steps=[step],
        grapher=True,
        private=form.is_private,
        workers=1,
    )
    po.put_success("Import to MySQL successful!")

    if commit:
        po.put_markdown("""## Commiting and pushing to Github...""")
        github_link = _commit_and_push(fast_import, snapshot_path)
        po.put_success("Changes commited and pushed successfully!")
    else:
        github_link = ""

    # TODO: add link to commit in ETL
    po.put_markdown("""## Links""")
    po.put_markdown(
        f"""
    * [Dataset in admin]({os.environ.get("ADMIN_HOST", "")}/admin/datasets/{_dataset_id(meta.dataset)})
    * [Commit in ETL]({github_link})
    """
    )

    po.put_markdown(
        """
## Generated files
        """
    )
    walkthrough_utils.preview_file(fast_import.metadata_path, language="yaml")
    walkthrough_utils.preview_file(fast_import.step_path, language="python")
    walkthrough_utils.preview_file(snapshot_path, language="yaml")
    walkthrough_utils.preview_dag(dag_content, dag_name="dag/fasttrack.yml")


class Options(Enum):
    INFER_METADATA = "Infer missing metadata (instead of raising an error)"
    IS_PRIVATE = "Make dataset private (your metadata will be still public!)"


class FasttrackForm(BaseModel):
    new_sheets_url: str
    existing_sheets_url: Optional[str]
    infer_metadata: bool
    is_private: bool
    local_csv: Any

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["is_private"] = Options.IS_PRIVATE.value in options
        data["infer_metadata"] = Options.INFER_METADATA.value in options
        super().__init__(**data)


def _load_data_and_meta(
    dummies: dict[str, str]
) -> Tuple[pd.DataFrame, YAMLMeta, str, FasttrackForm, sheets.PartialSnapshotMeta]:
    existing_sheets = [
        {"label": "Choose previously uploaded dataset", "value": "unselected"}
    ] + _load_existing_sheets_from_snapshots()

    sheets_url = None
    selected_sheet = "unselected"

    # endless loop that breaks if everything passed validation
    while True:
        sheets_url = sheets_url or dummies.get("sheet_url", "")

        def _onchange_existing_sheets_url(c: str) -> None:
            """If user selects existing sheet, update its public/private to be consistent with the sheet."""
            if c == "unselected":
                # new sheet, use private by default
                pi.input_update("options", value=[Options.INFER_METADATA.value, Options.IS_PRIVATE.value])
            else:
                # existing sheet, loads its public/private
                is_public = [e["is_public"] for e in existing_sheets if e["value"] == c][0]
                if is_public:
                    value = [Options.INFER_METADATA.value]
                else:
                    value = [Options.INFER_METADATA.value, Options.IS_PRIVATE.value]
                pi.input_update("options", value=value)

        form_dict = dict(
            pi.input_group(
                "Import from Google Sheets",
                [
                    pi.input(
                        "New Google Sheets URL",
                        value=sheets_url if selected_sheet == "unselected" else "",
                        name="new_sheets_url",
                        help_text="Click on `File -> Share -> Publish to Web` and share the entire document as csv. Copy the link above.",
                    ),
                    pi.select(
                        "OR Existing Google Sheets",
                        existing_sheets,
                        value=selected_sheet,
                        name="existing_sheets_url",
                        help_text="Selected sheet will be used if you don't specify Google Sheets URL",
                        onchange=_onchange_existing_sheets_url,
                    ),
                    pi.file_upload("OR Use local CSV file", name="local_csv", accept="text/csv"),
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

        log.info("fasttrack.form", form=form_dict)

        # use selected sheet if URL is not available
        if not form.local_csv:
            if not form.new_sheets_url:
                if form.existing_sheets_url == "unselected":
                    _bail([sheets.ValidationError("Please either set URL or pick from existing Google Sheets")])
                    continue
            else:
                if form.existing_sheets_url != "unselected":
                    _bail([sheets.ValidationError("You cannot set both URL and pick from existing Google Sheets")])
                    continue

        if form.local_csv:
            csv_df = pd.read_csv(StringIO(form.local_csv["content"].decode()))

            data = csv.parse_data_from_csv(csv_df)
            meta, partial_snapshot_meta = csv.parse_metadata_from_csv(form.local_csv["filename"], csv_df.columns)

            sheets_url = "local_csv"

            po.put_success("Data imported from CSV")
        else:
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
                if "?output=csv" not in sheets_url:
                    raise sheets.ValidationError(
                        "URL does not contain `?output=csv`. Have you published it as CSV and not as HTML by accident?"
                    )

                google_sheets = sheets.import_google_sheets(sheets_url)
                # TODO: it would make sense to repeat the import until we're sure that it has been updated
                # we wouldn't risk importing data that is not up to date then
                # the question is how much can we trust the timestamp in the published version
                po.put_success(
                    f"Data imported (sheet refreshed {_last_updated_before_minutes(google_sheets['dataset_meta'])} minutes ago)"
                )
                meta, partial_snapshot_meta = sheets.parse_metadata_from_sheets(
                    google_sheets["dataset_meta"], google_sheets["variables_meta"], google_sheets["sources_meta"]
                )
                data = sheets.parse_data_from_sheets(google_sheets["data"])
            except urllib.error.HTTPError:
                _bail(
                    [
                        sheets.ValidationError(
                            "Sheet not found, have you copied the template? Creating new Google Sheets document or new "
                            "sheets with the same name in the existing document does not work."
                        )
                    ]
                )
                continue
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

        # NOTE: harmonization is not done in ETL, but here in fast-track for technical reasons
        # It's not yet clear what will authors prefer and how should we handle preprocessing from
        # raw data to data saved as snapshot
        data, unknown_countries = _harmonize_countries(data)
        if unknown_countries:
            po.put_error("Unknown countries:\n\t" + "\n\t".join(unknown_countries))

            if dummies:
                unknown_countries_select = "drop unknown countries"
            else:
                unknown_countries_select = pi.select(
                    "Unknown countries found, what do you want to do?",
                    ["keep unknown countries", "drop unknown countries", "restart import"],
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

    return data, meta, sheets_url, form, partial_snapshot_meta


def _dataset_id(meta_ds: YAMLDatasetMeta) -> int:
    with Session(get_engine()) as session:
        ds = gm.Dataset.load_with_path(
            session, namespace=meta_ds.namespace, short_name=meta_ds.short_name, version=str(meta_ds.version)
        )
        assert ds.id
        return ds.id


def _last_updated_before_minutes(dataset_meta: pd.DataFrame) -> int:
    td = pd.Timestamp.utcnow() - pd.to_datetime(dataset_meta.set_index(0)[1].loc["updated"], dayfirst=True, utc=True)
    return int(td.total_seconds() / 60)


def _load_existing_sheets_from_snapshots() -> List[Dict[str, str]]:
    # get all fasttrack snapshots
    metas = [SnapshotMeta.load_from_yaml(path) for path in (SNAPSHOTS_DIR / "fasttrack").rglob("*.dvc")]

    # sort them by date accessed
    metas.sort(key=lambda meta: meta.date_accessed, reverse=True)

    # exclude local CSVs
    metas = [m for m in metas if m.source_name != "Local CSV"]

    # decrypt URLs if private
    for meta in metas:
        if not meta.is_public:
            assert meta.source_data_url
            meta.source_data_url = _decrypt(meta.source_data_url)

    # extract their name and url
    return [{"label": f"{meta.name} / {meta.version}", "value": meta.source_data_url, "is_public": meta.is_public} for meta in metas]  # type: ignore


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

    ds_regions = Dataset(LATEST_REGIONS_DATASET_PATH)
    alias_to_country = ds_regions["definitions"].join(ds_regions["aliases"], how="left").set_index("alias")["name"]

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


def _data_diff(fast_import: FasttrackImport, data: pd.DataFrame) -> bool:
    console = Console(record=True)

    # load data from snapshot
    if not fast_import.snapshot.path.exists():
        fast_import.snapshot.pull()
    existing_data = pd.read_csv(fast_import.snapshot.path)

    exit_code = diff_print(
        df1=existing_data,
        df2=data.reset_index(),
        df1_label="existing",
        df2_label="imported",
        absolute_tolerance=0.00000001,
        relative_tolerance=0.05,
        show_values=True,
        show_shared=True,
        truncate_lists_at=20,
        print=console.print,
    )

    html = console.export_html(inline_styles=True, code_format="<pre>{code}</pre>")

    po.put_html(html)

    return exit_code != 0


def _metadata_diff(fast_import: FasttrackImport, meta: YAMLMeta) -> bool:
    # load existing metadata file
    with open(fast_import.metadata_path, "r") as f:
        existing_meta = f.read()

    diff = difflib.HtmlDiff()
    html_diff = diff.make_table(existing_meta.split("\n"), meta.to_yaml().split("\n"), context=True)
    if "No Differences Found" in html_diff:
        po.put_success("No metadata differences found.")
        return False
    else:
        po.put_html(html_diff)
        return True


def _commit_and_push(fast_import: FasttrackImport, snapshot_path: Path) -> str:
    """Format generated files, commit them and push to GitHub."""
    apply_black_formatter_to_files([fast_import.step_path])

    repo = Repo(BASE_DIR)
    repo.index.add(
        [
            str(snapshot_path),
            str(fast_import.metadata_path),
            str(fast_import.step_path),
            str(DAG_FASTTRACK_PATH),
        ]
    )
    commit = repo.index.commit(f"fasttrack: {fast_import.snapshot.uri}")
    origin = repo.remote(name="origin")
    origin.push()

    github_link = f"https://github.com/owid/etl/commit/{commit.hexsha}"
    return github_link


def _ask_to_continue() -> bool:
    answer = pi.actions(
        buttons=[
            {
                "label": "Continue",
                "value": "Continue",
                "color": "primary",
            },
            {
                "label": "Cancel",
                "value": "Cancel",
                "color": "danger",
            },
        ],
        help_text="Do you want to continue and add the dataset to GrapherDB?",
    )

    # start saving the dataset after we click continue
    if answer == "Cancel":
        run_js("window.location.reload()")
        return False
    else:
        return True


def _bail(errors: Sequence[Exception]) -> None:
    for e in errors:
        po.put_error(str(e))
    po.put_info("Please fix these errors and try again.")


def _get_secret_key() -> Optional[Fernet]:
    secret_key = os.environ.get("FASTTRACK_SECRET_KEY")
    if not secret_key:
        log.warning("FASTTRACK_SECRET_KEY not found in environment variables. Not using encryption.")
        return None
    return Fernet(secret_key)


def _encrypt(s: str) -> str:
    fernet = _get_secret_key()
    return fernet.encrypt(s.encode()).decode() if fernet else s


def _decrypt(s: str) -> str:
    fernet = _get_secret_key()
    # content is not encrypted, this is to keep it backward compatible with old datasets
    # that weren't using encryption
    if "docs.google.com" in s:
        return s
    else:
        return fernet.decrypt(s.encode()).decode() if fernet else s


if __name__ == "__main__":
    cli()
