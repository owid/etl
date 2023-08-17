import datetime as dt
import difflib
import functools
import json
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
from owid.catalog import Dataset, DatasetMeta, Source, Table, VariableMeta
from owid.catalog.utils import underscore, validate_underscore
from owid.datautils import io
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
from etl.files import apply_black_formatter_to_files, yaml_dump
from etl.metadata_export import metadata_export
from etl.paths import (
    BASE_DIR,
    DAG_DIR,
    DATA_DIR,
    LATEST_REGIONS_DATASET_PATH,
    SNAPSHOTS_DIR,
    STEP_DIR,
)
from etl.snapshot import Snapshot, SnapshotMeta
from walkthrough import utils as walkthrough_utils

from . import csv, sheets

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
        dataset: Dataset,
        sheets_url: str,
    ):
        self.dataset = dataset
        self.sheets_url = sheets_url

    @property
    def meta(self) -> DatasetMeta:
        return self.dataset.metadata

    @property
    def data(self) -> Table:
        return self.dataset[self.meta.short_name]  # type: ignore

    @property
    def dataset_dir(self) -> Path:
        return STEP_DIR / "data" / "grapher" / self.meta.namespace / str(self.meta.version)  # type: ignore

    @property
    def step_path(self) -> Path:
        return self.dataset_dir / (self.meta.short_name + ".py")  # type: ignore

    @property
    def metadata_path(self) -> Path:
        return self.dataset_dir / (self.meta.short_name + ".meta.yml")  # type: ignore

    @property
    def snapshot(self) -> Snapshot:
        return Snapshot(f"{self.meta.namespace}/{self.meta.version}/{self.meta.short_name}.csv")

    @property
    def snapshot_meta(self) -> SnapshotMeta:
        # since sheets url is accessible with link, we have to encrypt it when storing in metadata
        sheets_url = _encrypt(self.sheets_url) if not self.meta.is_public else self.sheets_url

        source_name = "Google Sheet" if self.sheets_url != "local_csv" else "Local CSV"

        if len(self.meta.sources) == 1:
            dataset_source = self.meta.sources[0]
            source = Source(
                url=dataset_source.url,
                name=source_name,
                published_by=source_name,
                source_data_url=sheets_url,
                date_accessed=str(dt.date.today()),
                publication_year=dataset_source.publication_year,
            )
            origin = None
            license = self.meta.licenses[0]
        elif len(self.meta.origins) == 1:
            source = None
            origin = self.meta.origins[0]
            origin.date_accessed = str(dt.date.today())

            # Misuse the version field and dataset_url_download fields to store info about the spreadsheet
            origin.version = source_name
            origin.dataset_url_download = sheets_url
            license = self.meta.licenses[0]
        else:
            raise ValueError("Dataset must have either one source or one origin")

        return SnapshotMeta(
            namespace=self.meta.namespace,  # type: ignore
            short_name=self.meta.short_name,  # type: ignore
            name=self.meta.title,  # type: ignore
            version=str(self.meta.version),
            file_extension="csv",
            description=self.meta.description,  # type: ignore
            source=source,
            origin=origin,
            license=license,
            is_public=self.meta.is_public,
        )

    def snapshot_exists(self) -> bool:
        try:
            self.snapshot
            return True
        except FileNotFoundError:
            return False

    def dataset_yaml(self) -> str:
        """Generate dataset YAML file."""
        return yaml_dump(metadata_export(self.dataset))  # type: ignore

    def save_metadata(self) -> None:
        with open(self.metadata_path, "w") as f:
            f.write(self.dataset_yaml())

    def upload_snapshot(self) -> Path:
        # save snapshotmeta YAML file
        self.snapshot_meta.save()

        # upload snapshot
        snap = self.snapshot
        io.df_to_file(self.data, file_path=snap.path)
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

    dataset, sheets_url = _load_data_and_meta(dummies)

    fast_import = FasttrackImport(dataset, sheets_url)

    # diff with existing dataset
    if fast_import.snapshot_exists() and fast_import.metadata_path.exists():
        po.put_markdown("""## Data differences from existing dataset...""")
        _data_diff(fast_import)

        po.put_markdown("""## Metadata differences from existing dataset...""")
        _metadata_diff(fast_import)

        # if data_is_different or metadata_is_different:
        do_continue = _ask_to_continue()
        if not do_continue:
            return

    # add dataset to dag
    dag_content = _add_to_dag(dataset.metadata)

    # create step and metadata file
    walkthrough_utils.generate_step_to_channel(CURRENT_DIR / "grapher_cookiecutter/", fast_import.meta.to_dict())
    fast_import.save_metadata()

    po.put_markdown(
        """
    ## Uploading Snapshot...

    """
    )
    snapshot_path = fast_import.upload_snapshot()
    po.put_success("Upload successful!")

    po.put_markdown("""## Running ETL and upserting to GrapherDB...""")
    step = f"{dataset.metadata.uri}"
    log.info("fasttrack.etl", step=step)
    etl_main(
        dag_path=DAG_FASTTRACK_PATH,
        steps=[step],
        grapher=True,
        private=not dataset.metadata.is_public,
        workers=1,
        # NOTE: force is necessary because we are caching checksums with files.CACHE_CHECKSUM_FILE
        # we could have cleared the cache, but this is cleaner
        force=True,
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
    * [Dataset in admin]({os.environ.get("ADMIN_HOST", "")}/admin/datasets/{_dataset_id(dataset.metadata)})
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


def _load_data_and_meta(dummies: dict[str, str]) -> Tuple[Dataset, str]:
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
                # new sheet, use public by default
                pi.input_update("options", value=[Options.INFER_METADATA.value])
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
                        value=[Options.INFER_METADATA.value],
                    ),
                ],
            )
        )

        form = FasttrackForm(**form_dict)

        log.info("fasttrack.form", form={k: v for k, v in form_dict.items() if k not in ("local_csv",)})

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
            dataset_meta, variables_meta_dict = csv.parse_metadata_from_csv(form.local_csv["filename"], csv_df.columns)

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
                dataset_meta, variables_meta_dict = sheets.parse_metadata_from_sheets(
                    google_sheets["dataset_meta"],
                    google_sheets["variables_meta"],
                    google_sheets["sources_meta"],
                    google_sheets["origins_meta"],
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
            data, variables_meta_dict = _infer_metadata(data, variables_meta_dict)
            # add unknown source if we have neither sources nor origins
            if not dataset_meta.sources and not dataset_meta.origins:
                dataset_meta.sources = [
                    Source(
                        name="Unknown",
                        published_by="Unknown",
                        publication_year=dt.date.today().year,
                        date_accessed=str(dt.date.today()),
                    )
                ]

        # validation
        success = _validate_data(data, variables_meta_dict)

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

    dataset_meta.is_public = not form.is_private
    dataset_meta.channel = "grapher"

    # create table
    tb = Table(data, short_name=dataset_meta.short_name)
    for short_name, var_meta in variables_meta_dict.items():
        tb[short_name].metadata = var_meta

    # create dataset and add table
    dataset = Dataset.create_empty(DATA_DIR / dataset_meta.uri, dataset_meta)
    dataset.add(tb)
    dataset.save()

    return dataset, sheets_url


def _dataset_id(ds_meta: DatasetMeta) -> int:
    with Session(get_engine()) as session:
        ds = gm.Dataset.load_with_path(
            session, namespace=ds_meta.namespace, short_name=ds_meta.short_name, version=str(ds_meta.version)  # type: ignore
        )
        assert ds.id
        return ds.id


def _last_updated_before_minutes(dataset_meta: pd.DataFrame) -> int:
    updated = dataset_meta.set_index(0)[1].loc["updated"]
    if pd.isnull(updated):
        return 0
    else:
        td = pd.Timestamp.utcnow() - pd.to_datetime(
            dataset_meta.set_index(0)[1].loc["updated"], dayfirst=True, utc=True
        )
        return int(td.total_seconds() / 60)


def _load_existing_sheets_from_snapshots() -> List[Dict[str, str]]:
    # get all fasttrack snapshots
    metas = [SnapshotMeta.load_from_yaml(path) for path in (SNAPSHOTS_DIR / "fasttrack").rglob("*.dvc")]

    existing_sheets = []
    for meta in metas:
        # exclude local CSVs
        if (getattr(meta.source, "name", None) or getattr(meta.origin, "version")) == "Local CSV":
            continue

        if meta.source:
            assert meta.source.source_data_url
            url = meta.source.source_data_url
            date_accessed = meta.source.date_accessed
        elif meta.origin:
            assert meta.origin.dataset_url_download
            url = meta.origin.dataset_url_download
            date_accessed = meta.origin.date_accessed
        else:
            raise ValueError("Neither source nor origin")

        # decrypt URLs if private
        if not meta.is_public:
            url = _decrypt(url)

        existing_sheets.append(
            {
                "label": f"{meta.name} / {meta.version}",
                "value": url,
                "is_public": meta.is_public,
                "date_accessed": str(date_accessed),
            }
        )

    # sort them by date accessed
    existing_sheets.sort(key=lambda m: m["date_accessed"], reverse=True)  # type: ignore

    return existing_sheets


def _infer_metadata(
    data: pd.DataFrame, meta_variables: Dict[str, VariableMeta]
) -> Tuple[pd.DataFrame, Dict[str, VariableMeta]]:
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


def _validate_data(df: pd.DataFrame, variables_meta_dict: Dict[str, VariableMeta]) -> bool:
    po.put_markdown("""## Validating data and metadata...""")
    errors = []

    # check column names
    for col in df.columns:
        try:
            validate_underscore(col, "Variables")
        except NameError as e:
            errors.append(sheets.ValidationError(e))

    # missing columns in metadata
    for col in set(df.columns) - set(variables_meta_dict.keys()):
        errors.append(sheets.ValidationError(f"Variable {col} is not defined in metadata"))

    # extra columns in metadata
    for col in set(variables_meta_dict.keys()) - set(df.columns):
        errors.append(sheets.ValidationError(f"Variable {col} in metadata is not in the data"))

    # missing titles
    for col in df.columns:
        if col in variables_meta_dict and not variables_meta_dict[col].title:
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

    # Read the main table of the regions dataset.
    tb_regions = Dataset(LATEST_REGIONS_DATASET_PATH)["regions"][["name", "aliases"]]

    # Convert strings of lists of aliases into lists of aliases.
    tb_regions["aliases"] = [json.loads(alias) if pd.notnull(alias) else [] for alias in tb_regions["aliases"]]

    # Explode list of aliases to have one row per alias.
    tb_regions = tb_regions.explode("aliases").reset_index(drop=True)

    # Create a series that maps aliases to country names.
    alias_to_country = tb_regions.rename(columns={"aliases": "alias"}).set_index("alias")["name"]

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


def _add_to_dag(ds_meta: DatasetMeta) -> str:
    public_data_step = f"data://{ds_meta.uri}"
    private_data_step = f"data-private://{ds_meta.uri}"

    # add steps to dag, replace public by private and vice versa if needed
    snapshot_uri = ds_meta.uri.replace("grapher/", "")
    if ds_meta.is_public:
        to_remove = private_data_step
        to_add = {public_data_step: [f"snapshot://{snapshot_uri}.csv"]}
    else:
        to_remove = public_data_step
        to_add = {private_data_step: [f"snapshot-private://{snapshot_uri}.csv"]}

    walkthrough_utils.remove_from_dag(
        to_remove,
        DAG_FASTTRACK_PATH,
    )
    return walkthrough_utils.add_to_dag(
        to_add,
        DAG_FASTTRACK_PATH,
    )


def _data_diff(fast_import: FasttrackImport) -> bool:
    console = Console(record=True)

    # load data from snapshot
    if not fast_import.snapshot.path.exists():
        fast_import.snapshot.pull()
    existing_data = pd.read_csv(fast_import.snapshot.path)

    exit_code = diff_print(
        df1=existing_data,
        df2=fast_import.data.reset_index(),
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


def _metadata_diff(fast_import: FasttrackImport) -> bool:
    # load existing metadata file
    with open(fast_import.metadata_path, "r") as f:
        existing_meta = f.read()

    # load old snapshot metadata from path
    old_snapshot_yaml = fast_import.snapshot.metadata.to_yaml()
    # create new snapshot metadata
    new_snapshot_yaml = fast_import.snapshot_meta.to_yaml()

    # create metadata file
    new_meta_yaml = fast_import.dataset_yaml()

    # combine snapshot YAML and grapher YAML file
    diff = difflib.HtmlDiff()
    html_diff = diff.make_table(
        (existing_meta + old_snapshot_yaml).split("\n"), (new_meta_yaml + new_snapshot_yaml).split("\n"), context=True
    )
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
