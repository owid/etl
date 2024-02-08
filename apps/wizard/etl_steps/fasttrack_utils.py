"""Fast-track import."""
import datetime as dt
import difflib
import json
import os
import urllib.error
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from cryptography.fernet import Fernet
from git.repo import Repo
from owid.catalog import Dataset, DatasetMeta, Origin, Source, Table, VariableMeta
from owid.catalog.utils import underscore, validate_underscore
from owid.datautils import io
from rich.console import Console
from sqlmodel import Session
from structlog import get_logger

import apps.fasttrack.csv as csv
import apps.fasttrack.sheets as sheets
from apps.wizard import utils as wizard_utils
from etl import grapher_model as gm
from etl.compare import diff_print
from etl.db import get_engine
from etl.files import apply_ruff_formatter_to_files, yaml_dump
from etl.metadata_export import metadata_export
from etl.paths import BASE_DIR, DAG_DIR, LATEST_REGIONS_DATASET_PATH, SNAPSHOTS_DIR, STEP_DIR
from etl.snapshot import Snapshot, SnapshotMeta

log = get_logger()
DAG_FASTTRACK_PATH = DAG_DIR / "fasttrack.yml"


class FasttrackImport:
    def __init__(
        self,
        dataset: Dataset,
        origin: Optional[Origin],
        dataset_uri: str,
        is_gsheet: bool = False,
    ):
        self.dataset = dataset
        self.origin = origin
        self.dataset_uri = dataset_uri
        self.is_gsheet = is_gsheet

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
        dataset_uri = _encrypt(self.dataset_uri) if not self.meta.is_public else self.dataset_uri

        if len(self.meta.sources) == 1:
            dataset_source = self.meta.sources[0]
            source = Source(
                url=dataset_source.url,
                name=dataset_source.name,
                published_by=dataset_source.published_by,
                source_data_url=dataset_uri,
                date_accessed=str(dt.date.today()),
                publication_year=dataset_source.publication_year
                if not pd.isnull(dataset_source.publication_year)
                else None,
            )
            origin = None
            license = self.meta.licenses[0]
        elif self.origin:
            source = None
            origin = self.origin
            origin.date_accessed = str(dt.date.today())

            # Misuse the version field and url_download fields to store info about the spreadsheet
            origin.version_producer = "Google Sheet" if self.is_gsheet else "Local CSV"
            origin.url_download = dataset_uri
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
        yml = metadata_export(self.dataset)
        # source is already in the snapshot and is propagated
        yml["dataset"].pop("sources", None)
        return yaml_dump(yml)  # type: ignore

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


def _data_diff(fast_import: FasttrackImport) -> bool:
    """Get difference between existing and imported data."""
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
    html = html.replace('"', "'")
    st.markdown(
        f'<iframe srcdoc="{html}" width="100%" style="border: 1px solid black; background: white"></iframe>',
        unsafe_allow_html=True,
    )

    return exit_code != 0


def _diff_files_as_list(current, new):
    d = difflib.Differ()
    diff = list(d.compare(current, new))
    diff_html = "<pre>"
    for line in diff:
        if line.startswith("+"):
            diff_html += f"<span style='color:green;'>{line}</span>"
        elif line.startswith("-"):
            diff_html += f"<span style='color:red;'>{line}</span>"
        else:
            diff_html += line
    diff_html += "</pre>"
    return diff_html


def _metadata_diff(fast_import: FasttrackImport) -> bool:
    # Load existing metadata
    ## Grapher
    with open(fast_import.metadata_path, "r") as f:
        meta_now_grapher = f.readlines()
    ## Snapshot
    meta_now_snapshot = fast_import.snapshot.metadata.to_yaml().split("\n")
    meta_now_snapshot = [line + "\n" for line in meta_now_snapshot if line]
    ## Combine
    meta_now = meta_now_snapshot + meta_now_grapher

    # Load new metadata
    ## Snapshot
    meta_new_snapshot = fast_import.snapshot_meta.to_yaml().split("\n")
    meta_new_snapshot = [line + "\n" for line in meta_new_snapshot if line]
    ## Grapher
    meta_new_grapher = fast_import.dataset_yaml().split("\n")
    meta_new_grapher = [line + "\n" for line in meta_new_grapher if line]
    ## Combine
    meta_new = meta_new_snapshot + meta_new_grapher

    # Compare Snapshot
    html_diff = _diff_files_as_list(meta_now, meta_new)
    if "No Differences Found" in html_diff:
        st.success("No metadata differences found in Snapshot metadata.")
        return False
    else:
        st.markdown(
            f'<iframe srcdoc="{html_diff}" width="100%" height="400px" style="border: 1px solid black; background: white"></iframe>',
            unsafe_allow_html=True,
        )
        return True


def _load_existing_sheets_from_snapshots() -> List[Dict[str, str]]:
    # get all fasttrack snapshots
    metas = [SnapshotMeta.load_from_yaml(path) for path in (SNAPSHOTS_DIR / "fasttrack").rglob("*.dvc")]

    existing_sheets = []
    for meta in metas:
        # exclude local CSVs
        if (getattr(meta.source, "name", None) or getattr(meta.origin, "version_producer")) == "Local CSV":
            continue

        if meta.source:
            assert meta.source.source_data_url
            url = meta.source.source_data_url
            date_accessed = meta.source.date_accessed
        elif meta.origin:
            assert meta.origin.url_download
            url = meta.origin.url_download
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


def _get_secret_key() -> Optional[Fernet]:
    secret_key = os.environ.get("FASTTRACK_SECRET_KEY")
    if not secret_key:
        log.warning("FASTTRACK_SECRET_KEY not found in environment variables. Not using encryption.")
        return None
    return Fernet(secret_key)


def _add_to_dag(ds_meta: DatasetMeta) -> str:
    """Add dataset to the DAG."""
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

    wizard_utils.remove_from_dag(
        to_remove,
        DAG_FASTTRACK_PATH,
    )
    return wizard_utils.add_to_dag(
        to_add,
        DAG_FASTTRACK_PATH,
    )


def _commit_and_push(fast_import: FasttrackImport, snapshot_path: Path) -> str:
    """Format generated files, commit them and push to GitHub."""
    apply_ruff_formatter_to_files([fast_import.step_path])

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


def _last_updated_before_minutes(dataset_meta: pd.DataFrame) -> int:
    updated = dataset_meta.set_index(0)[1].loc["updated"]
    if pd.isnull(updated):
        return 0
    else:
        td = pd.Timestamp.utcnow() - pd.to_datetime(
            dataset_meta.set_index(0)[1].loc["updated"], dayfirst=True, utc=True
        )
        return int(td.total_seconds() / 60)


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
            st.warning(
                st.markdown(
                    f"`{short_name}` isn't in [snake_case](https://en.wikipedia.org/wiki/Snake_case) format and was renamed to `{new_short_name}`. Please update it in your sheet `variables_meta`."
                )
            )
            meta_variables[new_short_name] = meta_variables.pop(short_name)

    return data, meta_variables


def _validate_data(df: pd.DataFrame, variables_meta_dict: Dict[str, VariableMeta]) -> bool:
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
        for error in errors:
            st.exception(error)
        return False
    else:
        return True


def _harmonize_countries(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Check if all countries are harmonized."""
    # Read the main table of the regions dataset.
    tb_regions = Dataset(LATEST_REGIONS_DATASET_PATH)["regions"][["name", "aliases", "iso_alpha2", "iso_alpha3"]]

    # First convert ISO2 and ISO3 country codes.
    df = df.reset_index()
    for iso_col in ["iso_alpha2", "iso_alpha3"]:
        df["country"] = df["country"].replace(tb_regions.set_index(iso_col)["name"])
        # lowercase
        df["country"] = df["country"].replace(
            tb_regions.assign(**{iso_col: tb_regions.iso_alpha2.str.lower()}).set_index(iso_col)["name"]
        )

    # Convert strings of lists of aliases into lists of aliases.
    tb_regions["aliases"] = [json.loads(alias) if pd.notnull(alias) else [] for alias in tb_regions["aliases"]]

    # Explode list of aliases to have one row per alias.
    tb_regions = tb_regions.explode("aliases").reset_index(drop=True)

    # Create a series that maps aliases to country names.
    alias_to_country = tb_regions.rename(columns={"aliases": "alias"}).set_index("alias")["name"]

    unknown_countries = []

    for country in set(df.country):
        # country is in reference dataset
        if country in alias_to_country.values:
            continue

        # there is an alias for this country
        elif country in alias_to_country.index:
            df.country = df.country.replace({country: alias_to_country[country]})
            st.warning(f"Country `{country}` harmonized to `{alias_to_country.loc[country]}`")

        # unknown country
        else:
            unknown_countries.append(country)

    df.set_index(["country", "year"], inplace=True)

    return df, unknown_countries


# Read file methods
IMPORT_GSHEET = "import_gsheet"
UPDATE_GSHEET = "update_gsheet"
LOCAL_CSV = "local_csv"


@st.cache_data(show_spinner=False)
def load_data_from_resource(import_method, dataset_uri, infer_metadata, is_private, _status):
    # 1/ LOCAL CSV
    if import_method == LOCAL_CSV:
        # Get filename, show notification
        data, dataset_meta, variables_meta_dict, origin = load_data_from_csv(dataset_uri)

    # 2/ GOOGLE SHEET (New or existing)
    else:
        # Get filename, show notification
        sheets_url = dataset_uri
        if import_method in (UPDATE_GSHEET, IMPORT_GSHEET):
            dataset_uri = sheets_url["value"]
        data, dataset_meta, variables_meta_dict, origin = load_data_from_sheets(dataset_uri, _status=_status)

    # PROCES
    if infer_metadata:
        st.write("Inferring metadata...")
        data, variables_meta_dict = _infer_metadata(data, variables_meta_dict)
        # add unknown source if we have neither sources nor origins
        if not dataset_meta.sources and not origin:
            dataset_meta.sources = [
                Source(
                    name="Unknown",
                    published_by="Unknown",
                    publication_year=dt.date.today().year,
                    date_accessed=str(dt.date.today()),
                )
            ]

    # VALIDATION
    st.write("Validating data and metadata...")
    success = _validate_data(data, variables_meta_dict)
    if not success:
        _status.update(state="error")
        st.stop()

    # HARMONIZATION
    # NOTE: harmonization is not done in ETL, but here in fast-track for technical reasons
    # It's not yet clear what will authors prefer and how should we handle preprocessing from
    # raw data to data saved as snapshot
    st.write("Harmonizing countries...")
    data, unknown_countries = _harmonize_countries(data)
    if unknown_countries:
        st.error(f"There are {len(unknown_countries)} unknown entities!")
        _status.update(state="error")

    # Update dataset metadata
    dataset_meta.is_public = not is_private

    return data, dataset_meta, variables_meta_dict, origin, unknown_countries, dataset_uri


@st.cache_data(show_spinner=False)
def load_data_from_csv(uploaded_file):
    print("RUNNING FOR SHEETS")
    # Read CSV file as a dataframe
    st.write("Importing CSV...")
    csv_df = pd.read_csv(uploaded_file)

    # Parse dataframe
    st.write("Parsing data...")
    data = csv.parse_data_from_csv(csv_df)

    # Obtain dataset and other objects
    st.write("Parsing metadata...")
    dataset_meta, variables_meta_dict, origin = csv.parse_metadata_from_csv(
        uploaded_file.name,
        csv_df.columns,
    )

    # Success message
    st.success("Data imported from CSV")

    return data, dataset_meta, variables_meta_dict, origin


@st.cache_data(show_spinner=False)
def load_data_from_sheets(sheets_url, _status):
    # Show status progress as we import data
    st.info(
        """
        Note that Google Sheets refreshes its published version every 5 minutes, so you may need to wait a bit after you update your data.
        """
    )
    # Sanity check
    st.write("Sanity checks...")
    if "?output=csv" not in sheets_url:
        st.exception(
            sheets.ValidationError(
                f"URL does not contain `?output=csv`. Have you published it as CSV and not as HTML by accident? URL was {sheets_url}"
            )
        )
        _status.update(state="error")
        st.stop()
    else:
        try:
            # Import data from Google Sheets
            st.write(f"Importing [sheet]({sheets_url.replace('?output=csv', '')})...")
            google_sheets = sheets.import_google_sheets(sheets_url)
            # TODO: it would make sense to repeat the import until we're sure that it has been updated
            # we wouldn't risk importing data that is not up to date then
            # the question is how much can we trust the timestamp in the published version

            # Parse data into dataframe
            st.write("Parsing data...")
            data = sheets.parse_data_from_sheets(google_sheets["data"])

            # Obtain dataset and other objects
            st.write("Creating dataset...")
            dataset_meta, variables_meta_dict, origin = sheets.parse_metadata_from_sheets(
                google_sheets["dataset_meta"],
                google_sheets["variables_meta"],
                google_sheets["sources_meta"],
                google_sheets["origins_meta"],
            )

        except urllib.error.HTTPError:
            st.exception(
                sheets.ValidationError(
                    "Sheet not found, have you copied the template? Creating new Google Sheets document or new "
                    "sheets with the same name in the existing document does not work."
                )
            )
            _status.update(state="error")
            st.stop()
        except sheets.ValidationError as e:
            st.exception(e)
            st.stop()
        else:
            st.success(
                f"Data imported (sheet refreshed {_last_updated_before_minutes(google_sheets['dataset_meta'])} minutes ago)"
            )

    return data, dataset_meta, variables_meta_dict, origin


def _dataset_id(ds_meta: DatasetMeta) -> int:
    """Get dataset ID from dataset."""
    with Session(get_engine()) as session:
        ds = gm.Dataset.load_with_path(
            session,
            namespace=ds_meta.namespace,
            short_name=ds_meta.short_name,
            version=str(ds_meta.version),  # type: ignore
        )
        assert ds.id
        return ds.id
