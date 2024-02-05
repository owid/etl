"""Fast-track import."""
import datetime as dt
import difflib
import json
import os
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
from structlog import get_logger

import apps.fasttrack.sheets as sheets
from apps.wizard import utils as wizard_utils
from etl.compare import diff_print
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
        sheets_url: str,
    ):
        self.dataset = dataset
        self.origin = origin
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

        if len(self.meta.sources) == 1:
            dataset_source = self.meta.sources[0]
            source = Source(
                url=dataset_source.url,
                name=dataset_source.name,
                published_by=dataset_source.published_by,
                source_data_url=sheets_url,
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
            origin.version_producer = "Google Sheet" if self.sheets_url != "local_csv" else "Local CSV"
            origin.url_download = sheets_url
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

    st.markdown(html, unsafe_allow_html=True)

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
        st.success("No metadata differences found.")
        return False
    else:
        st.markdown(html_diff, unsafe_allow_html=True)
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
        st.success("Data and metadata is valid")
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
