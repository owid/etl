"""Definition of FasttrackImport object (mainly backend)."""
import datetime as dt
import difflib
import html
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from git.repo import Repo
from owid.catalog import Dataset, DatasetMeta, Origin, Source, Table
from owid.datautils import io
from rich.console import Console
from sqlmodel import Session
from structlog import get_logger

from apps.utils.files import add_to_dag
from apps.wizard import utils as wizard_utils
from apps.wizard.pages.fasttrack.utils import _encrypt
from etl import grapher_model as gm
from etl.compare import diff_print
from etl.db import get_engine
from etl.files import apply_ruff_formatter_to_files, yaml_dump
from etl.metadata_export import metadata_export
from etl.paths import BASE_DIR, DAG_DIR, STEP_DIR
from etl.snapshot import Snapshot, SnapshotMeta

log = get_logger()
# Paths
DAG_FASTTRACK_PATH = DAG_DIR / "fasttrack.yml"


class FasttrackImport:
    """Helps handling Fasttrack imports.

    Import `dataset`, with `origin` and `dataset_uri`.
    """

    def __init__(
        self,
        dataset: Dataset,
        origin: Optional[Origin],
        dataset_uri: str,
        is_gsheet: bool = False,
    ) -> None:
        """Construct object.

        dataset: Dataset to be imported.
        origin: Origin of the dataset.
        dataset_uri: URI of the dataset.
        is_gsheet: Whether the dataset is a Google Sheet.
        """
        self.dataset = dataset
        self.origin = origin
        self.dataset_uri = dataset_uri
        self.is_gsheet = is_gsheet
        self.__snapshot_path = None

    # Dataset-level (Grapher) properties
    @property
    def meta(self) -> DatasetMeta:
        """Dataset's metadata."""
        return self.dataset.metadata

    @property
    def data(self) -> Table:
        """Dataset's data.

        The data of the dataset consists of its one and only table.
        """
        return self.dataset[self.meta.short_name]  # type: ignore

    @property
    def dataset_dir(self) -> Path:
        """Directory of the dataset."""
        return STEP_DIR / "data" / "grapher" / self.meta.namespace / str(self.meta.version)  # type: ignore

    @property
    def step_path(self) -> Path:
        """Path to the step python script."""
        return self.dataset_dir / (self.meta.short_name + ".py")  # type: ignore

    @property
    def metadata_path(self) -> Path:
        """Path to the step's metadata YAML file."""
        return self.dataset_dir / (self.meta.short_name + ".meta.yml")  # type: ignore

    # Snapshot-level properties
    @property
    def snapshot(self) -> Snapshot:
        """Get Snapshot of the import."""
        return Snapshot(f"{self.meta.namespace}/{self.meta.version}/{self.meta.short_name}.csv")

    @property
    def snapshot_meta(self) -> SnapshotMeta:
        """Get snapshot metadata of the import."""
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

    @property
    def snapshot_path(self):
        """Path to the snapshot."""
        if self.__snapshot_path is None:
            raise Exception("Snapshot path not set! Should run `upload_snapshot` first.")
        return self.__snapshot_path

    #  Other snapshot functions
    def snapshot_exists(self) -> bool:
        """Check if snapshot exists."""
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
        """Save dataset YAML metadata file as `self.metadata_path`."""
        with open(self.metadata_path, "w") as f:
            f.write(self.dataset_yaml())

    def upload_snapshot(self) -> Path:
        """Upload snapshot and save metadata."""
        # save snapshotmeta YAML file
        self.snapshot_meta.save()

        # upload snapshot
        snap = self.snapshot
        io.df_to_file(self.data, file_path=snap.path)
        snap.dvc_add(upload=True)

        # save metadata
        self.__snapshot_path = snap.metadata_path

        return snap.metadata_path

    # Check diff
    def data_diff(self) -> Tuple[bool, str]:
        """Get difference between existing and imported data.

        Only applicable when the Import is from a Google Sheet that already exists in ETL.
        """
        console = Console(record=True)
        # load data from snapshot
        if not self.snapshot.path.exists():
            self.snapshot.pull()
        existing_data = pd.read_csv(self.snapshot.path)

        exit_code = diff_print(
            df1=existing_data,
            df2=self.data.reset_index(),
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

        return (
            exit_code != 0,
            html,
        )

    def metadata_diff(self) -> Tuple[bool, str]:
        """Get differences in metadata files.

        Tuple with two elements:
            - Are different: True if files are different.
            - Text: Message.
        """
        # Load existing metadata
        meta_now_grapher = self.metadata_path.read_text().splitlines(keepends=True)
        meta_now_snapshot = self.snapshot.metadata.to_yaml().splitlines(keepends=True)
        meta_now = meta_now_snapshot + meta_now_grapher

        # Load new metadata
        meta_new_snapshot = self.snapshot_meta.to_yaml().splitlines(keepends=True)
        meta_new_grapher = self.dataset_yaml().splitlines(keepends=True)
        meta_new = meta_new_snapshot + meta_new_grapher

        # Compare Snapshot
        html_diff = _diff_files_as_list(meta_now, meta_new)
        if "No Differences Found" in html_diff:
            return (
                False,
                "No metadata differences found in Snapshot metadata.",
            )
        else:
            return (
                True,
                f'<iframe srcdoc="{html.escape(html_diff)}" width="100%" height="400px" style="border: 1px solid black; background: white"></iframe>',
            )

    def commit_and_push(self) -> str:
        """Format generated files, commit them and push to GitHub."""
        apply_ruff_formatter_to_files([self.step_path])

        repo = Repo(BASE_DIR)
        repo.index.add(
            [
                str(self.snapshot_path),
                str(self.metadata_path),
                str(self.step_path),
                str(DAG_FASTTRACK_PATH),
            ]
        )
        commit = repo.index.commit(f"fasttrack: {self.snapshot.uri}")
        origin = repo.remote(name="origin")
        origin.push()

        github_link = f"https://github.com/owid/etl/commit/{commit.hexsha}"
        return github_link

    def add_to_dag(self) -> str:
        """Update the DAG with dataset steps."""
        ds_meta = self.dataset.metadata

        public_data_step = f"data://{ds_meta.uri}"
        private_data_step = f"data-private://{ds_meta.uri}"

        # Determine the steps to add and remove based on dataset visibility
        snapshot_uri = ds_meta.uri.replace("grapher/", "")
        if ds_meta.is_public:
            to_remove = private_data_step
            to_add = {public_data_step: [f"snapshot://{snapshot_uri}.csv"]}
        else:
            to_remove = public_data_step
            to_add = {private_data_step: [f"snapshot-private://{snapshot_uri}.csv"]}

        # Remove the step from the DAG
        wizard_utils.remove_from_dag(to_remove, DAG_FASTTRACK_PATH)

        # Add the step to the DAG
        return add_to_dag(to_add, DAG_FASTTRACK_PATH)

    @property
    def dataset_id(self) -> int:
        """Get dataset ID from dataset."""
        with Session(get_engine()) as session:
            ds = gm.Dataset.load_with_path(
                session,
                namespace=str(self.dataset.metadata.namespace),
                short_name=str(self.dataset.metadata.short_name),
                version=str(self.dataset.metadata.version),  # type: ignore
            )
            assert ds.id, "No ID found in dataset object!"
            return ds.id


def _diff_files_as_list(current, new):
    """Get differences item by item from `current` and `new` lists."""
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
