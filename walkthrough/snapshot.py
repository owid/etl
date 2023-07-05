import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from botocore.exceptions import ClientError
from owid.catalog import s3_utils
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po
from pywebio.session import go_app

from . import utils

CURRENT_DIR = Path(__file__).parent


class Options(Enum):
    IS_PRIVATE = "Make dataset private"
    DATASET_MANUAL_IMPORT = "Import dataset from local file"


class SnapshotForm(BaseModel):
    namespace: str
    snapshot_version: str
    short_name: str
    name: str
    source_name: str
    source_published_by: str
    publication_year: Optional[str]
    publication_date: Optional[str]
    url: str
    source_data_url: str
    file_extension: str
    license_name: str
    license_url: str
    description: str
    is_private: bool
    dataset_manual_import: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["is_private"] = Options.IS_PRIVATE.value in options
        data["dataset_manual_import"] = Options.DATASET_MANUAL_IMPORT.value in options
        super().__init__(**data)

    @property
    def version(self) -> str:
        return self.snapshot_version or self.publication_year or self.publication_date  # type: ignore


def app(run_checks: bool) -> None:
    state = utils.APP_STATE

    with open(CURRENT_DIR / "snapshot.md", "r") as f:
        po.put_markdown(f.read())

    # run checks
    if run_checks:
        _check_aws_profile()
        _check_s3_connection()

    # get info from user
    data = pi.input_group(
        "Dataset details",
        [
            pi.input(
                "Namespace",
                name="namespace",
                placeholder="institution",
                required=True,
                value=state.get("namespace"),
                help_text="Institution name. Example: emdat",
            ),
            pi.input(
                "Snapshot version",
                name="snapshot_version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("snapshot_version", str(dt.date.today())),
                help_text="Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.input(
                "Snapshot dataset short name",
                name="short_name",
                placeholder="testing_dataset_name",
                required=True,
                value=state.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored dataset short name. Example: natural_disasters",
            ),
            pi.input(
                "Dataset full name",
                name="name",
                placeholder="Testing Dataset Name (Institution, 2023)",
                required=True,
                value=state.get("name"),
                help_text="Human-readable dataset name, followed by (Institution, Year of version). Example: Natural disasters (EMDAT, 2022)",
            ),
            pi.input(
                "Source short citation",
                name="source_name",
                placeholder="Testing Short Citation",
                required=True,
                value=state.get("source_name"),
                help_text="Short source citation (to show in charts). Example: EM-DAT",
            ),
            pi.input(
                "Source full citation",
                name="source_published_by",
                placeholder="Testing Full Citation",
                required=True,
                value=state.get("source_published_by"),
                help_text="Testing Full Citation, as recommended by the source. Example: EM-DAT, CRED / UCLouvain, Brussels, Belgium",
            ),
            pi.input(
                "Publication date",
                name="publication_date",
                placeholder="",
                value=state.get("publication_date"),
                help_text="Date when the dataset was published by the source. Example: 2023-01-01",
            ),
            pi.input(
                "Publication year",
                name="publication_year",
                type=pi.NUMBER,
                placeholder="",
                help_text="Only if the exact publication date is unknown, year when the dataset was published by the source. Example: 2023",
            ),
            pi.input(
                "Dataset webpage URL",
                name="url",
                placeholder=("https://url_of_testing_source.com/"),
                required=True,
                value=state.get("url"),
                help_text="URL to the main page of the project.",
            ),
            pi.input(
                "Dataset download URL",
                name="source_data_url",
                placeholder="https://url_of_testing_source.com/data.csv",
                value=state.get("source_data_url"),
                help_text="URL to download the data file.",
            ),
            pi.input(
                "File extension",
                name="file_extension",
                placeholder="csv",
                value=state.get("file_extension"),
                help_text="File extension (without the '.') of the file to be downloaded. Example: csv",
            ),
            pi.input(
                "License URL",
                name="license_url",
                placeholder=("https://url_of_testing_source.com/license"),
                help_text="URL to the page where the source specifies the license of the dataset.",
            ),
            pi.input(
                "License name",
                name="license_name",
                placeholder="Creative Commons BY 4.0",
                help_text="Name of the dataset license. Example: 'Creative Commons BY 4.0'",
            ),
            pi.textarea(
                "Description", name="description", value=state.get("description"), help_text="Dataset description."
            ),
            pi.checkbox(
                "Other options",
                options=[
                    Options.IS_PRIVATE.value,
                    Options.DATASET_MANUAL_IMPORT.value,
                ],
                name="options",
            ),
        ],
    )
    form = SnapshotForm(**data)

    # save form data to global state for next steps
    state.update(form.dict())

    # use multi-line description
    form.description = form.description.replace("\n", "\n  ")

    # cookiecutter on python files
    SNAPSHOT_DIR = utils.generate_step(
        CURRENT_DIR / "snapshot_cookiecutter/", dict(**form.dict(), version=form.snapshot_version, channel="snapshots")
    )

    ingest_path = SNAPSHOT_DIR / (form.short_name + ".py")
    meta_path = SNAPSHOT_DIR / f"{form.short_name}.{form.file_extension}.dvc"

    po.put_markdown(
        f"""
## Next steps

1. Verify that generated files are correct and update them if necessary

2. Run the snapshot step to upload files to S3
```bash
python snapshots/{form.namespace}/{form.version}/{form.short_name}.py
```

3. Continue to the meadow step
"""
    )
    po.put_buttons(["Go to meadow"], [lambda: go_app("meadow", new_window=False)])
    po.put_markdown(
        """
## Generated files
"""
    )

    utils.preview_file(meta_path, "yaml")
    utils.preview_file(ingest_path, "python")

    return


def _check_aws_profile() -> None:
    po.put_markdown("""## Checking AWS profile in ~/.aws/config...""")
    try:
        s3_utils.check_for_default_profile()
        po.put_success("AWS profile is valid")
    except s3_utils.MissingCredentialsError as e:
        po.put_error("Invalid AWS profile:\n{}".format(e))
        raise e


def _check_s3_connection() -> None:
    po.put_markdown("""## Trying to connect to S3 on DigitalOcean...""")
    s3 = s3_utils.connect()

    try:
        buckets = s3.list_buckets()["Buckets"]
        po.put_success("Connected to S3")
    except ClientError as e:
        po.put_error("Error connecting to S3:\n{}".format(e))
        raise e

    bucket_names = [b["Name"] for b in buckets]
    if "owid-catalog" not in bucket_names:
        po.put_error(po.put_markdown("`owid-catalog` bucket not found"))
        raise Exception()
