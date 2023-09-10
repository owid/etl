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

from etl.paths import SNAPSHOTS_DIR

from . import utils

CURRENT_DIR = Path(__file__).parent


class Options(Enum):
    IS_PRIVATE = "Make dataset private"
    DATASET_MANUAL_IMPORT = "Import dataset from local file"


class SnapshotFormWithOrigin(BaseModel):
    namespace: str
    snapshot_version: str
    short_name: str
    is_private: bool
    dataset_manual_import: bool
    file_extension: str

    # origin
    dataset_title_owid: str
    dataset_title_producer: str
    dataset_description_owid: str
    dataset_description_producer: str
    producer: str
    citation_producer: str
    attribution: str
    attribution_short: str
    origin_version: str
    dataset_url_main: str
    dataset_url_download: str
    date_accessed: str
    date_published: str

    # license
    license_url: str
    license_name: str

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["is_private"] = Options.IS_PRIVATE.value in options
        data["dataset_manual_import"] = Options.DATASET_MANUAL_IMPORT.value in options
        super().__init__(**data)


class SnapshotFormWithSource(BaseModel):
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

    po.put_markdown("# Walkthrough - Snapshot")
    with open(CURRENT_DIR / "snapshot.md", "r") as f:
        # if utils.WALKTHROUGH_ORIGINS:
        #     po.put_info(po.put_markdown("To use **Sources**, call it with `WALKTHROUGH_ORIGINS=0 walkthrough ...` "))
        if not utils.WALKTHROUGH_ORIGINS:
            po.put_info(po.put_markdown("To use **Origins**, call it with `WALKTHROUGH_ORIGINS=1 walkthrough ...` "))
        po.put_collapse("Instructions", [po.put_markdown(f.read())])

    # run checks
    if run_checks:
        _check_aws_profile()
        _check_s3_connection()

    # get info from user
    if utils.WALKTHROUGH_ORIGINS:
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
                    "Version",
                    name="snapshot_version",
                    placeholder=str(dt.date.today()),
                    required=True,
                    value=state.get("snapshot_version", str(dt.date.today())),
                    help_text="Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
                ),
                pi.input(
                    "Short name",
                    name="short_name",
                    placeholder="testing_dataset_name",
                    required=True,
                    value=state.get("short_name"),
                    validate=utils.validate_short_name,
                    help_text="Underscored dataset short name. Example: natural_disasters",
                ),
                pi.input(
                    "Origin: Dataset title by OWID",
                    name="dataset_title_owid",
                    placeholder="Testing Title OWID",
                    required=True,
                    value=state.get("dataset_title_owid"),
                    help_text="Dataset title written by OWID (without a year). Example: Natural disasters",
                ),
                pi.input(
                    "Origin: Dataset title by the Producer",
                    name="dataset_title_producer",
                    placeholder="Testing Title Producer",
                    value=state.get("dataset_title_producer"),
                    help_text="Dataset title written by Producer (without a year). Example: Natural disasters",
                ),
                pi.textarea(
                    "Origin: Dataset description by OWID",
                    name="dataset_description_owid",
                    value=state.get("dataset_description_owid"),
                    help_text="Our description of the dataset.",
                ),
                pi.textarea(
                    "Origin: Dataset description by the Producer",
                    name="dataset_description_producer",
                    value=state.get("dataset_description_producer"),
                    help_text="The description for this dataset used by the producer.",
                ),
                pi.input(
                    "Origin: Producer",
                    name="producer",
                    placeholder="Testing Institution",
                    value=state.get("producer"),
                    help_text="The name of the institution (without a year) or the main authors of the paper. Example: EM-DAT",
                ),
                # TODO: should it contain year too? it would be good to clarify
                pi.input(
                    "Origin: Citation by the Producer",
                    name="citation_producer",
                    placeholder="Testing Citation",
                    value=state.get("citation_producer"),
                    help_text="The full citation that the producer asks for. Example: EM-DAT, CRED / UCLouvain, Brussels, Belgium",
                ),
                pi.input(
                    "Origin: Attribution",
                    name="attribution",
                    placeholder="Attribution",
                    value=state.get("attribution"),
                    help_text="This will be often empty and then producer is used instead, but for the (relatively common) cases where the data product is more famous than the authors we would use this (e.g. VDEM instead of the first authors)",
                ),
                pi.input(
                    "Origin: Attribution short",
                    name="attribution_short",
                    placeholder="Attribution short",
                    value=state.get("attribution_short"),
                    help_text="Short version of attribution",
                ),
                pi.input(
                    "Origin: Version",
                    name="origin_version",
                    placeholder="Testing Citation",
                    value=state.get("origin_version"),
                    help_text="This will be often empty but if not then it will be part of the short citation (e.g. for VDEM)",
                ),
                pi.input(
                    "Origin: Dataset main URL",
                    name="dataset_url_main",
                    placeholder="https://url_of_testing_source.com/",
                    value=state.get("dataset_url_main"),
                    help_text="URL to the main page of the project.",
                ),
                pi.input(
                    "Origin: Dataset download URL",
                    name="dataset_url_download",
                    placeholder="https://url_of_testing_source.com/data.csv",
                    value=state.get("dataset_url_download"),
                    help_text="Direct URL to download the dataset.",
                ),
                pi.input(
                    "Origin: Publication date",
                    name="date_published",
                    placeholder="2023-01-01",
                    value=state.get("date_published"),
                    help_text="Date when the dataset was published, could be date or year. Example: 2023-01-01 or 2023",
                ),
                pi.input(
                    "Origin: Date accessed",
                    name="date_accessed",
                    placeholder="",
                    value=str(dt.date.today()),
                    help_text="Accessed date, usually today's date.",
                ),
                pi.input(
                    "License: URL",
                    name="license_url",
                    value=state.get("license_url"),
                    placeholder=("https://url_of_testing_source.com/license"),
                    help_text="URL to the page where the source specifies the license of the dataset.",
                ),
                pi.input(
                    "License: name",
                    name="license_name",
                    placeholder="Creative Commons BY 4.0",
                    value=state.get("license_name"),
                    help_text="Name of the dataset license. Example: 'Creative Commons BY 4.0'",
                ),
                pi.input(
                    "File extension",
                    name="file_extension",
                    placeholder="csv",
                    value=state.get("file_extension"),
                    help_text="File extension (without the '.') of the file to be downloaded. Example: csv",
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
        form = SnapshotFormWithOrigin(**data)
    else:
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
        form = SnapshotFormWithSource(**data)

    # save form data to global state for next steps
    state.update(form.dict())

    # cookiecutter on python files
    if utils.WALKTHROUGH_ORIGINS:
        cookiecutter_path = CURRENT_DIR / "snapshot_origins_cookiecutter/"
    else:
        cookiecutter_path = CURRENT_DIR / "snapshot_cookiecutter/"

    utils.generate_step(
        cookiecutter_path,
        dict(**form.dict(), channel="snapshots", walkthrough_origins=utils.WALKTHROUGH_ORIGINS),
        SNAPSHOTS_DIR,
    )

    ingest_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / (form.short_name + ".py")
    meta_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / f"{form.short_name}.{form.file_extension}.dvc"

    if form.dataset_manual_import:
        manual_import_instructions = "--path-to-file **relative path of file**"
    else:
        manual_import_instructions = ""

    po.put_markdown(
        f"""
## Next steps

1. Verify that generated files are correct and update them if necessary

2. Run the snapshot step to upload files to S3
```bash
python snapshots/{form.namespace}/{form.snapshot_version}/{form.short_name}.py {manual_import_instructions}
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
