import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from botocore.exceptions import ClientError
from owid.catalog import s3_utils
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

from . import utils

CURRENT_DIR = Path(__file__).parent


class Options(Enum):

    IS_PRIVATE = "Make dataset private"


class SnapshotForm(BaseModel):

    namespace: str
    snapshot_version: str
    short_name: str
    name: str
    source_name: str
    publication_year: Optional[str]
    publication_date: Optional[str]
    url: str
    source_data_url: str
    file_extension: str
    license_name: str
    license_url: str
    description: str
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)

    @property
    def version(self) -> str:
        return self.snapshot_version or self.publication_year or self.publication_date  # type: ignore


def app(run_checks: bool, dummy_data: bool) -> None:
    dummies = utils.DUMMY_DATA if dummy_data else {}

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
                placeholder="ggdc",
                help_text="E.g. institution name",
                required=True,
                value=dummies.get("namespace"),
            ),
            pi.input(
                "Version",
                name="snapshot_version",
                placeholder=str(dt.date.today()),
                help_text="E.g. current date, publication date or year is used if not given",
                required=False,
                value=dummies.get("snapshot_version", str(dt.date.today())),
            ),
            pi.input(
                "Short name",
                name="short_name",
                placeholder="ggdc_maddison",
                required=True,
                value=dummies.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored short name",
            ),
            pi.input(
                "Full name",
                name="name",
                placeholder="Maddison Project Database (GGDC, 2020)",
                required=True,
                value=dummies.get("name"),
            ),
            pi.input(
                "Source name",
                name="source_name",
                placeholder="Maddison Project Database 2020 (Bolt and van Zanden, 2020)",
                required=True,
                value=dummies.get("source_name"),
            ),
            pi.input(
                "Publication year",
                name="publication_year",
                type=pi.NUMBER,
                placeholder="2020",
                help_text="Fill either publication year or publication date",
            ),
            pi.input(
                "Publication date",
                name="publication_date",
                placeholder="2020-10-01",
                help_text="Fill either publication year or publication date",
                value=dummies.get("publication_date"),
            ),
            pi.input(
                "Dataset webpage URL",
                name="url",
                placeholder="https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020",
                help_text="Url to the main page of the project",
                required=True,
                value=dummies.get("url"),
            ),
            pi.input(
                "Dataset download URL",
                name="source_data_url",
                placeholder="https://www.rug.nl/ggdc/historicaldevelopment/maddison/data/mpd2020.xlsx",
                value=dummies.get("source_data_url"),
            ),
            pi.input(
                "File extension",
                name="file_extension",
                placeholder="xlsx",
                value=dummies.get("file_extension"),
            ),
            pi.input(
                "License URL",
                name="license_url",
                placeholder="https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020",
            ),
            pi.input(
                "License name",
                name="license_name",
                placeholder="Creative Commons BY 4.0",
            ),
            pi.textarea("Description", name="description", value=dummies.get("description")),
            pi.checkbox(
                "Other options",
                options=[
                    Options.IS_PRIVATE.value,
                ],
                name="options",
            ),
        ],
    )
    form = SnapshotForm(**data)

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

2. Test your ingest script with
```bash
python snapshots/{form.namespace}/{form.version}/{form.short_name}.py --skip-upload
```

3. Once you are happy with the ingest script, run it without the `--skip-upload` flag to upload files to S3. Running it again will overwrite the dataset.

4. Exit the process and run next step with `poetry run walkthrough meadow`

## Loading snapshots

```python
from etl.snapshot import Snapshot

snap = Snapshot("{form.namespace}/{form.version}/{form.short_name}.{form.file_extension}")
# call snap.pull() if you don't have it locally in snapshots/
df = pd.read_excel(snap.path)
```

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
