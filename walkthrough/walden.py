import shutil
import tempfile
from pathlib import Path

from botocore.exceptions import ClientError
from cookiecutter.main import cookiecutter
from owid import walden
from owid.catalog import s3_utils
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

from . import utils

CURRENT_DIR = Path(__file__).parent
WALDEN_INGEST_DIR = Path(walden.__file__).parent.parent.parent / "ingests"


class WaldenForm(BaseModel):

    short_name: str
    namespace: str
    name: str
    source_name: str
    publication_year: str
    publication_date: str
    url: str
    source_data_url: str
    file_extension: str
    license_name: str
    license_url: str
    description: str

    @property
    def version(self) -> str:
        return self.publication_year or self.publication_date


def app(run_checks: bool, dummy_data: bool) -> None:
    dummies = utils.DUMMY_DATA if dummy_data else {}

    with open(CURRENT_DIR / "walden.md", "r") as f:
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
                "Namespace",
                name="namespace",
                placeholder="ggdc",
                help_text="E.g. institution name",
                required=True,
                value=dummies.get("namespace"),
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
                value=dummies.get("publication_year"),
            ),
            pi.input(
                "Publication date",
                name="publication_date",
                placeholder="2020-10-01",
                help_text="Fill either publication year or publication date",
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
            pi.textarea(
                "Description", name="description", value=dummies.get("description")
            ),
        ],
    )
    form = WaldenForm(**data)

    # use multi-line description
    form.description = form.description.replace("\n", "\n  ")

    # cookiecutter on python files
    with tempfile.TemporaryDirectory() as temp_dir:
        OUTPUT_DIR = temp_dir

        # generate ingest scripts
        cookiecutter(
            (CURRENT_DIR / "walden_cookiecutter/").as_posix(),
            no_input=True,
            output_dir=temp_dir,
            overwrite_if_exists=True,
            extra_context=dict(directory_name="walden", **form.dict()),
        )

        # move them into the walden ingest directory
        shutil.copytree(
            Path(OUTPUT_DIR) / "walden", WALDEN_INGEST_DIR, dirs_exist_ok=True
        )

        ingest_path = WALDEN_INGEST_DIR / (form.short_name + ".py")
        meta_path = WALDEN_INGEST_DIR / (form.short_name + ".meta.yml")

        po.put_markdown(
            f"""
## Next steps

1. Verify that generated files are correct and update them if necessary

2. Test your ingest script with
```bash
python vendor/walden/ingests/{form.short_name}.py --skip-upload
```

3. Once you are happy with the ingest script, run it without the `--skip-upload` flag to upload files to S3. Running it again will overwrite the dataset.

4. Commit changes to walden (if you develop locally you don't need to commit it, but you need to upload the dataset to S3)

5. Exit the process and run next step with `poetry run walkthrough meadow`

## Getting dataset from Walden

If you have uploaded your dataset to Walden, you can get it from Walden catalog with the following snippet (to be used in meadow phase).

```python
from owid.walden import Catalog as WaldenCatalog

walden_ds = WaldenCatalog().find_one(namespace="{form.namespace}", short_name="{form.short_name}", version="{form.version}")
local_file = walden_ds.ensure_downloaded()
df = pd.read_csv(local_file)
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
    if "walden" not in bucket_names:
        po.put_error(po.put_markdown("`walden` bucket not found"))
        raise Exception()
