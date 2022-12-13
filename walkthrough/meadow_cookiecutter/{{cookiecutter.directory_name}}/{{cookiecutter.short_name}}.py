import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
{% if cookiecutter.load_population == "True" %}
from etl.paths import DATA_DIR
{% endif -%}
{% if cookiecutter.load_countries_regions == "True" %}
from etl.paths import REFERENCE_DATASET
{% endif -%}
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = Names(__file__)

{% if cookiecutter.load_countries_regions == "True" %}
def load_countries_regions() -> Table:
    # load countries regions (e.g. to map from iso codes to country names)
    reference_dataset = Dataset(REFERENCE_DATASET)
    return reference_dataset["countries_regions"]
{% endif -%}
{% if cookiecutter.load_population == "True" %}
def load_population() -> Table:
    # load countries regions (e.g. to map from iso codes to country names)
    indicators = Dataset(DATA_DIR / "garden/owid/latest/key_indicators")
    return indicators["population"]
{% endif -%}

def run(dest_dir: str) -> None:
    log.info("{{cookiecutter.short_name}}.start")

    # retrieve snapshot
    snap = Snapshot("{{cookiecutter.namespace}}/{{cookiecutter.snapshot_version}}/{{cookiecutter.short_name}}.{{cookiecutter.snapshot_file_extension}}")
    df = pd.read_excel(snap.path, sheet_name="Full data")

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "{{cookiecutter.version}}"

    # # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)
    {% if cookiecutter.include_metadata_yaml == "True" %}
    # update metadata
    ds.update_metadata(N.metadata_path)
    {% endif %}
    # finally save the dataset
    ds.save()

    log.info("{{cookiecutter.short_name}}.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "country": "country",
            "year": "year",
            "pop": "population",
            "gdppc": "gdp",
        }
    ).drop(columns=["countrycode"])
