import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR, REFERENCE_DATASET, SNAPSHOTS_DIR
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
{% endif %}
{% if cookiecutter.load_population == "True" %}
def load_population() -> Table:
    # load countries regions (e.g. to map from iso codes to country names)
    indicators = Dataset(DATA_DIR / "garden/owid/latest/key_indicators")
    return indicators["population"]
{% endif %}
def run(dest_dir: str) -> None:
    log.info("{{cookiecutter.short_name}}.start")

    # retrieve snapshot
    snap = Snapshot(SNAPSHOTS_DIR / "{{cookiecutter.namespace}}" / "{{cookiecutter.snapshot_version}}" / "{{cookiecutter.short_name}}.{{cookiecutter.snapshot_file_extension}}")
    df = pd.read_excel(snap.path, sheet_name="Full data")

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = "{{cookiecutter.version}}"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)
    {% if cookiecutter.include_metadata_yaml == "True" %}
    ds.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(N.metadata_path, "{{cookiecutter.short_name}}")
    {% endif %}
    # add table to a dataset
    ds.add(tb)

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
