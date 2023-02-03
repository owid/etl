"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
{% if cookiecutter.load_countries_regions == "True" %}


def load_countries_regions() -> Table:
    # Load countries-regions table from reference dataset (e.g. to map from iso codes to country names).
    ds_reference = paths.load_dependency("reference")
    tb_countries_regions = ds_reference["countries_regions"]

    return tb_countries_regions
{% endif -%}
{% if cookiecutter.load_population == "True" %}


def load_population() -> Table:
    # Load population table from key_indicators dataset.
    ds_indicators = paths.load_dependency("key_indicators")
    tb_population = ds_indicators["population"]

    return tb_population
{% endif -%}


def run(dest_dir: str) -> None:
    log.info("{{cookiecutter.short_name}}.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("{{cookiecutter.short_name}}.{{cookiecutter.snapshot_file_extension}}")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("{{cookiecutter.short_name}}.end")
