"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile
from pathlib import Path

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("baseline.zip")

    # Load country codes
    codes = paths.load_dataset("general_files")["country_codes"]

    # Unzip to temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(snap.path)
        data_files = [f for f in z.namelist() if "/png/" not in f and "/zip/" not in f]
        z.extractall(temp_dir, members=data_files)

        # Population table
        country_path = Path(temp_dir) / "baseline" / "txt" / "popc_c.txt"
        population = (
            pd.read_csv(country_path.as_posix(), sep=" ")
            .rename({"region": "country_code"}, axis=1)
            .melt(id_vars="country_code", var_name="year", value_name="population")
        )

    population = population[-population.country_code.isin(["Total"])]
    population["year"] = population.year.astype(int)
    population["country_code"] = population.country_code.astype(int)

    population_norm = pd.merge(codes, population, on="country_code", how="inner", validate="one_to_many").drop(
        columns="country_code"
    )
    population_norm.set_index(["country", "year"], inplace=True)

    tb = Table(population_norm, short_name="population")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()
