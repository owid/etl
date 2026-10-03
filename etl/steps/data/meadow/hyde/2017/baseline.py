"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile
from pathlib import Path

import owid.catalog.processing as pr

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
            pr.read_csv(country_path, sep=" ", metadata=snap.to_table_metadata(), origin=snap.m.origin)
            .rename({"region": "country_code"}, axis=1)
            .melt(id_vars="country_code", var_name="year", value_name="population")
        )

    population = population[-population.country_code.isin(["Total"])]
    population["year"] = population.year.astype(int)
    population["country_code"] = population.country_code.astype(int)

    population_norm = pr.merge(
        codes.reset_index(), population, on="country_code", how="inner", validate="one_to_many"
    ).drop(columns="country_code")

    population_norm.set_index(["country", "year"], inplace=True)
    population_norm.metadata.short_name = "population"

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds = create_dataset(
        dest_dir, tables=[population_norm], default_metadata=snap.metadata, check_variables_metadata=True
    )
    ds.save()
