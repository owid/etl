"""Script to create a snapshot of dataset 'Consumption of controlled substances (UNEP, 2023)'.

The data for this Snapshot needs to be downloaded and imported from a local file. Steps:

    1. Go to https://ozone.unep.org/countries/data-table?q=countries/data
    2. Download each of the available datasets. Each table has an "X" (Excel-like) sign, click it to download the corresponding XLSX file.
    3. Name them with appropriate names (chemical name, using snake-case). Accepted names are listed in variable `CHEMICAL_NAMES`, which should
       also match those stated in the DVC files ("consumption_controlled_substances.[checmical-name].xlsx.dvc").
    4. Place all XLSX files into one single folder.
    5. Run the script as: `python snapshots/unep/2023-03-17/consumption_controlled_substances.py --path-to-folder /path/to/folder/with/xlsx/files`

"""

from pathlib import Path
from typing import Union

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
CHEMICAL_NAMES = {
    "bromochloromethane",
    "carbon_tetrachloride",
    "chlorofluorocarbons",
    "halons",
    "hydrobromofluorocarbons",
    "hydrochlorofluorocarbons",
    "hydrofluorocarbons",
    "methyl_bromide",
    "methyl_chloroform",
    "other_fully_halogenated",
}


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-folder", prompt=True, type=str, help="Path to local folder containing all files.")
def main(path_to_folder: Union[Path, str], upload: bool) -> None:
    # Check correctness of files in local folder
    # There should be a file for each chemical name listed in `CHEMICAL_NAMES`.
    path_to_folder = Path(path_to_folder)
    files = [f for f in path_to_folder.iterdir() if f.suffix == ".xlsx"]
    names = set(f.with_suffix("").name for f in files)
    assert names == CHEMICAL_NAMES, (
        "There is a missmatch between expected chemical names and provided file names! Check"
        " them and try again. You may find the guide at the top of this script useful for that."
    )
    for name in names:
        # Create a new snapshot. Raise an error if DVC file for a given chemical is not found.
        try:
            snap = Snapshot(f"unep/{SNAPSHOT_VERSION}/consumption_controlled_substances.{name}.xlsx")
        except FileNotFoundError as e:
            raise ValueError(f"Could not find DVC file for chemical '{name}'.") from e
        # Ensure destination folder exists.
        snap.path.parent.mkdir(exist_ok=True, parents=True)
        # Copy local data file to snapshots data folder.
        path_to_file = path_to_folder / f"{name}.xlsx"
        snap.path.write_bytes(path_to_file.read_bytes())
        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
