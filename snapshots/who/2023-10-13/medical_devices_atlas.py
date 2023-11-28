"""
Script to create a snapshot of dataset 'Global atlas of medical devices, World Health Organisation.'.
Data is available to download:  https://www.who.int/teams/health-product-policy-and-standards/assistive-and-medical-technology/medical-devices/global-atlas-of-medical-devices
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option(
    "--path-to-file-mri",
    prompt=True,
    type=str,
    help="Path to local data file with Magnetic Resonance Imaging Units data.",
)
@click.option(
    "--path-to-file-pet", prompt=True, type=str, help="Path to local data file with Positron Emission Tomography data."
)
@click.option(
    "--path-to-file-ct", prompt=True, type=str, help="Path to local data file with Computed Tomography Units data."
)
@click.option(
    "--path-to-file-gc-nm",
    prompt=True,
    type=str,
    help="Path to local data file with Gamma Camera or Nuclear Medicine data.",
)
def main(
    path_to_file_mri: str, path_to_file_pet: str, path_to_file_ct: str, path_to_file_gc_nm: str, upload: bool
) -> None:
    snapshot_paths = [
        f"who/{SNAPSHOT_VERSION}/medical_devices_atlas_mri.csv",
        f"who/{SNAPSHOT_VERSION}/medical_devices_atlas_pet.csv",
        f"who/{SNAPSHOT_VERSION}/medical_devices_atlas_ct.csv",
        f"who/{SNAPSHOT_VERSION}/medical_devices_atlas_gc_nm.csv",
    ]

    path_to_files = [
        path_to_file_mri,
        path_to_file_pet,
        path_to_file_ct,
        path_to_file_gc_nm,
    ]
    for meta_path, file_path in zip(snapshot_paths, path_to_files):
        snap = Snapshot(meta_path)
        # Ensure destination folder exists.
        snap.path.parent.mkdir(exist_ok=True, parents=True)
        # Copy local data file to snapshots data folder.
        snap.path.write_bytes(Path(file_path).read_bytes())
        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
