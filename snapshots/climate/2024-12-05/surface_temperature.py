"""Script to create a snapshot of the monthly averaged surface temperature data from 1950 to present from the Copernicus Climate Change Service.

The script assumes that the data is available on the CDS API.
Instructions on how to access the API on a Mac are here: https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+macOS

More information on how to access the data is here: hhttps://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview

The data is downloaded as a NetCDF file. Tutorials for using the Copernicus API are here and work with the NETCDF format are here: https://ecmwf-projects.github.io/copernicus-training-c3s/cds-tutorial.html
"""

import tempfile
from pathlib import Path

# CDS API
import cdsapi
import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/surface_temperature.zip")

    # Save data as a compressed temporary file.
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "era5_monthly_t2m_eur.nc"

        client = cdsapi.Client()

        dataset = "reanalysis-era5-single-levels-monthly-means"
        request = {
            "product_type": ["monthly_averaged_reanalysis"],
            "variable": ["2m_temperature"],
            "year": [str(year) for year in range(1940, 2025)],
            "month": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
            "time": "00:00",
            "area": [90, -180, -90, 180],
            "data_format": "grib",
            "download_format": "zip",
        }

        client.retrieve(dataset, request, output_file)

        # Upload snapshot.
        snap.create_snapshot(filename=output_file, upload=upload)


if __name__ == "__main__":
    main()
