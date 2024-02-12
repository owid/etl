"""Script to create a snapshot of the monthly averaged surface land temperature data from 1950 to present from the Copernicus Climate Change Service.

   The script assumes that the data is available on the CDS API.
   Instructions on how to access the API on a Mac are here: https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+macOS

   More information on how to access the data is here: https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels-monthly-means?tab=overview

   The data is downloaded as a NetCDF file. Tutorials for using the Copernicus API are here and work with the NETCDF format are here: https://ecmwf-projects.github.io/copernicus-training-c3s/cds-tutorial.html
   """

import gzip
import shutil
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
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/surface_land_temperature.gz")
    # Save data as a compressed temporary file.
    with tempfile.TemporaryDirectory() as temp_dir:
        c = cdsapi.Client()
        output_file = Path(temp_dir) / "era5_monthly_t2m_eur.nc"

        c.retrieve(
            "reanalysis-era5-single-levels-monthly-means",
            {
                "product_type": "monthly_averaged_reanalysis",
                "variable": "2m_temperature",
                "year": [str(year) for year in range(1950, 2024)],
                "month": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
                "time": "00:00",
                "area": [90, -180, -90, 180],
                "format": "netcdf",
            },
            output_file,
        )
        # Compress the file
        with open(output_file, "rb") as f_in:
            with gzip.open(str(output_file) + ".gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        gzip_file = str(output_file) + ".gz"
        # Upload snapshot.
        snap.create_snapshot(filename=gzip_file, upload=upload)


if __name__ == "__main__":
    main()
