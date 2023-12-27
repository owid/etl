"""Script to create a snapshot of the monthly averaged surface temperature data from 1950 to present from the Copernicus Climate Change Service.

   The script assumes that the data is available on the CDS API.
   Instructions on how to access the API on a Mac are here: https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+macOS

   More information on how to access the data is here: https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels-monthly-means?tab=overview

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
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/surface_temperature.nc")
    # Save data as a compressed temporary file.
    with tempfile.TemporaryDirectory() as temp_dir:
        c = cdsapi.Client()
        c.retrieve(
            "reanalysis-era5-single-levels-monthly-means",
            {
                "product_type": "monthly_averaged_reanalysis",
                "variable": "2m_temperature",
                "year": [
                    "1950",
                    "1951",
                    "1952",
                    "1953",
                    "1954",
                    "1955",
                    "1956",
                    "1957",
                    "1958",
                    "1959",
                    "1960",
                    "1961",
                    "1962",
                    "1963",
                    "1964",
                    "1965",
                    "1966",
                    "1967",
                    "1968",
                    "1969",
                    "1970",
                    "1971",
                    "1972",
                    "1973",
                    "1974",
                    "1975",
                    "1976",
                    "1977",
                    "1978",
                    "1979",
                    "1980",
                    "1981",
                    "1982",
                    "1983",
                    "1984",
                    "1985",
                    "1986",
                    "1987",
                    "1988",
                    "1989",
                    "1990",
                    "1991",
                    "1992",
                    "1993",
                    "1994",
                    "1995",
                    "1996",
                    "1997",
                    "1998",
                    "1999",
                    "2000",
                    "2001",
                    "2002",
                    "2003",
                    "2004",
                    "2005",
                    "2006",
                    "2007",
                    "2008",
                    "2009",
                    "2010",
                    "2011",
                    "2012",
                    "2013",
                    "2014",
                    "2015",
                    "2016",
                    "2017",
                    "2018",
                    "2019",
                    "2020",
                    "2021",
                    "2022",
                    "2023",
                ],
                "month": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
                "time": "00:00",
                "area": [
                    90,
                    -180,
                    -90,
                    180,
                ],
                "format": "netcdf",
            },
            Path(temp_dir) / "era5_monthly_t2m_eur.nc",
        )

        output_file = Path(temp_dir) / "era5_monthly_t2m_eur.nc"

        # Add file to DVC and upload to S3.
        snap.create_snapshot(filename=output_file, upload=upload)


if __name__ == "__main__":
    main()
