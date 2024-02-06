"""The file is found in the data site from OECD:

https://data-explorer.oecd.org/vis?fs[0]=Topic%2C1%7CRegions%2C%20cities%20and%20local%20areas%23GEO%23%7CCities%20and%20functional%20urban%20areas%23GEO_URB%23&pg=0&fc=Topic&bp=true&snb=17&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_FUA_TERR%40DF_DENSITY&df[ag]=OECD.CFE.EDS&df[vs]=1.0&pd=%2C&dq=.A.POP_DEN..&ly[rw]=REF_AREA%2CTERRITORIAL_LEVEL&ly[cl]=TIME_PERIOD&to[TIME_PERIOD]=false
In order to find the direct link to download the CSV --> Click on download -> Full indicator data (.csv) - Unfiltered data in tabular text. This is the link used in url_download in the yaml file."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/population_density_cities_fuas.csv")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
