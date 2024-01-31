"""Script to create a snapshot for each of the climate change datasets that have regular updates.

The publication date will be automatically extracted from the source website, if possible, and otherwise it will be
assumed to be the same as the access date. These dates will be written to the metadata dvc files.

"""

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import requests
from bs4 import BeautifulSoup
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of data files.
FILES = [
    # NASA Goddard Institute for Space Studies - GISS Surface Temperature Analysis.
    # NOTE: Publication date cannot be automatically extracted.
    "surface_temperature_analysis_world.csv",
    "surface_temperature_analysis_northern_hemisphere.csv",
    "surface_temperature_analysis_southern_hemisphere.csv",
    # National Snow and Ice Data Center - Sea Ice Index.
    "sea_ice_index.xlsx",
    # Met Office Hadley Centre - HadSST.
    "sea_surface_temperature_world.csv",
    "sea_surface_temperature_northern_hemisphere.csv",
    "sea_surface_temperature_southern_hemisphere.csv",
    # NOAA National Centers for Environmental Information - Ocean Heat Content.
    # NOTE: Publication date cannot be automatically extracted.
    "ocean_heat_content_monthly_world_700m.csv",
    "ocean_heat_content_monthly_world_2000m.csv",
    "ocean_heat_content_annual_world_700m.csv",
    "ocean_heat_content_annual_world_2000m.csv",
    # School of Ocean and Earth Science and Technology - Hawaii Ocean Time-series.
    "hawaii_ocean_time_series.csv",
    # Rutgers University Global Snow Lab - Snow Cover Extent.
    # NOTE: Publication date cannot be automatically extracted. But they seem to have regular updates (even daily).
    "snow_cover_extent_north_america.csv",
    "snow_cover_extent_northern_hemisphere.csv",
    # NOAA Global Monitoring Laboratory.
    "co2_concentration_monthly.csv",
    "ch4_concentration_monthly.csv",
    "n2o_concentration_monthly.csv",
]

########################################################################################################################
# Other possible datasets to include:
# * Ocean heat content data from MRI/JMA. We have this data as part of the EPA ocean heat content compilation.
#   But in the following link, they claim the data is updated every year, so it could be added to our yearly data.
#   https://www.data.jma.go.jp/gmd/kaiyou/english/ohc/ohc_global_en.html
# * Rutgers University Global Snow Lab also includes snow cover extent for:
#   * Eurasia: https://climate.rutgers.edu/snowcover/files/moncov.eurasia.txt
#   * North America (excluding Greenland): https://climate.rutgers.edu/snowcover/files/moncov.nam.txt
# * Ice sheet mass balance from NASA EarthData. This is regularly updated, but to access it one has to manually log in.
#   The data can be manually accessed from:
#   https://climate.nasa.gov/vital-signs/ice-sheets/
#   By clicking on the HTTP link. This leads to a manual log in page.
#   Once logged in, the data is accessible via the following link:
#   https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ANTARCTICA_MASS_TELLUS_MASCON_CRI_TIME_SERIES_RL06.1_V3/antarctica_mass_200204_202310.txt
#   So, one could use this link, trying with different dates (e.g. ..._202401.txt, ..._202312.txt, ..._202311.txt),
#   until the most recent file is downloaded.
#   I contacted EarthData to ask if there is any way to access the latest data programmatically.
# * Global sea level from NASA.
#   We could get more up-to-date data on sea levels from https://sealevel.jpl.nasa.gov/
#   but we would need to use a special library with credentials to fetch the data (and the baseline and format would
#   probably be different).
########################################################################################################################


def find_date_published(snap: Snapshot) -> Optional[str]:
    # Extract publication date for each individual origin, if possible.
    # Otherwise, assign the current access date as publication date.
    if snap.path.name == "sea_ice_index.xlsx":
        # * For sea_ice_index, the date_published can be found on:
        #   https://noaadata.apps.nsidc.org/NOAA/G02135/seaice_analysis/
        #   Next to the file name (Sea_Ice_Index_Monthly_Data_by_Year_G02135_v3.0.xlsx).

        # Extract all the text in the web page.
        url = "/".join(snap.metadata.origin.url_download.split("/")[:-1])  # type: ignore
        response = requests.get(url)
        # Parse HTML content.
        soup = BeautifulSoup(response.text, "html.parser")

        # Fetch the date that is written next to the title.
        for line in soup.text.split("\n"):
            if "Sea_Ice_Index_Monthly_Data_by_Year" in line:
                dates = re.findall(r"\d{2}-\w{3}-\d{4}", line)
                if len(dates) == 1:
                    # Format date conveniently.
                    date = datetime.strptime(dates[0], "%d-%b-%Y").strftime("%Y-%m-%d")
                    return date
                else:
                    log.warn(f"Failed to extract date_published for: {snap.path.name}")

    elif snap.path.name.startswith("sea_surface_temperature_"):
        # * For sea_surface_temperature_* the date_published can be found on:
        #   https://www.metoffice.gov.uk/hadobs/hadsst4/data/download.html

        # Extract all the text in the web page.
        url = snap.metadata.origin.url_download.split("/data/")[0] + "/data/download.html"  # type: ignore
        response = requests.get(url)
        # Parse HTML content.
        soup = BeautifulSoup(response.text, "html.parser")

        for line in soup.text.split("\n"):
            # At the bottom of the page, there is a line like "Last updated: 09/01/2024 Expires: 09/01/2025".
            if "Last updated" in line:
                dates = re.findall(r"\d{2}/\d{2}/\d{4}", line)
                if len(dates) == 2:
                    # Format date conveniently.
                    date = datetime.strptime(dates[0], "%d/%m/%Y").strftime("%Y-%m-%d")
                    return date
                else:
                    log.warn(f"Failed to extract date_published for: {snap.path.name}")

    elif snap.path.name == "hawaii_ocean_time_series.csv":
        # * For the Hawaii Ocean Time-Series, the date_published can be found written on the header of the data itself:
        #   https://hahana.soest.hawaii.edu/hot/hotco2/HOT_surface_CO2.txt

        # Extract text from data file.
        url = snap.metadata.origin.url_download  # type: ignore
        response = requests.get(url)
        for line in response.text.split("\n"):
            # At the top of the file, there is a line like "Last updated 11 December 2023 by J.E. Dore".
            if "Last updated" in line:
                # Regular expression to extract the date
                dates = re.findall(r"\d{1,2}\s+\w+\s+\d{4}", line)
                if len(dates) == 1:
                    # Format date conveniently.
                    date = datetime.strptime(dates[0], "%d %B %Y").strftime("%Y-%m-%d")
                    return date
                else:
                    log.warn(f"Failed to extract date_published for: {snap.path.name}")

    elif "_concentration" in snap.path.name:
        # * For NOAA GML concentration data, the date_published can be found in the header of each data file.
        # The date is in a line like "# File Creation: Fri Jan  5 03:55:24 2024".

        # Extract text from data file.
        url = snap.metadata.origin.url_download  # type: ignore
        response = requests.get(url)
        for line in response.text.split("\n"):
            # At the top of the file, there is a line like "Last updated 11 December 2023 by J.E. Dore".
            if "File Creation" in line:
                # Regular expression to extract the date
                dates = re.findall(r"\w{3}\s\w{3}\s+\d{1,2}\s\d{2}:\d{2}:\d{2}\s\d{4}", line)
                if len(dates) == 1:
                    # Format date conveniently.
                    date = datetime.strptime(dates[0], "%a %b %d %H:%M:%S %Y").strftime("%Y-%m-%d")
                    return date
                else:
                    log.warn(f"Failed to extract date_published for: {snap.path.name}")

    # In all other cases, assume date_published is the same as date_accessed.
    return snap.metadata.origin.date_accessed  # type: ignore


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot for each of the data files.
    for file_name in FILES:
        snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/{file_name}")

        # To ease the recurrent task update, fetch the access date from the version, and write it to the dvc files.
        snap.metadata.origin.date_accessed = SNAPSHOT_VERSION  # type: ignore

        # Extract publication date, if possible, and otherwise assume it is the same as the access date.
        snap.metadata.origin.date_published = find_date_published(snap=snap)  # type: ignore

        # Extract publication year from date_published (which will be used in the custom attribution).
        year_published = snap.metadata.origin.date_published.split("-")[0]  # type: ignore

        # Assign a custom attribution.
        snap.metadata.origin.attribution = (  # type: ignore
            f"{snap.metadata.origin.producer} - {snap.metadata.origin.title} ({year_published})"  # type: ignore
        )

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
