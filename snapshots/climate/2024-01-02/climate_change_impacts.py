"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of data files.
FILES = [
    # NASA Goddard Institute for Space Studies - GISS Surface Temperature Analysis.
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
    "ocean_heat_content_monthly_world_700m.csv",
    "ocean_heat_content_monthly_world_2000m.csv",
    "ocean_heat_content_annual_world_700m.csv",
    "ocean_heat_content_annual_world_2000m.csv",
    # School of Ocean and Earth Science and Technology - Hawaii Ocean Time-series.
    "hawaii_ocean_time_series.csv",
    # Rutgers University Global Snow Lab - Snow Cover Extent.
    "snow_cover_extent_north_america.csv",
    "snow_cover_extent_northern_hemisphere.csv",
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
########################################################################################################################


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot for each of the data files.
    for file_name in FILES:
        snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/{file_name}")

        # To ease the recurrent task update, fetch the access date from the version, and write it to the dvc files.
        # For simplicity, also assume that the publication date is the same as the access date.
        snap.metadata.origin.date_accessed = SNAPSHOT_VERSION  # type: ignore
        snap.metadata.origin.date_published = SNAPSHOT_VERSION  # type: ignore
        # NOTE: Date published can in principle be extracted for some of the sources:
        # * For sea_ice_index, the date_published can be found on:
        #   https://noaadata.apps.nsidc.org/NOAA/G02135/seaice_analysis/
        #   Next to the file name (Sea_Ice_Index_Monthly_Data_by_Year_G02135_v3.0.xlsx).
        # * For sea_surface_temperature_* the date_published can be found on:
        #   https://www.metoffice.gov.uk/hadobs/hadsst4/data/download.html
        #   At the very bottom of the page, where it says "Last updated:".
        # * For the Hawaii Ocean Time-Series, the date_published can be found written on the header of the data itself:
        #   https://hahana.soest.hawaii.edu/hot/hotco2/HOT_surface_CO2.txt

        # Extract publication year from date_published.
        year_published = snap.metadata.origin.date_published.split("-")[0]  # type: ignore

        snap.metadata.origin.attribution = (  # type: ignore
            f"{snap.metadata.origin.producer} - {snap.metadata.origin.title} ({year_published})"  # type: ignore
        )

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
