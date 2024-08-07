"""Script to create a snapshot of USGS Mineral Commodity Summaries for a specific year.

To create a snapshot for another year:
- Copy this file and the accompanying .zip.dvc file, and rename it for the corresponding year.
    - Rename also the year, which is in the snapshot path, below in this script.
- Go to the ScienceBase-Catalog and search for "Mineral Commodity Summaries", or simply go to this page:
https://www.sciencebase.gov/catalog/items?parentId=5c8c03e4e4b0938824529f7d&q=Mineral%20Commodity%20Summaries
- Click on the Mineral Commodity Summaries of the year you need. In the case of 2022, this will be the main page:
https://www.sciencebase.gov/catalog/item/6197ccbed34eb622f692ee1c
- On the top-right corner, click on the "View" button, and select "JSON" (optionally, tick on "Pretty print" on the top-left).
- Search (cmd+f) for "world.zip". There should be two results. The first one will be inside an element as follows:
    {
      "cuid": null,
      "key": null,
      ...
      "name": "world.zip",
      "title": "Mineral Commodity Summaries 2022 World Data Files",
      ...
      "url": "https://www.sciencebase.gov/catalog/file/get/6197ccbed34eb622f692ee1c?f=__disk__ef%2Fa8%2F27%2Fefa827f9cad2012de291a711b2a073b0a7cd4aa5",
      "downloadUri": "https://www.sciencebase.gov/catalog/file/get/6197ccbed34eb622f692ee1c?f=__disk__ef%2Fa8%2F27%2Fefa827f9cad2012de291a711b2a073b0a7cd4aa5"
    }
- Copy the "downloadUri". This is the URL that you should use in the Snapshot url_download field.
- Update all metadata fields in the new .zip.dvc file:
    - Replace all instances of the year with the new year.
    - Update the date_published with the new date (which can be found at the beginning of the main page for that new year, next to "Publication Date").
    - Update the date_accessed.
    - Replace the citation_full with the text suggested in "Citation" at the beginning of the main page.
    - Replace the url_main with the doi that appears in the citation.
    - Replace the url_download with the "downloadUri" copied before.
- Remove the "outs" part of the snapshot .zip.dvc file.
- Execute the new snapshot.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"usgs/{SNAPSHOT_VERSION}/mineral_commodity_summaries_2023.zip")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
