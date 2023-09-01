"""Script to create a snapshot made of data manually extracted from different websites."""

from pathlib import Path

import click
import numpy as np

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/chick_culling_laws_or_lack_thereof.csv")

    # Create a table of data manually extracted from different websites.
    columns = ["country", "status", "year", "comments", "evidence", "url"]
    data = [
        (
            "Australia",
            "No laws",
            np.nan,
            "",
            "Royal Society for the Prevention of Cruelty to Animals (RSPCA) Australia (2021-09-22).",
            "https://kb.rspca.org.au/knowledge-base/what-happens-with-male-chicks-in-the-egg-industry/",
        ),
        (
            "Canada",
            "No laws",
            np.nan,
            "",
            "Canadian Poultry Magazine (2016-12-19)",
            "https://www.canadianpoultrymag.com/hypereye-a-game-changer-30033/",
        ),
        (
            "Luxembourg",
            "Banned by law",
            2018,
            "",
            "Enforcement date: 2018-06-18. Animal protection law on 6 June 2018, The Luxembourg Government.",
            "https://gouvernement.lu/en/dossiers/2018/tierschutz.html",
        ),
        (
            "New Zealand",
            "No laws",
            np.nan,
            "",
            "Save Animals From Exploitation (SAFE) New Zealand (2023).",
            "https://safe.org.nz/our-work/animals-in-aotearoa/male-chicks/",
        ),
        (
            "Switzerland",
            "Partially banned by law",
            np.nan,
            "Grinding chicks was banned in 2019, but gassing is still legal.",
            "Swissinfo (2019-09-20).",
            "https://www.swissinfo.ch/eng/society/animal-protection_-switzerland-bans-shredding-of-male-chicks-/45240798",
        ),
        (
            "United Kingdom",
            "No laws",
            np.nan,
            "",
            "The Humane League (2021-07-29).",
            "https://thehumaneleague.org.uk/article/what-happens-to-male-chicks-in-the-egg-industry",
        ),
        (
            "United States",
            "No laws",
            np.nan,
            "",
            "Vox (2021-04-12).",
            "https://www.vox.com/future-perfect/22374193/eggs-chickens-animal-welfare-culling",
        ),
    ]
    # Countries in the European Union for which there is no law against chick culling.
    rest_of_eu = [
        "Belgium",
        "Bulgaria",
        "Croatia",
        "Cyprus",
        "Czechia",
        "Denmark",
        "Estonia",
        "Finland",
        "Greece",
        "Hungary",
        "Ireland",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Malta",
        "Netherlands",
        "Poland",
        "Portugal",
        "Romania",
        "Slovakia",
        "Slovenia",
        "Spain",
        "Sweden",
    ]
    for country in rest_of_eu:
        data += (
            (
                country,
                "No laws",
                np.nan,
                "",
                "European Parliamentary Research Service (2022-12).",
                "https://www.europarl.europa.eu/RegData/etudes/ATAG/2022/739246/EPRS_ATA(2022)739246_EN.pdf",
            ),
        )
    tb = snap.read_from_records(data=data, columns=columns)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)


if __name__ == "__main__":
    main()
