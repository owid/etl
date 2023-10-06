"""Script to create a snapshot of dataset."""

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
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/chick_culling_laws.csv")

    # Create a table of data manually extracted from different websites.
    columns = ["country", "status", "year_effective", "comments", "evidence", "url", "annotation"]
    data = [
        # Add countries where chick culling is fully or partially banned (or planned to be so).
        (
            "Austria",
            "Banned",
            2023,
            "Date effective: 2022-07-18. The prohibition was adopted in July 2022 through a law amending the Animal Welfare Act. Scope excludes male chicks used as feed in zoos or for birds of prey. Destruction of non-hatched is allowed up until 14 day of incubation.",
            "Section 6(2), Animal Welfare Act.",
            "https://www.ris.bka.gv.at/GeltendeFassung.wxe?Abfrage=Bundesnormen&Gesetzesnummer=20003541",
            "Excludes male chicks used as feed in zoos or for birds of prey.",
        ),
        (
            "France",
            "Banned",
            2023,
            "Date effective: 2022-12-31. The prohibition was adopted on January 2022 through a regulation. In ovo sexing technologies benefit from a five-year nonobsolescence clause. Male chicks for animal food production benefit from an exemption. Destruction of non-hatched is allowed up until 15 day of incubation.",
            "R 214-17 of the Rural Code.",
            "https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000028969470",
            "Male chicks for animal food production benefit from an exemption.",
        ),
        (
            "Germany",
            "Banned",
            2022,
            "Date effective: 2022-01-01. The prohibition was adopted on January 2022, through a regulation which prohibits the culling of one-day old chicks by 2022, and the culling of fertilized eggs passed the 6th day of incubation. Note: No derogation.",
            "Section 3 (4c), Animal Welfare Act.",
            "https://www.gesetze-im-internet.de/tierschg/BJNR012770972.html",
            "",
        ),
        (
            "Italy",
            "Banned but not yet in effect",
            2027,
            "Date effective: 2026-12-31. The law prohibits the selective killing of male chicks by December 31st, 2026 and provides exemptions for animal protection purposes only. A decree will later specify the ways in which the law should be implemented. The law does not provide a rule regarding the destruction of non-hatched eggs nor exemptions, other than exemptions for animal health and protection purposes. A decree will likely specify these two aspects.",
            "Article 18, European Delegation Law (22G00136).",
            "https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:legge:2022-08-04;127",
            "Provides exemptions for animal protection purposes only.",
        ),
        (
            "Luxembourg",
            "Banned",
            2018,
            "",
            "Date effective: 2018-06-18. Animal protection law on 6 June 2018, The Luxembourg Government.",
            "https://gouvernement.lu/en/dossiers/2018/tierschutz.html",
            "",
        ),
        (
            "Switzerland",
            "Partially banned",
            2019,
            "Grinding chicks was banned in 2019, but gassing is still legal.",
            "Swissinfo (2019-09-20).",
            "https://www.swissinfo.ch/eng/society/animal-protection_-switzerland-bans-shredding-of-male-chicks-/45240798",
            "Grinding is banned, but gassing is still legal.",
        ),
        # Add countries for which there is evidence of chick culling with no ban.
        (
            "Australia",
            "Not banned",
            np.nan,
            "",
            "Royal Society for the Prevention of Cruelty to Animals (RSPCA) Australia (2021-09-22).",
            "https://kb.rspca.org.au/knowledge-base/what-happens-with-male-chicks-in-the-egg-industry/",
            "",
        ),
        (
            "Canada",
            "Not banned",
            np.nan,
            "",
            "Canadian Poultry Magazine (2016-12-19).",
            "https://www.canadianpoultrymag.com/hypereye-a-game-changer-30033/",
            "",
        ),
        (
            "New Zealand",
            "Not banned",
            np.nan,
            "",
            "Save Animals From Exploitation (SAFE) New Zealand (2023).",
            "https://safe.org.nz/our-work/animals-in-aotearoa/male-chicks/",
            "",
        ),
        (
            "United Kingdom",
            "Not banned",
            np.nan,
            "",
            "The Humane League (2021-07-29).",
            "https://thehumaneleague.org.uk/article/what-happens-to-male-chicks-in-the-egg-industry",
            "",
        ),
        (
            "United States",
            "Not banned",
            np.nan,
            "",
            "Vox (2021-04-12).",
            "https://www.vox.com/future-perfect/22374193/eggs-chickens-animal-welfare-culling",
            "",
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
                "Not banned",
                np.nan,
                "",
                "European Parliamentary Research Service (2022-12).",
                "https://www.europarl.europa.eu/RegData/etudes/ATAG/2022/739246/EPRS_ATA(2022)739246_EN.pdf",
                "",
            ),
        )
    tb = snap.read_from_records(data=data, columns=columns)

    # Add all individual sources to the full citation in the metadata.
    sources_text = """Evidence of laws banning chick culling, and evidence of chick culling being practiced without any ban, has been gathered from various sources for different countries.\n Some of those sources were extracted from [a report by the European Institute for Animal Law & Policy](https://animallaweurope.com/wp-content/uploads/2023/01/Animal-Law-Europe-%E2%80%93-Chick-Killing-Report-2023.pdf): "Chick and Duckling Killing: Achieving an EU-Wide Prohibition" (White paper, January 2023) by Alice Di Concetto, Olivier Morice, Matthias Corion, Simão Santos.\n"""
    for i, row in tb.iterrows():
        sources_text += f"- {row['country']}: {row['status']}. Source: [{row['evidence']}]({row['url']})"
        if len(row["comments"]) > 0:
            sources_text += f" {row['comments']}"
        sources_text += "\n"
    # Replace the full citation in the metadata.
    snap.metadata.origin.citation_full = sources_text  # type: ignore
    # Rewrite metadata to dvc file.
    snap.metadata_path.write_text(snap.metadata.to_yaml())

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)


if __name__ == "__main__":
    main()
