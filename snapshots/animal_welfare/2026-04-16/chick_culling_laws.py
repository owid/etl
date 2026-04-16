"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import numpy as np

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/chick_culling_laws.csv")

    # Create a table of data manually extracted from different websites.
    columns = ["country", "status", "year_effective", "comments", "evidence", "url"]
    data = [
        # Add countries where chick culling is fully or partially banned (or planned to be so).
        (
            "Austria",
            "Banned",
            2023,
            "Chicks used for feed production are exempt.",
            "Section 6(2a) of the Animal Welfare Act (§ 6 Abs. 2a Tierschutzgesetz).",
            "https://www.ris.bka.gv.at/GeltendeFassung.wxe?Abfrage=Bundesnormen&Gesetzesnummer=20003541",
        ),
        (
            "France",
            "Banned",
            2023,
            "Chicks used for animal feed are exempt.",
            "Article R 214-17(II) of the Rural Code (Article R 214-17(II) du Code rural et de la pêche maritime).",
            "https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000045129069",
        ),
        (
            "Germany",
            "Banned",
            2022,
            "",
            "Section 4c of the Animal Welfare Act (§ 4c Tierschutzgesetz).",
            "https://www.gesetze-im-internet.de/tierschg/BJNR012770972.html",
        ),
        (
            "Italy",
            "Banned but not yet in effect",
            2027,
            "Date effective: 2026-12-31.",
            "Legislative Decree 205/2023 (Decreto Legislativo 7 dicembre 2023, n. 205).",
            "https://www.normattiva.it/eli/id/2023/12/23/23G00212/ORIGINAL",
        ),
        (
            "Luxembourg",
            "Banned",
            2018,
            "",
            "Law of 27 June 2018 on animal protection (Loi du 27 juin 2018 sur la protection des animaux).",
            "https://legilux.public.lu/eli/etat/leg/loi/2018/06/27/a537/jo",
        ),
        (
            "Switzerland",
            "Partially banned",
            2020,
            "Shredding of live chicks is banned. Killing by gas remains legal.",
            "Article 20(g) of the Animal Protection Ordinance (Art. 20 Bst. g Tierschutzverordnung).",
            "https://www.fedlex.admin.ch/eli/cc/2008/416/de",
        ),
        (
            "Belgium",
            "Partially banned",
            2025,
            "Wallonia banned chick culling in 2021 and adopted a ban on gassing in first reading on 3 July 2025. Flanders and Brussels have not enacted bans.",
            "Walloon Government communiqué, 3 July 2025 (Communiqué du Gouvernement de Wallonie, 3 juillet 2025).",
            "https://www.wallonie.be/fr/acteurs-et-institutions/wallonie/gouvernement-de-wallonie/communiques-presse/2025-07-03-0",
        ),
        (
            "Norway",
            "Not banned",
            np.nan,
            "No binding legislation. The industry pledged to adopt in-ovo sexing by July 2027.",
            "White Paper on Animal Welfare, Norwegian Government (Meld. St. 8 (2024-2025) Dyrevelferd, Regjeringen).",
            "https://www.regjeringen.no/no/dokumenter/meld.-st.-8-20242025/id3080297/?ch=7",
        ),
        # Add countries for which there is evidence of chick culling with no ban.
        (
            "Australia",
            "Not banned",
            np.nan,
            "",
            "Royal Society for the Prevention of Cruelty to Animals (RSPCA) Australia (2021-09-22).",
            "https://kb.rspca.org.au/knowledge-base/what-happens-with-male-chicks-in-the-egg-industry/",
        ),
        (
            "Canada",
            "Not banned",
            np.nan,
            "",
            "Canadian Poultry Magazine (2016-12-19).",
            "https://www.canadianpoultrymag.com/hypereye-a-game-changer-30033/",
        ),
        (
            "New Zealand",
            "Not banned",
            np.nan,
            "",
            "Save Animals From Exploitation (SAFE) New Zealand.",
            "https://safe.org.nz/our-work/animals-in-aotearoa/hens-2/",
        ),
        (
            "United Kingdom",
            "Not banned",
            np.nan,
            "",
            "The Humane League (2024-03-21).",
            "https://thehumaneleague.org.uk/article/what-happens-to-male-chicks-in-the-egg-industry",
        ),
        (
            "United States",
            "Not banned",
            np.nan,
            "",
            "Animal Legal & Historical Center, Michigan State University.",
            "https://www.animallaw.info/article/detailed-discussion-legal-protections-domestic-chicken-united-states-and-europe",
        ),
    ]
    # Countries in the European Union for which there is no law against chick culling.
    rest_of_eu = [
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
            ),
        )
    tb = snap.read_from_records(data=data, columns=columns)

    # Add all individual sources to the full citation in the metadata.
    sources_text = """Evidence of laws banning chick culling, and evidence of chick culling being practiced without any ban, has been gathered from various sources for different countries.\n Some of those sources were extracted from [a report by the European Institute for Animal Law & Policy](https://animallaweurope.com/wp-content/uploads/2023/01/Animal-Law-Europe-%E2%80%93-Chick-Killing-Report-2023.pdf): "Chick and Duckling Killing: Achieving an EU-Wide Prohibition" (White paper, January 2023) by Alice Di Concetto, Olivier Morice, Matthias Corion, Simão Santos.\n"""
    for _, row in tb.iterrows():
        sources_text += f"- {row['country']}: {row['status']}. Source: [{row['evidence']}]({row['url']})"
        if len(row["comments"]) > 0:
            sources_text += f" {row['comments']}"
        sources_text += "\n"
    # Replace the full citation in the metadata.
    snap.metadata.origin.citation_full = sources_text  # ty: ignore
    # Rewrite metadata to dvc file.
    snap.metadata.save()

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)


if __name__ == "__main__":
    run()
