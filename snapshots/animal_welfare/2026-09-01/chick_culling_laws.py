"""Script to create a snapshot of dataset.

The data is manually curated, and this file is its single source of truth: every per-country fact
(status, effective year, source, and legal nuances) is defined below. The countries with a (full or
partial) ban become the data; all other countries are assumed to have no ban (this is handled in the
garden step). The citation_full field of the .dvc file is built by this script from the entries below —
never edit it by hand.
"""

from pathlib import Path

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Countries with a (full or partial) ban on chick culling.
# The status and year_effective fields become the data; source, url and note are used only in the citation.
BANS = [
    {
        "country": "Austria",
        "status": "Banned",
        "year_effective": 2023,
        "source": "Section 6(2a) of the Animal Welfare Act (§ 6 Abs. 2a Tierschutzgesetz)",
        "url": "https://www.ris.bka.gv.at/GeltendeFassung.wxe?Abfrage=Bundesnormen&Gesetzesnummer=20003541",
        "note": "Chicks used for feed production are exempt.",
    },
    {
        "country": "Belgium",
        "status": "Partially banned",
        "year_effective": 2021,
        "source": "Walloon Government communiqué, 3 July 2025 (Communiqué du Gouvernement de Wallonie, 3 juillet 2025)",
        "url": "https://www.wallonie.be/fr/acteurs-et-institutions/wallonie/gouvernement-de-wallonie/communiques-presse/2025-07-03-0",
        "note": "Regional measures only: Wallonia banned the grinding of live chicks (a ban on gassing is under discussion), and Flanders approved [a ban on killing day-old chicks](https://www.vlaamsparlement.be/nl/actueel/nieuws-uit-het-vlaams-parlement/wanneer-komt-er-een-verbod-op-het-doden-van-eendagshaantjes) in its Animal Welfare Code, which will only take effect on a date yet to be decided.",
    },
    {
        "country": "France",
        "status": "Banned",
        "year_effective": 2023,
        "source": "Article R 214-17(II) of the Rural Code (Article R 214-17(II) du Code rural et de la pêche maritime)",
        "url": "https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000045129069",
        "note": "Chicks used for animal feed are exempt.",
    },
    {
        "country": "Germany",
        "status": "Banned",
        "year_effective": 2022,
        "source": "Section 4c of the Animal Welfare Act (§ 4c Tierschutzgesetz)",
        "url": "https://www.gesetze-im-internet.de/tierschg/BJNR012770972.html",
        "note": "",
    },
    {
        "country": "Italy",
        "status": "Banned but not yet in effect",
        "year_effective": 2026,
        "source": "Legislative Decree 205/2023 (Decreto Legislativo 7 dicembre 2023, n. 205)",
        "url": "https://www.normattiva.it/eli/id/2023/12/23/23G00212/ORIGINAL",
        "note": "Implementing guidelines on in-ovo sexing were published in September 2025.",
    },
    {
        "country": "Luxembourg",
        "status": "Banned",
        "year_effective": 2018,
        "source": "Article 12(13) of the Law of 27 June 2018 on animal protection (Loi du 27 juin 2018 sur la protection des animaux)",
        "url": "https://legilux.public.lu/eli/etat/leg/loi/2018/06/27/a537/jo",
        "note": "General prohibition on killing animals for purely economic reasons.",
    },
    {
        "country": "Switzerland",
        "status": "Partially banned",
        "year_effective": 2020,
        "source": "Article 20(g) of the Animal Protection Ordinance (Art. 20 Bst. g Tierschutzverordnung)",
        "url": "https://www.fedlex.admin.ch/eli/cc/2008/416/de",
        "note": "Shredding of live chicks is banned, and destroying embryos from day 13 of incubation is also banned since February 2025, but killing hatched chicks by gas remains legal.",
    },
]

# Countries (and the European Union) without a ban, where a ban has been formally considered; used only in the
# citation.
CONSIDERED = [
    {
        "country": "European Union",
        "source": "EU Livestock Strategy, European Commission (COM(2026) 576 final)",
        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52026DC0576",
        "note": "The European Commission intends to propose, by the end of 2026, a revision of animal welfare rules for laying hens and broilers that includes ending the systematic killing of male chicks.",
    },
    {
        "country": "Brazil",
        "source": "Bill PL 783/2024, Chamber of Deputies of Brazil (Projeto de Lei 783/2024, Câmara dos Deputados)",
        "url": "https://www.camara.leg.br/proposicoesWeb/fichadetramitacao?idProposicao=2421303",
        "note": "A bill to ban the cruel disposal of chicks is under discussion in Congress.",
    },
    {
        "country": "Netherlands",
        "source": "Parliamentary letter on the chick culling roadmap, Dutch Government (Kamerbrief aanbieden roadmap eendagshaantjes)",
        "url": "https://www.rijksoverheid.nl/documenten/kamerstukken/2025/02/11/kamerbrief-aanbieden-roadmap-eendagshaantjes",
        "note": "A voluntary agreement between the government and the egg sector phases out the culling of male chicks for table-egg production from 2026, but it is not legally binding.",
    },
    {
        "country": "Norway",
        "source": "Parliamentary recommendation on the animal welfare white paper (Innst. 200 S (2024-2025), Stortinget)",
        "url": "https://www.stortinget.no/no/Saker-og-publikasjoner/Publikasjoner/Innstillinger/Stortinget/2024-2025/inns-202425-200s/",
        "note": "A parliamentary proposal to ban chick culling was rejected in 2025; the industry pledged to adopt in-ovo sexing by July 2027.",
    },
    {
        "country": "United Kingdom",
        "source": "The Welfare of Animals at the Time of Killing (England) Regulations 2015 (WATOK)",
        "url": "https://www.legislation.gov.uk/uksi/2015/1782/schedule/2/paragraph/44/made",
        "note": "The government's 2025 animal welfare strategy encourages the industry to end chick culling, but has no legal force.",
    },
]

CITATION_INTRO = (
    "Evidence of laws banning chick culling, and of notable legislative activity in countries without a ban, has "
    "been gathered from official sources for each country. Some of those sources were extracted from [a report by "
    "the European Institute for Animal Law & Policy]"
    "(https://animallaweurope.org/wp-content/uploads/Chick-and-Duckling-Killing-UPDATE-December-2024.pdf): "
    '"Chick and Duckling Killing: Achieving an EU-Wide Prohibition" (White paper, updated December 2024) by '
    "Alice Di Concetto, Olivier Morice, Matthias Corion, Simão Santos."
)

CITATION_CLOSING = (
    "All other countries are shown as not banned: no law against chick culling is known there. For European Union "
    "countries, this is confirmed (as of December 2024) by the report above."
)


def build_citation_full() -> str:
    lines = [
        CITATION_INTRO,
        "",
        "Sources for the countries with a full or partial ban (the status and the year when the ban became, or "
        "will become, effective are given in the data):",
    ]
    lines += [f"- {row['country']}: [{row['source']}]({row['url']}). {row['note']}".strip() for row in BANS]
    lines += ["", "Countries and regions without a ban, where a ban has been formally considered:"]
    lines += [f"- {row['country']}: [{row['source']}]({row['url']}). {row['note']}" for row in CONSIDERED]
    lines += ["", CITATION_CLOSING]
    return "\n".join(lines)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/chick_culling_laws.csv")

    # Create the data table.
    columns = ["country", "status", "year_effective"]
    data = [(row["country"], row["status"], row["year_effective"]) for row in BANS]
    tb = snap.read_from_records(data=data, columns=columns)

    # Build the full citation from the curated entries, and rewrite the metadata (.dvc) file.
    snap.metadata.origin.citation_full = build_citation_full()  # ty: ignore
    snap.metadata.save()

    # Add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)
