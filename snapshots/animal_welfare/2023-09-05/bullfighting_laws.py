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
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/bullfighting_laws.csv")

    # Create a table of data manually extracted from different websites.
    columns = ["country", "status", "year_effective", "comments", "evidence", "url", "annotation"]
    data = [
        (
            "Argentina",
            "Banned",
            1891,
            "Last bullfight took place [in 1899](https://www.animanaturalis.org/p/883).",
            "Law 2786 - Buenos Aires, July 25th, 1891.",
            "https://www.argentina.gob.ar/normativa/nacional/ley-2786-283995/texto",
            "",
        ),
        (
            "Bolivia",
            "Banned",
            2015,
            "Law for the defense of animals against acts of cruelty and mistreatment, Law 700, June 1st, 2015.",
            "https://faolex.fao.org/docs/pdf/bol146525.pdf",
            # Also relevant: Los Tiempos (2018-05-22)
            # "https://www.lostiempos.com/actualidad/pais/20180522/alcaldia-paz-impide-corrida-toros-pese-resistencia-vecinal",
            "",
        ),
        (
            "Brazil",
            "Banned",
            np.nan,
            "",
            "",
            "",
            "",
        ),
        (
            "Canada",
            "Partially banned",
            np.nan,
            'Although bullfighting is not banned, the bull cannot be stabbed or killed during the event - a ["bloodless" bullfight](https://madridbullfighting.com/blog/ufaqs/how-is-american-bullfighting-still-legal/). Velcro spears are used to simulate the stabbing of the bulls.',
            "National Post (2015-01-25).",
            "https://nationalpost.com/news/canada/there-will-be-no-blood-in-ontario-bullfighting-ring-where-matadors-fight-with-velcro-tipped-blades",
            "",
        ),
        (
            "Colombia",
            "Not banned",
            np.nan,
            "",
            "Animal Legal and Historical Center (2023).",
            "http://196.40.56.11/scij/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?nValor1=1&nValor2=11967",
            "",
        ),
        (
            "Costa Rica",
            "Partially banned",
            1989,
            "Although bullfighting is not banned, the bull cannot be killed during the event.",
            "Bullfighting Activities Regulations 19183-G-S, July 7th, 1989.",
            "https://www.cbsnews.com/sanfrancisco/news/bloodless-bullfighting-a-portuguese-tradition-kept-alive-in-central-valley/",
            "",
        ),
        (
            "Cuba",
            "Banned",
            1899,
            "",
            "Cubanet (2022-02-10).",
            "https://www.cubanet.org/opiniones/corridas-de-toros-en-cuba-la-historia-no-contada/",
            "",
        ),
        (
            "Denmark",
            "Banned",
            np.nan,
            "",
            "Humane Society International (2023).",
            "https://www.hsi.org/news-resources/bullfighting/",
            "",
        ),
        (
            "Ecuador",
            "Not banned",
            np.nan,
            "",
            "People for the Ethical Treatment of Animals (PETA) Latino.",
            "https://www.petalatino.com/blog/las-corridas-de-toros-siguen-siendo-legales-en-estos-paises/",
            "",
        ),
        (
            "France",
            "Not banned",
            np.nan,
            "Although not banned nationally, bullfighting is only practiced in certain regions in the south of France.",
            "France24 (2022-11-24).",
            "https://www.france24.com/en/europe/20221124-french-bid-to-ban-bullfighting-dropped-amid-obstruction-in-parliament",
            "",
        ),
        (
            "Italy",
            "Banned",
            np.nan,
            "",
            "Humane Society International (2023).",
            "https://www.hsi.org/news-resources/bullfighting/",
            "",
        ),
        (
            "Mexico",
            "Not banned",
            np.nan,
            "Although not banned nationally, bullfighting has been banned in various states, and [indefinitely suspended in Plaza Mexico](https://www.dw.com/es/juez-suspende-corridas-de-toros-en-la-mayor-plaza-de-m%C3%A9xico/a-62097769), in Mexico City, which is the biggest bullfighting ring in the world.",
            "Humane Society International (2022-01-31).",
            "https://www.hsi.org/news-resources/sinaloa-becomes-fifth-state-in-mexico-to-ban-bullfighting/",
            "",
        ),
        (
            "Nicaragua",
            "Banned",
            np.nan,
            "",
            "Humane Society International (2023).",
            "https://www.hsi.org/news-resources/bullfighting/",
            "",
        ),
        (
            "Peru",
            "Not banned",
            np.nan,
            "",
            "People for the Ethical Treatment of Animals (PETA) Latino.",
            "https://www.petalatino.com/blog/las-corridas-de-toros-siguen-siendo-legales-en-estos-paises/",
            "",
        ),
        (
            "Portugal",
            "Not banned",
            np.nan,
            "Although killing the bull is not allowed during the event, the bull is still severely injured (by stabbing different weapons in the bull's back) and is in most occasions killed after the event.",
            "European Society of Dog and Animal Welfare (2022-12).",
            "https://www.esdaw.eu/bullfighting---portugal.html",
            "",
        ),
        (
            "Spain",
            "Not banned",
            np.nan,
            "Although not banned nationally, it is banned in the autonomous community of Canary Islands, and have stopped taking place in Catalonia, as well as many Spanish municipalities. However, bullfighting is considered cultural heritage in other autonomous communities of Spain.",
            "ABC Cultura (2021-08-24)",
            "https://www.abc.es/cultura/toros/abci-situacion-tauromaquia-espana-donde-prohibidas-corridas-toros-nsv-202108241122_noticia.html",
            "",
        ),
        (
            "United Kingdom",
            "Banned",
            np.nan,
            "",
            "Humane Society International (2023).",
            "https://www.hsi.org/news-resources/bullfighting/",
            "",
        ),
        (
            "United States",
            "Partially banned",
            np.nan,
            'Although bullfighting is not banned, the bull cannot be stabbed or killed during the event - a ["bloodless" bullfight](https://madridbullfighting.com/blog/ufaqs/how-is-american-bullfighting-still-legal/). Velcro spears are used to simulate the stabbing of the bulls.',
            "CBS (2021-10-11).",
            "https://www.cbsnews.com/sanfrancisco/news/bloodless-bullfighting-a-portuguese-tradition-kept-alive-in-central-valley/",
            "",
        ),
        (
            "Uruguay",
            "Banned",
            1912,
            "",
            "Animal health legislation, Law 4.277, October 30th, 1912.",
            "https://www.gub.uy/ministerio-ganaderia-agricultura-pesca/comunicacion/publicaciones/legislacion-sanitaria-animal/introduccion/c-organismo-responsable",
            "",
        ),
        (
            "Venezuela",
            "Not banned",
            np.nan,
            "Although not banned nationally, various municipalities are officially declared anti-bullfighting, including the capital, Caracas.",
            "France24 (2023-04-05).",
            "https://www.france24.com/en/live-news/20230405-look-brave-children-taught-bullfighting-at-venezuelan-torero-school",
            "",
        ),
    ]
    tb = snap.read_from_records(data=data, columns=columns)

    # Add all individual sources to the full citation in the metadata.
    sources_text = """Evidence of laws banning bullfighting, and evidence of bullfighting being practiced without any ban, has been gathered from various sources for different countries. Some of them come from [Humane Society International](https://www.hsi.org/news-resources/bullfighting/).\n"""
    for i, row in tb.iterrows():
        sources_text += f"- {row['country']}: {row['status']}. Source: [{row['evidence']}]({row['url']})"
        if len(row["comments"]) > 0:
            sources_text += f" {row['comments']}"
        sources_text += "\n"
    # Replace the full citation in the metadata.
    snap.metadata.origin.citation_producer = sources_text  # type: ignore
    # Rewrite metadata to dvc file.
    snap.metadata_path.write_text(snap.metadata.to_yaml())

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)


if __name__ == "__main__":
    main()
