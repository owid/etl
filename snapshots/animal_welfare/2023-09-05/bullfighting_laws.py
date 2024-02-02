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
            "",
            "Law 700, Law for the defense of animals against acts of cruelty and mistreatment, June 1st, 2015.",
            "https://faolex.fao.org/docs/pdf/bol146525.pdf",
            # Also relevant: Los Tiempos (2018-05-22)
            # "https://www.lostiempos.com/actualidad/pais/20180522/alcaldia-paz-impide-corrida-toros-pese-resistencia-vecinal",
            "",
        ),
        (
            "Brazil",
            "Banned",
            1934,
            "",
            "Decree 24645, July 10th, 1934.",
            "https://www2.camara.leg.br/legin/fed/decret/1930-1939/decreto-24645-10-julho-1934-516837-publicacaooriginal-1-pe.html",
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
            "Chile",
            "Banned",
            1823,
            "",
            "Law bulletin 1 number 18, heading 140 (page 183), September 15th, 1823.",
            "https://www.bcn.cl/leychile/navegar?idNorma=1092548",
            "",
        ),
        (
            "China",
            "Not banned",
            np.nan,
            "Chinese bullfighting practices involve bulls fighting against each other or against humans.",
            "Sixth Tone (2020-11-06).",
            "https://www.sixthtone.com/news/1006372",
            "",
        ),
        (
            "Colombia",
            "Not banned",
            np.nan,
            "",
            "Animal Legal and Historical Center (2023).",
            "http://web.archive.org/web/20230906141841/http://196.40.56.11/scij/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?nValor1=1&nValor2=11967",
            "",
        ),
        (
            "Costa Rica",
            "Partially banned",
            1989,
            "Although bullfighting is not banned, the bull cannot be killed during the event. Traditional bullfighting practices do not involve stabbing the bull.",
            "Bullfighting Activities Regulations 19183-G-S, July 7th, 1989.",
            "http://www.pgrweb.go.cr/scij/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?nValor1=1&nValor2=11967",
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
        # Although Denmark's ban is mentioned by Humane Society International, I haven't found any evidence for this ban, or for any bullfighting in Denmark.
        # (
        #     "Denmark",
        #     "Banned",
        #     np.nan,
        #     "",
        #     "Humane Society International (2023).",
        #     "https://www.hsi.org/news-resources/bullfighting/",
        #     "",
        # ),
        (
            "Dominican Republic",
            "Partially banned",
            2022,
            "Although bullfighting is not banned, the bull cannot be harmed during the event. Bullfighting is practiced only in El Seibo province, where it is considered Cultural Heritage.",
            "Law 1311, Declaring Bullfighting Cultural Heritage, approval published on April 15th, 2022.",
            "https://memoriahistorica.senadord.gob.do/handle/123456789/23159",
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
        # I have found some evidence that bullfighting also takes place in Guatemala, but no good sources.
        (
            "Guatemala",
            "Not banned",
            np.nan,
            "",
            "Portal taurino (2020-03-26).",
            "http://www.portaltaurino.net/enciclopedia/doku.php/guatemala",
            "",
        ),
        (
            "Honduras",
            "Partially banned",
            2016,
            "Although bullfighting is not banned, the use of spears, swords, fires or other objects that may kill or cause pain to the animal is prohibited.",
            "Decree 115-2015, Animal Protection and Welfare Law, April 5th, 2016.",
            "https://www.poderjudicial.gob.hn/CEDIJ/Leyes/Documents/Ley%20de%20Proteccion%20y%20Bienestar%20Animal.pdf",
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
            "India",
            "Not banned",
            np.nan,
            "Indian bullfighting involves bulls wrestling against humans.",
            "The Times of India (2023-05-19).",
            "https://timesofindia.indiatimes.com/india/supreme-court-upholds-tamil-nadu-law-passed-to-overturn-courts-jallikattu-ban/articleshow/100339245.cms?from=mdr",
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
            "Partially banned",
            2011,
            "Although bullfighting is not banned, the use of spears, swords, fires or other objects that may kill or cause pain to the animal is prohibited.",
            "Law 747, Law for the protection and welfare of domestic animals and domesticated wild animals, May 11th, 2011.",
            "http://legislacion.asamblea.gob.ni/normaweb.nsf/b92aaea87dac762406257265005d21f7/cf820e2a63b1b690062578b00074ec1b",
            "",
        ),
        (
            "Panama",
            "Banned",
            2012,
            "",
            "Law 308, Animal Protection Law, March 15th, 2012.",
            "https://www.asamblea.gob.pa/APPS/SEG_LEGIS/PDF_SEG/PDF_SEG_2010/PDF_SEG_2011/PROYECTO/2011_P_308.pdf",
            "",
        ),
        (
            "Paraguay",
            "Banned",
            2013,
            "",
            "Law 4840, Animal Protection and Welfare Law, June 3rd, 2013.",
            "https://www.bacn.gov.py/leyes-paraguayas/954/de-proteccion-y-bienestar-animal",
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
            "ABC Cultura (2021-08-24).",
            "https://www.abc.es/cultura/toros/abci-situacion-tauromaquia-espana-donde-prohibidas-corridas-toros-nsv-202108241122_noticia.html",
            "",
        ),
        (
            "United Kingdom",
            "Banned",
            1835,
            "Although bullfighting has not been practiced, other similar events like bull-baiting used to take place prior to the enactment of the Cruelty to Animals Act 1835.",
            "5 & 6 William 4 c.59: Cruelty to Animals Act, 1835.",
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
            "Law 4277, Animal Health Legislation, October 30th, 1912.",
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
    sources_text = """Evidence of laws banning bullfighting, and evidence of bullfighting being practiced without any ban, has been gathered from various sources for different countries.\n"""
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
