"""Script to create a snapshot of dataset.

The data is manually curated, and this file is its single source of truth: every per-country fact (status, the year
when the ban took effect, source, and comments) is defined below. The citation_full field of the .dvc
file is built by this script from the entries below; never edit it by hand.

Each source has a copy in the Wayback Machine (url_archived). When the original page is gone, url is the archived copy.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Status of bullfighting in each country, and the evidence for it.
# The status and year_effective fields become the data; the rest is used only in the citation.
COUNTRIES = [
    {
        "country": "Argentina",
        "status": "Banned",
        "year_effective": 1891,
        "source": "Law 2786 - Buenos Aires, July 25th, 1891.",
        "url": "https://web.archive.org/web/20261014084646/https://www.argentina.gob.ar/normativa/nacional/ley-2786-283995/texto",
        "url_archived": "",
        "comments": "Last bullfight took place [in 1899](https://www.animanaturalis.org/p/883) ([archived](https://web.archive.org/web/20260828194738/https://www.animanaturalis.org/p/883)).",
    },
    {
        "country": "Bolivia",
        "status": "Banned",
        "year_effective": 2015,
        "source": "Law 700, Law for the defense of animals against acts of cruelty and mistreatment, June 1st, 2015.",
        "url": "https://faolex.fao.org/docs/pdf/bol146525.pdf",
        "url_archived": "https://web.archive.org/web/20260721012417/https://faolex.fao.org/docs/pdf/bol146525.pdf",
        "comments": "",
    },
    {
        "country": "Brazil",
        "status": "Banned",
        "year_effective": 1934,
        "source": "Decree 24645, July 10th, 1934.",
        "url": "https://www2.camara.leg.br/legin/fed/decret/1930-1939/decreto-24645-10-julho-1934-516837-publicacaooriginal-1-pe.html",
        "url_archived": "https://web.archive.org/web/20261105061633/https://www2.camara.leg.br/legin/fed/decret/1930-1939/decreto-24645-10-julho-1934-516837-publicacaooriginal-1-pe.html",
        "comments": "",
    },
    {
        "country": "Canada",
        "status": "Partially banned",
        "year_effective": None,
        "source": "National Post (2015-01-25).",
        "url": "https://nationalpost.com/news/canada/there-will-be-no-blood-in-ontario-bullfighting-ring-where-matadors-fight-with-velcro-tipped-blades",
        # NOTE: The Wayback Machine could not save this page.
        "url_archived": "",
        "comments": 'Although bullfighting is not banned, the bull cannot be stabbed or killed during the event - a ["bloodless" bullfight](https://web.archive.org/web/20260813185611/https://madridbullfighting.com/blog/ufaqs/how-is-american-bullfighting-still-legal/). Velcro spears are used to simulate the stabbing of the bulls.',
    },
    {
        "country": "Chile",
        "status": "Banned",
        "year_effective": 1823,
        "source": "Law bulletin 1 number 18, heading 140 (page 183), September 15th, 1823.",
        "url": "https://www.bcn.cl/leychile/navegar?idNorma=1092548",
        "url_archived": "https://web.archive.org/web/20261004170228/https://www.bcn.cl/leychile/navegar?idNorma=1092548",
        "comments": "",
    },
    {
        "country": "China",
        "status": "Not banned",
        "year_effective": None,
        "source": "Sixth Tone (2020-11-06).",
        "url": "https://www.sixthtone.com/news/1006372",
        "url_archived": "https://web.archive.org/web/20260813222525/https://www.sixthtone.com/news/1006372",
        "comments": "Chinese bullfighting practices involve bulls fighting against each other or against humans.",
    },
    {
        "country": "Colombia",
        "status": "Banned but not yet in effect",
        "year_effective": 2027,
        "source": "Law 2385 of 2024, July 22nd, 2024.",
        "url": "https://normograma.mintic.gov.co/mintic/compilacion/docs/ley_2385_2024.htm",
        "url_archived": "https://web.archive.org/web/20261126110516/https://normograma.mintic.gov.co/mintic/compilacion/docs/ley_2385_2024.htm",
        "comments": "The ban takes effect three years after the law entered into force, in July 2027. The Constitutional Court upheld the law [in 2025](https://www.corteconstitucional.gov.co/relatoria/2025/C-374-25.htm) ([archived](https://web.archive.org/web/20261126110640/https://www.corteconstitucional.gov.co/relatoria/2025/C-374-25.htm)).",
    },
    {
        "country": "Costa Rica",
        "status": "Partially banned",
        "year_effective": 1989,
        "source": "Bullfighting Activities Regulations 19183-G-S, July 7th, 1989.",
        "url": "http://www.pgrweb.go.cr/scij/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?nValor1=1&nValor2=11967",
        "url_archived": "https://web.archive.org/web/20261126091906/https://sinalevi.go.cr/ResultadosNormativa/Informacion?param1=11967&param2=118013&param3=1",
        "comments": "Although bullfighting is not banned, the bull cannot be killed during the event. Traditional bullfighting practices do not involve stabbing the bull.",
    },
    {
        "country": "Cuba",
        "status": "Banned",
        "year_effective": 1899,
        "source": "Cubanet (2022-02-10).",
        "url": "https://www.cubanet.org/opiniones/corridas-de-toros-en-cuba-la-historia-no-contada/",
        "url_archived": "https://web.archive.org/web/20221016212726/https://www.cubanet.org/opiniones/corridas-de-toros-en-cuba-la-historia-no-contada/",
        "comments": "",
    },
    # NOTE: Although Denmark's ban is mentioned by Humane Society International, I haven't found any evidence for this
    # ban, or for any bullfighting in Denmark.
    {
        "country": "Dominican Republic",
        "status": "Partially banned",
        "year_effective": 2022,
        "source": "Law 1311, Declaring Bullfighting Cultural Heritage, approval published on April 15th, 2022.",
        "url": "https://memoriahistorica.senadord.gob.do/handle/123456789/23159",
        "url_archived": "https://web.archive.org/web/20260714223325/https://memoriahistorica.senadord.gob.do/handle/123456789/23159",
        "comments": "Although bullfighting is not banned, the bull cannot be harmed during the event. Bullfighting is practiced only in El Seibo province, where it is considered Cultural Heritage.",
    },
    {
        "country": "Ecuador",
        "status": "Not banned",
        "year_effective": None,
        "source": "People for the Ethical Treatment of Animals (PETA) Latino.",
        "url": "https://www.petalatino.com/blog/las-corridas-de-toros-siguen-siendo-legales-en-estos-paises/",
        "url_archived": "https://web.archive.org/web/20250914023847/https://www.petalatino.com/blog/las-corridas-de-toros-siguen-siendo-legales-en-estos-paises/",
        "comments": "",
    },
    {
        "country": "France",
        "status": "Not banned",
        "year_effective": None,
        "source": "France24 (2022-11-24).",
        "url": "https://www.france24.com/en/europe/20221124-french-bid-to-ban-bullfighting-dropped-amid-obstruction-in-parliament",
        "url_archived": "https://web.archive.org/web/20230929202920/https://www.france24.com/en/europe/20221124-french-bid-to-ban-bullfighting-dropped-amid-obstruction-in-parliament",
        "comments": "Although not banned nationally, bullfighting is only practiced in certain regions in the south of France.",
    },
    # NOTE: I have found some evidence that bullfighting also takes place in Guatemala, but no good sources.
    {
        "country": "Guatemala",
        "status": "Not banned",
        "year_effective": None,
        "source": "Portal taurino (2020-03-26).",
        "url": "http://www.portaltaurino.net/enciclopedia/doku.php/guatemala",
        "url_archived": "https://web.archive.org/web/20250522033031/http://www.portaltaurino.net/enciclopedia/doku.php/guatemala",
        "comments": "",
    },
    {
        "country": "Honduras",
        "status": "Partially banned",
        "year_effective": 2016,
        "source": "Decree 115-2015, Animal Protection and Welfare Law, April 5th, 2016.",
        "url": "https://web.archive.org/web/20230123122257/https://www.poderjudicial.gob.hn/CEDIJ/Leyes/Documents/Ley%20de%20Proteccion%20y%20Bienestar%20Animal.pdf",
        "url_archived": "",
        "comments": "Although bullfighting is not banned, the use of spears, swords, fires or other objects that may kill or cause pain to the animal is prohibited.",
    },
    {
        "country": "India",
        "status": "Not banned",
        "year_effective": None,
        "source": "The Times of India (2023-05-19).",
        "url": "https://timesofindia.indiatimes.com/india/supreme-court-upholds-tamil-nadu-law-passed-to-overturn-courts-jallikattu-ban/articleshow/100339245.cms?from=mdr",
        "url_archived": "https://web.archive.org/web/20260107052412/https://timesofindia.indiatimes.com/india/supreme-court-upholds-tamil-nadu-law-passed-to-overturn-courts-jallikattu-ban/articleshow/100339245.cms?from=mdr",
        "comments": "Indian bullfighting involves bulls wrestling against humans.",
    },
    {
        "country": "Italy",
        "status": "Banned",
        "year_effective": None,
        "source": "Humane Society International (2023).",
        "url": "https://web.archive.org/web/20250113173306/https://www.hsi.org/news-resources/bullfighting/",
        "url_archived": "",
        "comments": "",
    },
    {
        "country": "Mexico",
        "status": "Not banned",
        "year_effective": None,
        "source": "Humane Society International (2022-01-31).",
        "url": "https://www.humaneworld.org/en/news/sinaloa-becomes-fifth-state-mexico-ban",
        "url_archived": "https://web.archive.org/web/20260717055502/https://www.humaneworld.org/en/news/sinaloa-becomes-fifth-state-mexico-ban",
        "comments": "Although not banned nationally, bullfighting has been banned in various states. In Mexico City, which has the biggest bullfighting ring in the world, bullfights are only allowed if the bull is neither killed nor wounded, since [a reform in 2025](https://www.unotv.com/estados/ciudad-de-mexico/corridas-de-toros-sin-violencia-en-cdmx-publican-ley-en-gaceta-entra-en-vigor-manana/) ([archived](https://web.archive.org/web/20250325190546/https://www.unotv.com/estados/ciudad-de-mexico/corridas-de-toros-sin-violencia-en-cdmx-publican-ley-en-gaceta-entra-en-vigor-manana/)).",
    },
    {
        "country": "Nicaragua",
        "status": "Partially banned",
        "year_effective": 2011,
        "source": "Law 747, Law for the protection and welfare of domestic animals and domesticated wild animals, May 11th, 2011.",
        "url": "http://legislacion.asamblea.gob.ni/normaweb.nsf/b92aaea87dac762406257265005d21f7/cf820e2a63b1b690062578b00074ec1b",
        "url_archived": "https://web.archive.org/web/20261004170207/http://legislacion.asamblea.gob.ni/normaweb.nsf/b92aaea87dac762406257265005d21f7/cf820e2a63b1b690062578b00074ec1b",
        "comments": "Although bullfighting is not banned, the use of spears, swords, fires or other objects that may kill or cause pain to the animal is prohibited.",
    },
    {
        "country": "Panama",
        "status": "Partially banned",
        "year_effective": 2012,
        "source": "Law 70 of 2012, Article 13, Official Gazette No. 27145-A, October 18th, 2012.",
        "url": "https://www.gacetaoficial.gob.pa/pdfTemp/27145_A/GacetaNo_27145a_20121018.pdf",
        "url_archived": "https://web.archive.org/web/20240416073252/https://www.gacetaoficial.gob.pa/pdfTemp/27145_A/GacetaNo_27145a_20121018.pdf",
        "comments": 'Spanish- and Portuguese-style bullfights are banned, but traditional bull events ("corridas o barrera de toros") are allowed.',
    },
    {
        "country": "Paraguay",
        "status": "Banned",
        "year_effective": 2013,
        "source": "Law 4840, Animal Protection and Welfare Law, June 3rd, 2013.",
        "url": "https://www.bacn.gov.py/leyes-paraguayas/954/de-proteccion-y-bienestar-animal",
        "url_archived": "https://web.archive.org/web/20250912145713/https://www.bacn.gov.py/leyes-paraguayas/954/de-proteccion-y-bienestar-animal",
        "comments": "",
    },
    {
        "country": "Peru",
        "status": "Not banned",
        "year_effective": None,
        "source": "People for the Ethical Treatment of Animals (PETA) Latino.",
        "url": "https://www.petalatino.com/blog/las-corridas-de-toros-siguen-siendo-legales-en-estos-paises/",
        "url_archived": "https://web.archive.org/web/20250914023847/https://www.petalatino.com/blog/las-corridas-de-toros-siguen-siendo-legales-en-estos-paises/",
        "comments": "",
    },
    {
        "country": "Portugal",
        "status": "Not banned",
        "year_effective": None,
        "source": "European Society of Dog and Animal Welfare (2022-12).",
        "url": "https://www.esdaw.eu/bullfighting---portugal.html",
        "url_archived": "https://web.archive.org/web/20260919125652/https://www.esdaw.eu/bullfighting---portugal.html",
        "comments": "Although killing the bull is not allowed during the event, the bull is still severely injured (by stabbing different weapons in the bull's back) and is in most occasions killed after the event.",
    },
    {
        "country": "Spain",
        "status": "Not banned",
        "year_effective": None,
        "source": "ABC Cultura (2021-08-24).",
        "url": "https://www.abc.es/cultura/toros/abci-situacion-tauromaquia-espana-donde-prohibidas-corridas-toros-nsv-202108241122_noticia.html",
        "url_archived": "https://web.archive.org/web/20251017150413/https://www.abc.es/cultura/toros/abci-situacion-tauromaquia-espana-donde-prohibidas-corridas-toros-nsv-202108241122_noticia.html",
        "comments": "Although not banned nationally, it is banned in the autonomous community of Canary Islands, and have stopped taking place in Catalonia, as well as many Spanish municipalities. However, bullfighting is considered cultural heritage in other autonomous communities of Spain.",
    },
    {
        "country": "United Kingdom",
        "status": "Banned",
        "year_effective": 1835,
        "source": "5 & 6 William 4 c.59: Cruelty to Animals Act, 1835.",
        "url": "https://web.archive.org/web/20250113173306/https://www.hsi.org/news-resources/bullfighting/",
        "url_archived": "",
        "comments": "Although bullfighting has not been practiced, other similar events like bull-baiting used to take place prior to the enactment of the Cruelty to Animals Act 1835.",
    },
    {
        "country": "United States",
        "status": "Partially banned",
        "year_effective": None,
        "source": "CBS (2021-10-11).",
        "url": "https://www.cbsnews.com/sanfrancisco/news/bloodless-bullfighting-a-portuguese-tradition-kept-alive-in-central-valley/",
        "url_archived": "https://web.archive.org/web/20260719073646/https://www.cbsnews.com/sanfrancisco/news/bloodless-bullfighting-a-portuguese-tradition-kept-alive-in-central-valley/",
        "comments": 'Although bullfighting is not banned, the bull cannot be stabbed or killed during the event - a ["bloodless" bullfight](https://web.archive.org/web/20260813185611/https://madridbullfighting.com/blog/ufaqs/how-is-american-bullfighting-still-legal/). Velcro spears are used to simulate the stabbing of the bulls.',
    },
    {
        "country": "Uruguay",
        "status": "Banned",
        "year_effective": 1912,
        "source": "Law 4277, Animal Health Legislation, October 30th, 1912.",
        "url": "https://www.gub.uy/ministerio-ganaderia-agricultura-pesca/comunicacion/publicaciones/legislacion-sanitaria-animal/introduccion/c-organismo-responsable",
        "url_archived": "https://web.archive.org/web/20260801052209/https://www.gub.uy/ministerio-ganaderia-agricultura-pesca/comunicacion/publicaciones/legislacion-sanitaria-animal/introduccion/c-organismo-responsable",
        "comments": "",
    },
    {
        "country": "Venezuela",
        "status": "Not banned",
        "year_effective": None,
        "source": "France24 (2023-04-05).",
        "url": "https://www.france24.com/en/live-news/20230405-look-brave-children-taught-bullfighting-at-venezuelan-torero-school",
        "url_archived": "https://web.archive.org/web/20230407005359/https://www.france24.com/en/live-news/20230405-look-brave-children-taught-bullfighting-at-venezuelan-torero-school",
        "comments": "Although not banned nationally, various municipalities are officially declared anti-bullfighting, including the capital, Caracas.",
    },
]

CITATION_INTRO = (
    "Evidence of laws banning bullfighting, and evidence of bullfighting being practiced without any ban, has been "
    "gathered from various sources for different countries."
)


def build_citation_full() -> str:
    lines = [CITATION_INTRO]
    for row in COUNTRIES:
        line = f"- {row['country']}: {row['status']}. Source: [{row['source']}]({row['url']})"
        if row["url_archived"]:
            line += f" ([archived]({row['url_archived']}))"
        if row["comments"]:
            line += f" {row['comments']}"
        lines.append(line)
    return "\n".join(lines)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Create the data table.
    columns = ["country", "status", "year_effective"]
    data = [(row["country"], row["status"], row["year_effective"]) for row in COUNTRIES]
    tb = snap.read_from_records(data=data, columns=columns)

    # Build the full citation from the curated entries, and rewrite the metadata (.dvc) file.
    snap.metadata.origin.citation_full = build_citation_full()  # ty: ignore
    snap.metadata.save()

    # Add file to DVC and upload to S3.
    snap.create_snapshot(data=tb, upload=upload)
