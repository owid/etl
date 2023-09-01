"""Load a snapshot and create a meadow dataset.

The data is manually extracted from the snapshot pdf file, specifically from pages 16 and 17.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("chick_culling_laws.pdf")

    #
    # Process data.
    #
    # Manually extract data from the tables in pages 16 and 17, where current laws in the EU are listed.
    columns = ["country", "status", "year", "comments", "evidence", "url"]
    data = [
        (
            "Austria",
            "Banned by law",
            2023,
            "Enforcement date: 2022-07-18. The prohibition was adopted in July 2022 through a law amending the Animal Welfare Act (130. Bundesgesetz, mit dem das Tierschutzgesetz-TSchG und das Tiertransportgesetz geändert werden). Scope excludes male chicks used as feed in zoos or for birds of prey. Destruction of non-hatched is allowed up until 14 day of incubation.",
            "Section 6(2), Animal Welfare Act [Original: Tierschutzgesetz-TSch, Section 6(2)]",
            "https://www.ris.bka.gv.at/GeltendeFassung.wxe?Abfrage=Bundesnormen&Gesetzesnummer=20003541",
        ),
        (
            "France",
            "Banned by law",
            2023,
            "Enforcement date: 2022-12-31. The prohibition was adopted on January 2022, through a regulation (Décret n° 2022-137 du 5 février 2022 relatif à l'interdiction de mise à mort des poussins des lignées de l'espèce Gallus gallus destinées à la production d'oeufs de consommation et à la protection des animaux dans le cadre de leur mise à mort en dehors des établissements d'abattage). In ovo sexing technologies benefit from a five-year nonobsolescence clause. Male chicks for animal food production benefit from an exemption. Destruction of non-hatched is allowed up until 15 day of incubation.",
            "R 214-17 of the Rural Code [Original: R.214-17, Code rural et la pêche maritime]",
            "https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000028969470",
        ),
        (
            "Germany",
            "Banned by law",
            2022,
            "Enforcement date: 2022-01-01. The prohibiton was adopted on January 2022, through a regulation which prohibits the culling of one-day old chicks by 2022, and the culling of fertilized eggs passed the 6th day of incubation. Note: No derogation.",
            "Section 3 (4c), Animal Welfare Act [Original: Tierschutzgesetz, Dritter Abschnitt Töten von Tieren, 4c]",
            "https://www.gesetze-im-internet.de/tierschg/BJNR012770972.html",
        ),
        (
            "Italy",
            "Planned to be banned by law",
            2027,
            "Enforcement date: 2026-12-31. The law prohibits the selective killing of male chicks by December 31st, 2026 and provides exemptions for animal protection purposes only. A decree will later specify the ways in which the law should be implemented. The law does not provide a rule regarding the destruction of non-hatched eggs nor exemptions, other than exemptions for animal health and protection purposes. A decree will likely specify these two aspects.",
            "Article 18, European Delegation Law (22G00136) [Original: Articolo 18, Delega al Governo per il recepimento delle direttive europee e l'attuazione di altri atti normativi dell'Unione europea - Legge di delegazione europea 2021 (22G00136)]",
            "https://www.normattiva.it/uri-res/N2Ls?urn:nir:stato:legge:2022-08-04;127",
        ),
    ]
    tb = snap.read_from_records(data=data, columns=columns)

    # Create a new table and ensure all columns are snake-case.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
