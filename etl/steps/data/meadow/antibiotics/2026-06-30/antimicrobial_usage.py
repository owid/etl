"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("antimicrobial_usage.xlsx")

    # Load data from snapshot.
    tb_class = snap.read(sheet_name="Antimicrobial_Use_ATC4")
    tb_aware = snap.read(sheet_name="Antibiotic_Use_AWaRe")
    # The sheet containing notes on the data which can go in the metadata
    tb_note = snap.read(sheet_name="Notes")
    note_dict = dict(zip(tb_note["Notes"], tb_note["Explenation"]))  # sic!

    tb_class = tb_class.rename(columns={"CountryTerritoryArea": "country", "Year": "year"})
    tb_aware = tb_aware.rename(columns={"CountryTerritoryArea": "country", "Year": "year"})

    tb_class["Note"] = tb_class["Note"].map(note_dict)
    tb_aware["Note"] = tb_aware["Note"].map(note_dict)

    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_class = tb_class.format(
        ["country", "year", "antimicrobialclass", "atc4name", "routeofadministration"], short_name="class"
    )
    tb_aware = tb_aware.format(["country", "year", "awarelabel", "routeofadministration"], short_name="aware")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb_class, tb_aware], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
