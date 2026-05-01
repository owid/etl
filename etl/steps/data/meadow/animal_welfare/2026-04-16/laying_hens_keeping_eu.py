"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("laying_hens_keeping_eu.xlsx")
    tb = snap.read_excel(sheet_name="MS reporting 2011-2021", header=2, usecols="A:H")

    #
    # Process data.
    #
    # Rename columns to snake_case.
    tb = tb.rename(
        columns={
            "Country": "country",
            "Year": "year",
            "Not enriched cage": "not_enriched_cage",
            "Enriched cage": "enriched_cage",
            "Free range": "free_range",
            "Barn": "barn",
            "Organic": "organic",
            "Total": "total",
        }
    )

    # Forward-fill country name (blank for rows after the first year of each country).
    tb["country"] = tb["country"].ffill()

    # Drop rows without a year (empty separator rows).
    tb = tb.dropna(subset=["year"])
    tb["year"] = tb["year"].astype(int)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
