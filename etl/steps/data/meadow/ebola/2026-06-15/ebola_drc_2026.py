"""Load the 2026 Ebola outbreak snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("ebola_drc_2026.csv")
    # Keep values as raw strings; the "ND"/"NA" sentinels are parsed to NaN in garden.
    tb = snap.read_csv(dtype=str)

    #
    # Process data.
    #
    # Low-cardinality string columns -> categoricals (smaller feather, faster reads).
    for col in ["level", "metric", "location"]:
        tb[col] = tb[col].astype("category")

    tb = tb.format(["level", "metric", "location", "date"], short_name="ebola_drc_2026")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
