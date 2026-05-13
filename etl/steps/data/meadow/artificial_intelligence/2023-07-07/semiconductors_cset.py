"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("semiconductors_cset.csv")
    tb = snap.read_csv()

    # Keep relevant columns.
    tb = tb[["provider_name", "provided_name", "share_provided", "provided_id"]]

    # Strip the trailing % sign so the column is numeric-ready.
    tb["share_provided"] = tb["share_provided"].str.replace("%", "")

    tb = tb.rename(columns={"provider_name": "country"})

    # Keep only the three top-level supply-chain stages.
    tb = tb[tb["provided_id"].isin(["S1", "S2", "S3"])]
    tb = tb.dropna(subset=["share_provided"]).reset_index(drop=True)

    tb = tb.format(["country", "provided_name"], short_name=paths.short_name)

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
