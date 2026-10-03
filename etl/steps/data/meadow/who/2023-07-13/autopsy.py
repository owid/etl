"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = paths.load_snapshot("autopsy.csv")
    tb = snap.read_csv()

    # clean data
    tb = tb.dropna(subset="VALUE")
    tb["COUNTRY"] = tb["COUNTRY"].fillna(tb["COUNTRY_GRP"])
    tb = tb.drop(columns="COUNTRY_GRP")
    tb = tb.underscore().format(["country", "sex", "year"], short_name=paths.short_name)

    # save dataset
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
