"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("child_mortality.feather")
    tb = snap.read_feather()

    tb = tb.drop(
        columns=["index", "location_id", "sex_id", "age_group_id", "measure_id", "metric_id", "upper", "lower"]
    )
    tb = tb.rename(columns={"location_name": "country", "year_id": "year", "val": "value"})
    tb = tb.drop_duplicates()

    tb = tb.format(
        ["country", "year", "sex", "age_group_name", "measure_name", "metric_name"],
        short_name="child_mortality",
    )

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
