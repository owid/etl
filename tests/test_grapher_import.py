import pandas as pd
from owid.catalog import Origin, VariableMeta, VariablePresentationMeta

import etl.grapher.to_db as db


def _get_data():
    return pd.DataFrame({"entityId": [1, 1, 3], "year": [2000, 2001, 2000], "value": ["1", "2", "3"]})


def _get_metadata():
    return VariableMeta(
        origins=[Origin(title="Title", producer="Producer")],
        presentation=VariablePresentationMeta(title_public="Title public"),
    )


def test_calculate_checksum_data():
    df = _get_data()

    assert db.calculate_checksum_data(df) == "3523058000783533578"

    # it is invariant to ordering
    assert db.calculate_checksum_data(df.iloc[::-1]) == "3523058000783533578"


def test_calculate_checksum_metadata():
    meta = _get_metadata()
    df = _get_data()

    # Checksum should be deterministic
    checksum = db.calculate_checksum_metadata(meta, df)
    assert checksum == db.calculate_checksum_metadata(meta, df)

    # Different metadata should produce different checksums
    meta2 = VariableMeta(
        origins=[Origin(title="Different", producer="Producer")],
        presentation=VariablePresentationMeta(title_public="Title public"),
    )
    assert checksum != db.calculate_checksum_metadata(meta2, df)


def test_calculate_checksum_metadata_invariant_to_empty_field_shapes():
    """Empty list / None / missing-from-dict should all hash the same.

    Regression: removing or flipping the default of a `VariableMeta` field (e.g. the
    `sources` field dropped in #6081) used to silently flip every metadataChecksum,
    making chart-diff flag every chart as METADATA CHANGE despite no observable
    difference in the JSON. The checksum now hashes the pruned dict, so these
    invisible shape flips collapse.
    """
    df = _get_data()

    meta_with_empties = VariableMeta(
        origins=[Origin(title="T", producer="P")],
        licenses=[],
        sort=[],
    )
    meta_without_empties = VariableMeta(origins=[Origin(title="T", producer="P")])
    assert db.calculate_checksum_metadata(meta_with_empties, df) == db.calculate_checksum_metadata(
        meta_without_empties, df
    )


def test_get_timespan_yearly():
    df = pd.DataFrame({"year": [1961, 1980, 2009]})
    assert db._get_timespan(df, VariableMeta()) == "1961-2009"


def test_get_timespan_decade():
    meta = VariableMeta(display={"timeInterval": "decade"})
    # Decades coded by their first year: end snaps up to the decade's last year.
    assert db._get_timespan(pd.DataFrame({"year": [1820, 1830, 2010]}), meta) == "1820-2019"
    # Off-boundary representative years (e.g. mid-decade) snap to decade boundaries too.
    assert db._get_timespan(pd.DataFrame({"year": [1825, 1835, 2015]}), meta) == "1820-2019"


def test_get_timespan_subyearly_is_empty():
    df = pd.DataFrame({"year": [0, 28, 59]})
    for interval in ("day", "week", "month", "quarter"):
        assert db._get_timespan(df, VariableMeta(display={"timeInterval": interval})) == ""
