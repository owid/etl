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
