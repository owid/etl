import pandas as pd
from owid.catalog import Origin, VariableMeta, VariablePresentationMeta

from etl import grapher_import as gi


def _get_data():
    return pd.DataFrame({"entityId": [1, 1, 3], "year": [2000, 2001, 2000], "value": ["1", "2", "3"]})


def _get_metadata():
    return VariableMeta(
        origins=[Origin(title="Title", producer="Producer")],
        presentation=VariablePresentationMeta(title_public="Title public"),
    )


def test_calculate_checksum_data():
    df = _get_data()

    assert gi.calculate_checksum_data(df) == "3523058000783533578"

    # it is invariant to ordering
    assert gi.calculate_checksum_data(df.iloc[::-1]) == "3523058000783533578"


def test_calculate_checksum_metadata():
    meta = _get_metadata()
    df = _get_data()

    assert gi.calculate_checksum_metadata(meta, df) == "-4368982562562216097"
