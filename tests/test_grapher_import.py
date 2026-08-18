import json

import pandas as pd
import pytest
from owid.catalog import Origin, Table, VariableMeta, VariablePresentationMeta

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


def _get_dataset_metadata():
    return {"datasetName": "Dataset", "datasetVersion": "2024-12-31", "updatePeriodDays": 365}


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


def _variable_table() -> Table:
    tb = Table(
        {
            "entityId": [1, 1, 3],
            "entityCode": ["FRA", "FRA", "DEU"],
            "entityName": ["France", "France", "Germany"],
            "year": [2000, 2001, 2000],
            "gdp": [1.0, 2.0, 3.0],
        }
    )
    tb["gdp"].metadata = VariableMeta(  # ty: ignore[unresolved-attribute]
        title="GDP",
        unit="US$",
        description_key="- A point\n- Another point",
        origins=[Origin(title="Title", producer="Producer", date_accessed="2026-01-01")],  # ty: ignore[invalid-argument-type]
        presentation=VariablePresentationMeta(title_public="Title public", topic_tags=["Energy"]),
    )
    return tb.set_index(["entityId", "entityCode", "entityName", "year"])


def _prepare():
    return db.prepare_indicator(_variable_table(), catalog_path="grapher/ns/2026-01-01/ds/tb#gdp")


def test_indicator_payload_sends_entity_ids_not_names():
    """Grapher resolves entity names and codes itself, so the payload carries ids only."""
    payload = _prepare().payload

    assert payload["entityIds"] == [1, 3]
    assert payload["years"] == [2000, 2001]
    assert "entityName" not in json.dumps(payload)


def test_indicator_payload_carries_what_only_the_values_can_give():
    """Grapher never sees the values, so it can't work these out for itself."""
    payload = _prepare().payload

    assert payload["type"]
    assert payload["timespan"] == "2000-2001"
    assert payload["dataChecksum"]


def test_indicator_payload_sends_description_key_as_a_string():
    assert _prepare().payload["descriptionKey"] == "- A point\n- Another point"


def test_indicator_metadata_is_always_sent():
    """No metadata checksum here — Grapher compares against what it published."""
    payload = _prepare().payload

    assert "metadataChecksum" not in payload
    assert payload["name"] == "GDP"


def _blocked(catalog_path: str, chart_id: int) -> dict:
    return {
        "catalogPath": catalog_path,
        "charts": [{"id": chart_id, "slug": f"chart-{chart_id}"}],
    }


def test_blocked_indicators_warn_in_production(monkeypatch):
    monkeypatch.setattr(db.config, "ENV", "production")

    # Not allowed to proceed, so the checksum stays unset and the next run tries again.
    assert not db.blocked_indicators_allow_run([_blocked("ns/ds/tb#old", 100)])


def test_blocked_indicators_raise_outside_production(monkeypatch):
    monkeypatch.setattr(db.config, "ENV", "dev")

    with pytest.raises(ValueError, match="chart-100"):
        db.blocked_indicators_allow_run([_blocked("ns/ds/tb#old", 100)])


def test_no_blocked_indicators_is_fine():
    assert db.blocked_indicators_allow_run([])
