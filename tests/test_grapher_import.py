import json
from types import SimpleNamespace

import pandas as pd
import pytest
import requests
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


def test_calculate_checksum_metadata():
    meta = _get_metadata()
    df = _get_data()
    ds_meta = _get_dataset_metadata()

    # Checksum should be deterministic
    checksum = db.calculate_checksum_metadata(meta, df, ds_meta)
    assert checksum == db.calculate_checksum_metadata(meta, df, ds_meta)

    # Different metadata should produce different checksums
    meta2 = VariableMeta(
        origins=[Origin(title="Different", producer="Producer")],
        presentation=VariablePresentationMeta(title_public="Title public"),
    )
    assert checksum != db.calculate_checksum_metadata(meta2, df, ds_meta)


def test_calculate_checksum_metadata_depends_on_dataset_fields():
    """Dataset-level fields are embedded in the indicator JSON, so they must flip the checksum.

    Regression: they used to be invisible to the checksum, so clearing e.g. `update_period_days`
    updated MySQL but never re-uploaded the JSON files in R2, which stayed stale indefinitely.
    """
    meta = _get_metadata()
    df = _get_data()
    checksum = db.calculate_checksum_metadata(meta, df, _get_dataset_metadata())

    # Clearing update_period_days drops it from the JSON (and from the hashed dict).
    cleared = {k: v for k, v in _get_dataset_metadata().items() if k != "updatePeriodDays"}
    assert checksum != db.calculate_checksum_metadata(meta, df, cleared)

    # Same for the other fields the JSON embeds.
    for field, value in [("datasetName", "Renamed"), ("datasetVersion", "2025-01-01"), ("nonRedistributable", True)]:
        assert checksum != db.calculate_checksum_metadata(meta, df, {**_get_dataset_metadata(), field: value})


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
    ds_meta = _get_dataset_metadata()
    assert db.calculate_checksum_metadata(meta_with_empties, df, ds_meta) == db.calculate_checksum_metadata(
        meta_without_empties, df, ds_meta
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


class _FakeAdminAPI:
    """Records the calls a cleanup makes and replays canned Admin API responses.

    A response may be an exception instance, which is raised instead of returned.
    """

    owid_env = SimpleNamespace(admin_api="http://localhost:3030/admin/api")

    def __init__(self, responses: list[dict | Exception]) -> None:
        self._responses = list(responses)
        self.calls: list[dict] = []

    def cleanup_ghost_variables(self, dataset_id: int, keep_variable_ids: list[int]) -> dict:
        self.calls.append({"dataset_id": dataset_id, "keep": keep_variable_ids})
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _blocked(variable_id: int, chart_id: int) -> dict:
    return {
        "variableId": variable_id,
        "variableName": f"Variable {variable_id}",
        "chartId": chart_id,
        "chartSlug": f"chart-{chart_id}",
    }


def _response(deleted=(), blocked=()) -> dict:
    return {"deleted": list(deleted), "blocked": list(blocked)}


def test_cleanup_ghost_variables_deletes_in_one_call():
    admin_api = _FakeAdminAPI([_response(deleted=[7])])

    assert db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]

    assert admin_api.calls == [{"dataset_id": 1, "keep": [5]}]


def test_cleanup_ghost_variables_is_a_no_op_when_there_are_no_ghosts():
    admin_api = _FakeAdminAPI([_response()])

    assert db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]


def test_cleanup_ghost_variables_warns_in_production_when_a_chart_still_uses_one(monkeypatch):
    monkeypatch.setattr(db.config, "ENV", "production")
    admin_api = _FakeAdminAPI([_response(deleted=[7], blocked=[_blocked(8, 100), _blocked(8, 101)])])

    # Not successful, so the checksum stays unset and the next run tries again.
    assert not db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]


def test_cleanup_ghost_variables_raises_outside_production(monkeypatch):
    monkeypatch.setattr(db.config, "ENV", "dev")
    admin_api = _FakeAdminAPI([_response(blocked=[_blocked(8, 100)])])

    with pytest.raises(ValueError, match="chart-100"):
        db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]


def test_cleanup_ghost_variables_warns_when_admin_api_is_unreachable(monkeypatch):
    """Working locally without a running admin: warn and let a later run clean up."""
    monkeypatch.setattr(db.config, "ENV", "dev")
    admin_api = _FakeAdminAPI([requests.exceptions.ConnectionError("connection refused")])

    assert not db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("env", ["staging", "production"])
def test_cleanup_ghost_variables_raises_when_a_deployed_admin_is_unreachable(monkeypatch, env):
    """A deployed environment always has an admin, so an unreachable one is an outage."""
    monkeypatch.setattr(db.config, "ENV", env)
    admin_api = _FakeAdminAPI([requests.exceptions.ConnectionError("connection refused")])

    with pytest.raises(requests.exceptions.ConnectionError):
        db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]


def test_cleanup_ghost_variables_does_not_swallow_other_admin_api_errors():
    admin_api = _FakeAdminAPI([requests.exceptions.HTTPError("500 Server Error")])

    with pytest.raises(requests.exceptions.HTTPError):
        db.cleanup_ghost_variables(admin_api, dataset_id=1, upserted_variable_ids=[5])  # ty: ignore[invalid-argument-type]


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


def test_variable_payload_sends_entity_ids_not_names():
    """Grapher resolves entity names and codes itself, so the payload carries ids only."""
    prepared = db.prepare_variable(
        _variable_table(),
        db.DatasetUpsertResult(1, _get_dataset_metadata()),
        catalog_path="grapher/ns/2026-01-01/ds/tb#gdp",
        checksums={},
    )
    assert prepared is not None

    assert prepared.payload["entityIds"] == [1, 3]
    assert prepared.payload["years"] == [2000, 2001]
    assert "entityName" not in json.dumps(prepared.payload)


def test_variable_payload_carries_the_inferred_type():
    """Grapher never sees the values, so it can't work the type out for itself."""
    prepared = db.prepare_variable(
        _variable_table(),
        db.DatasetUpsertResult(1, _get_dataset_metadata()),
        catalog_path="grapher/ns/2026-01-01/ds/tb#gdp",
        checksums={},
    )
    assert prepared is not None
    assert prepared.payload["type"]


def test_variable_payload_sends_description_key_as_a_string():
    prepared = db.prepare_variable(
        _variable_table(),
        db.DatasetUpsertResult(1, _get_dataset_metadata()),
        catalog_path="grapher/ns/2026-01-01/ds/tb#gdp",
        checksums={},
    )
    assert prepared is not None
    assert prepared.payload["descriptionKey"] == "- A point\n- Another point"


def test_prepare_variable_skips_when_both_checksums_match():
    args = (
        _variable_table(),
        db.DatasetUpsertResult(1, _get_dataset_metadata()),
    )
    kwargs = {"catalog_path": "grapher/ns/2026-01-01/ds/tb#gdp"}
    first = db.prepare_variable(*args, checksums={}, **kwargs)
    assert first is not None

    again = db.prepare_variable(
        *args,
        checksums={"dataChecksum": first.checksum_data, "metadataChecksum": first.checksum_metadata},
        **kwargs,
    )
    assert again is None
