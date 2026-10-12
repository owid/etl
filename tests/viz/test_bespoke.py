"""Tests for the framework half of `viz://bespoke` steps."""

from pathlib import Path

import pytest
from owid.catalog import License, Origin, Table, VariableMeta, VariablePresentationMeta

from etl import config
from etl.viz import bespoke

ORIGIN = Origin(
    producer="IHME, Global Burden of Disease",
    title="Global Burden of Disease",
    citation_full="Global Burden of Disease Collaborative Network (2024).",
    attribution_short="IHME-GBD",
    url_main="https://vizhub.healthdata.org/gbd-results/",
    date_accessed="2025-10-21",
    date_published="2024-05-17",
)


@pytest.fixture
def table() -> Table:
    tb = Table({"country": ["France"], "year": [2020], "value": [1.5]})
    tb._fields["value"] = VariableMeta(
        title="Deaths",
        unit="deaths",
        short_unit="",
        description_short="Number of deaths.",
        processing_level="minor",
        origins=[ORIGIN],
        presentation=VariablePresentationMeta(topic_tags=["Causes of Death"]),
    )
    return tb


def test_feed_location_splits_by_environment(monkeypatch):
    monkeypatch.setattr(config, "DATA_API_ENV", "production")
    location = bespoke.feed_location("bespoke/ihme_gbd/latest/gbd_treemap_json")
    assert location.s3_folder == "s3://owid-api/v1/bespoke/ihme_gbd/latest/gbd_treemap_json"
    assert location.public_url == "https://api.ourworldindata.org/v1/bespoke/ihme_gbd/latest/gbd_treemap_json"

    monkeypatch.setattr(config, "DATA_API_ENV", "staging-site-my-branch")
    location = bespoke.feed_location("bespoke/ihme_gbd/latest/gbd_treemap_json")
    assert location.s3_folder == (
        "s3://owid-api-staging/staging-site-my-branch/v1/bespoke/ihme_gbd/latest/gbd_treemap_json"
    )
    assert location.public_url == (
        "https://api-staging.owid.io/staging-site-my-branch/v1/bespoke/ihme_gbd/latest/gbd_treemap_json"
    )


def test_variable_meta_to_api_dict(table):
    api = bespoke.variable_meta_to_api_dict(table["value"], update_period_days=365)

    assert api["name"] == "Deaths"
    assert api["shortName"] == "value"
    assert api["descriptionShort"] == "Number of deaths."
    assert api["processingLevel"] == "minor"
    assert api["updatePeriodDays"] == 365
    assert api["origins"][0]["producer"] == "IHME, Global Burden of Disease"
    assert api["origins"][0]["citationFull"] == "Global Burden of Disease Collaborative Network (2024)."
    # Grapher-only presentation fields don't travel with a feed.
    assert "topicTags" not in api["presentation"]


def test_variable_meta_drops_unrendered_templates(table):
    """A long garden table's metadata is still templated -- it is rendered per dimension only when a
    grapher step builds wide tables -- so template text must not reach the feed."""
    table._fields["value"].title = "<% if metric == 'Number' %>Deaths<% endif %>"
    table._fields["value"].display = {"name": "<< cause >>", "numDecimalPlaces": 0}

    api = bespoke.variable_meta_to_api_dict(table["value"], default_title="Deaths")

    # The title the step gave the column stands in, so the citation doesn't read `"" [dataset]`.
    assert api["name"] == "Deaths"
    assert api["display"] == {"numDecimalPlaces": 0}


def test_variable_meta_drops_templated_list_items(table):
    """`description_key` is a list, and gbd_treemap templates one of its bullets -- an unfiltered
    list would be joined into markdown and published with the template text in it."""
    table._fields["value"].description_key = [
        "Deaths are counted by underlying cause.",
        "<% if cause == 'Conflict and terrorism' %>IHME splits conflict deaths evenly.<% endif %>",
    ]

    api = bespoke.variable_meta_to_api_dict(table["value"])

    assert api["descriptionKey"] == ["Deaths are counted by underlying cause."]


def test_variable_meta_drops_a_fully_templated_list(table):
    table._fields["value"].description_key = ["<% if cause == 'X' %>only bullet<% endif %>"]

    api = bespoke.variable_meta_to_api_dict(table["value"])

    assert "descriptionKey" not in api


def test_build_feed_metadata(table):
    table._fields["value"].description_key = ["Deaths are counted by underlying cause.", "Includes all ages."]

    metadata = bespoke.build_feed_metadata(
        title="Causes of death",
        columns={"Deaths": table["value"]},
        update_period_days=1460,
    )

    # Grapher's `BespokeMetadata` fields, at the top level.
    assert metadata["title"] == "Causes of death"
    # The source line grapher would put under a chart built on this column, rather than a string
    # typed into the step.
    assert metadata["attribution"] == "IHME, Global Burden of Disease (2024)"
    assert metadata["origins"][0]["producer"] == "IHME, Global Burden of Disease"
    assert metadata["updatePeriodDays"] == 1460
    # A single-column feed also gets the column's description and unit.
    assert metadata["descriptionShort"] == "Number of deaths."
    assert metadata["descriptionKey"] == "- Deaths are counted by underlying cause.\n- Includes all ages."
    assert metadata["processingLevel"] == "minor"
    assert metadata["unit"] == "deaths"
    # An empty short unit is left out rather than published as "".
    assert "shortUnit" not in metadata


def test_build_feed_metadata_of_several_columns(table):
    table["population"] = 67e6
    table._fields["population"] = VariableMeta(title="Population", unit="people", origins=[ORIGIN])

    metadata = bespoke.build_feed_metadata(
        title="Causes of death",
        columns={"Deaths": table["value"], "Population": table["population"]},
    )

    # Origins shared by the columns are listed once.
    assert len(metadata["origins"]) == 1
    # No single description or unit to show for a feed combining several columns.
    assert "unit" not in metadata
    assert "descriptionShort" not in metadata
    # The processing level combines: one minor column and one without a level is minor.
    assert metadata["processingLevel"] == "minor"


def test_build_feed_metadata_keeps_a_license_without_url(table):
    """Grapher's schema requires `license.url`, and rejects the whole file when it is missing."""
    origin = Origin(producer="P", title="T", license=License(name="CC BY 4.0"))
    table._fields["value"].origins = [origin]

    metadata = bespoke.build_feed_metadata("Causes of death", {"Deaths": table["value"]})

    assert metadata["origins"][0]["license"] == {"name": "CC BY 4.0", "url": ""}


def test_build_feed_metadata_refuses_a_license_without_name(table):
    table._fields["value"].origins = [Origin(producer="P", title="T", license=License(url="https://p.org/terms"))]

    with pytest.raises(ValueError, match="no name"):
        bespoke.build_feed_metadata("Causes of death", {"Deaths": table["value"]})


def test_add_feed_metadata(table):
    metadata = bespoke.build_feed_metadata("Causes of death", {"Deaths": table["value"]})

    merged = bespoke.add_feed_metadata({"dimensions": {}, "source": "x"}, metadata)
    assert merged["dimensions"] == {}
    assert merged["title"] == "Causes of death"

    with pytest.raises(ValueError, match="title"):
        bespoke.add_feed_metadata({"title": "Manifest title"}, metadata)


class _FakeClient:
    def __init__(self, delete_errors: list[dict] | None = None):
        self.deleted: list[str] = []
        self.delete_errors = delete_errors or []

    def delete_objects(self, Bucket: str, Delete: dict) -> dict:
        self.deleted += [obj["Key"] for obj in Delete["Objects"]]
        # S3 reports a per-key failure in the body of an otherwise successful response.
        return {"Errors": self.delete_errors}


def test_sync_feed_uploads_and_deletes_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_API_ENV", "production")
    local = tmp_path / "gbd_treemap_json"
    local.mkdir()
    (local / "causes-of-death.metadata.json").write_text("{}")
    (local / "causes-of-death.1.json").write_text("{}")
    # The dataset index is etl's own bookkeeping and must not reach the bucket.
    (local / "index.json").write_text("{}")

    prefix = "v1/bespoke/ihme_gbd/latest/gbd_treemap_json"
    client = _FakeClient()
    uploaded: list[str] = []
    monkeypatch.setattr(bespoke.s3_utils, "connect_r2", lambda: client)
    monkeypatch.setattr(bespoke.s3_utils, "upload", lambda url, path, **kwargs: uploaded.append(url))
    monkeypatch.setattr(
        bespoke.s3_utils,
        "list_s3_objects",
        # An entity file from a previous run that this one no longer produces.
        lambda folder, client=None: [
            f"{prefix}/causes-of-death.metadata.json",
            f"{prefix}/causes-of-death.1.json",
            f"{prefix}/causes-of-death.999.json",
        ],
    )

    location = bespoke.sync_feed("bespoke/ihme_gbd/latest/gbd_treemap_json", local)

    assert location.public_url == f"https://api.ourworldindata.org/{prefix}"
    assert sorted(uploaded) == [
        f"s3://owid-api/{prefix}/causes-of-death.1.json",
        f"s3://owid-api/{prefix}/causes-of-death.metadata.json",
    ]
    assert client.deleted == [f"{prefix}/causes-of-death.999.json"]


def test_sync_feed_refuses_an_empty_folder(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(AssertionError, match="produced no files"):
        bespoke.sync_feed("bespoke/ihme_gbd/latest/gbd_treemap_json", empty)


def test_output_dir_of_a_bespoke_step():
    from etl import paths as etl_paths
    from etl.helpers import PathFinder

    step_file = etl_paths.STEP_DIR / "viz/bespoke/ihme_gbd/latest/gbd_treemap_json.py"
    assert PathFinder(str(step_file)).output_dir == Path(etl_paths.VIZ_DIR / "bespoke/ihme_gbd/latest/gbd_treemap_json")


def test_sync_feed_raises_when_a_stale_object_cannot_be_deleted(tmp_path, monkeypatch):
    """A per-key delete failure arrives inside a 200 response. Unnoticed, the step would record
    its checksum and go on serving the stale file until something else made it dirty."""
    local = tmp_path / "gbd_treemap_json"
    local.mkdir()
    (local / "causes-of-death.metadata.json").write_text("{}")

    prefix = "v1/bespoke/ihme_gbd/latest/gbd_treemap_json"
    client = _FakeClient(delete_errors=[{"Key": f"{prefix}/causes-of-death.999.json", "Code": "AccessDenied"}])
    monkeypatch.setattr(bespoke.s3_utils, "connect_r2", lambda: client)
    monkeypatch.setattr(bespoke.s3_utils, "upload", lambda url, path, **kwargs: None)
    monkeypatch.setattr(
        bespoke.s3_utils,
        "list_s3_objects",
        lambda folder, client=None: [
            f"{prefix}/causes-of-death.metadata.json",
            f"{prefix}/causes-of-death.999.json",
        ],
    )

    with pytest.raises(RuntimeError, match="Failed to delete 1 stale object"):
        bespoke.sync_feed("bespoke/ihme_gbd/latest/gbd_treemap_json", local)
