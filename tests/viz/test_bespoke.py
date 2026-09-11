"""Tests for the framework half of `viz://bespoke` steps."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from owid.catalog import Origin, Table, VariableMeta, VariablePresentationMeta

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
    # Inferred from the values, as a grapher upsert would.
    assert api["type"] == "float"
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


def test_build_feed_metadata(table):
    metadata = bespoke.build_feed_metadata(
        title="Causes of death",
        columns={"Deaths": table["value"]},
        update_period_days=1460,
        build_date=date(2026, 9, 11),
    )

    assert metadata["feed"]["title"] == "Causes of death"
    # The source line grapher would put under a chart built on this column, rather than a string
    # typed into the step.
    assert metadata["feed"]["citation"] == "IHME, Global Burden of Disease (2024)"
    assert metadata["dateGenerated"] == "2026-09-11"

    column = metadata["columns"]["Deaths"]
    assert column["titleShort"] == "Deaths"
    assert column["unit"] == "deaths"
    assert column["lastUpdated"] == "2025-10-21"
    assert column["citationShort"].endswith("with minor processing by Our World in Data")
    # No variable in the grapher DB to point at.
    assert "owidVariableId" not in column
    assert "fullMetadata" not in column


def test_write_feed_metadata(tmp_path, table):
    metadata = bespoke.build_feed_metadata("Causes of death", {"Deaths": table["value"]})
    path = bespoke.write_feed_metadata(tmp_path / "feed", metadata)

    assert path.name == "metadata.json"
    assert pd.read_json(path).index.tolist()  # parses as JSON


class _FakeClient:
    def __init__(self):
        self.deleted: list[str] = []

    def delete_objects(self, Bucket: str, Delete: dict) -> None:
        self.deleted += [obj["Key"] for obj in Delete["Objects"]]


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
