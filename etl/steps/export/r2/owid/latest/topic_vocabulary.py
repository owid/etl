"""Publish the topic vocabulary: the suggested search terms per topic, read by the site.

`topic_vocabulary.json` next to this file is the full output of
`scripts/vocabulary/vocabulary.py`, stats included, committed so that runs can be
compared in git. This step uploads the part the site reads to
https://files.ourworldindata.org/topic_vocabulary.json. Because that file counts
towards the step's checksum, merging a new vocabulary is what publishes it, and the
live file is always the one on master.
"""

import json
import tempfile
from pathlib import Path

import structlog
from owid.catalog import s3_utils

from etl import config

log = structlog.get_logger()

# The full vocabulary, as the script writes it.
VOCABULARY_FILE = Path(__file__).with_suffix(".json")

S3_BUCKET_NAME = "owid-public"
# The key the site reads, through the files.ourworldindata.org worker (which adds CORS and an edge cache).
PRODUCTION_KEY = "topic_vocabulary.json"

# Per-topic fields in the published file: what the site's consumers read (the
# all-charts block reads `keywords` and `countries`, search reranking `keywords`
# and `topic_name`), plus `topic_slug`, so an entry still says which topic it is
# once taken out of the file.
# The stats stay in the committed file, where they're read when tuning, instead of
# making up most of every page load.
PUBLISHED_FIELDS = ("topic_slug", "topic_name", "keywords", "countries")


def published_vocabulary(vocabulary: dict) -> dict:
    """The part of the full vocabulary the site reads."""
    sanity_check(vocabulary)
    return {slug: {field: entry[field] for field in PUBLISHED_FIELDS} for slug, entry in vocabulary.items()}


def upload(published: dict, key: str) -> str:
    """Upload the published vocabulary to `key` in the public bucket and return its public URL."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "topic_vocabulary.json"
        tmp_path.write_text(json.dumps(published, separators=(",", ":")))
        # Without a Content-Type R2 serves the object untyped, and Cloudflare
        # compresses only typed responses, so this is what gets it gzip/brotli.
        s3_utils.upload(f"s3://{S3_BUCKET_NAME}/{key}", tmp_path, public=True, content_type="application/json")
    return f"https://files.ourworldindata.org/{key}"


def sanity_check(vocabulary: dict) -> None:
    # The file replaces the live vocabulary, so a broken one would empty the suggestions of every topic.
    assert vocabulary, "The topic vocabulary is empty."
    for slug, entry in vocabulary.items():
        assert entry.get("topic_slug") == slug, f"Entry under '{slug}' says it is '{entry.get('topic_slug')}'."
        assert entry.get("topic_name"), f"Topic '{slug}' has no topic_name."
        # An empty list is legitimate (no candidate term covered anything, no country stood out), and the
        # site then shows none.
        for field in ("keywords", "countries"):
            values = entry.get(field)
            assert isinstance(values, list) and all(isinstance(v, str) and v for v in values), (
                f"Topic '{slug}' has {field} that aren't a list of non-empty strings."
            )


def run(dest_dir: str) -> None:
    # Build the published file.
    published = published_vocabulary(json.loads(VOCABULARY_FILE.read_text()))
    (Path(dest_dir) / "topic_vocabulary.json").write_text(json.dumps(published, indent=2))

    # Upload it, only with --export: a staging or local build must not overwrite the live file.
    if not config.EXPORT_ENABLED:
        log.info("topic_vocabulary.upload_skipped", hint="pass --export to publish", topics=len(published))
        return
    url = upload(published, PRODUCTION_KEY)
    log.info("topic_vocabulary.uploaded", url=url, topics=len(published))
