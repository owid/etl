"""
Minimal Grapher DB upsert — bridge between lightweight ETL and MySQL.

This is a proof-of-concept. The heavyweight ETL does much more (threading,
S3 uploads, admin API, ghost cleanup, checksums). This just upserts a
dataset row.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pymysql


def _connect(env_path: str | None = None) -> pymysql.Connection:
    """Connect to the local Grapher MySQL using .env credentials."""
    import os

    from dotenv import load_dotenv

    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    return pymysql.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
        autocommit=False,
    )


def upsert_dataset(
    meta: dict,
    short_name: str,
    *,
    namespace: str = "owid",
    version: str = "",
    user_id: int = 1,
    env_path: str | None = None,
) -> int:
    """Upsert a dataset row in the Grapher DB. Returns the dataset id.

    Args:
        meta: Dataset metadata dict (from .meta.yml "dataset" section).
              Uses keys: title, description, source, tags.
        short_name: Dataset short name (e.g. "cherry_blossom").
        namespace: Grapher namespace. Default "owid".
        version: Dataset version string.
        user_id: Grapher user id for createdBy/editedBy.
        env_path: Path to .env file with DB creds. Default: auto-discover.

    Returns:
        The dataset id (existing or newly created).
    """
    conn = _connect(env_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    try:
        cur = conn.cursor()

        # Ensure namespace exists
        cur.execute(
            "INSERT INTO namespaces (name, description) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name=name",
            (namespace, ""),
        )

        # Build catalog path
        catalog_path = f"grapher/{namespace}/{version}/{short_name}" if version else f"grapher/{namespace}/{short_name}"

        title = meta.get("title", short_name)
        description = meta.get("description", "")

        # Check if dataset exists
        cur.execute(
            "SELECT id FROM datasets WHERE shortName = %s AND namespace = %s",
            (short_name, namespace),
        )
        row = cur.fetchone()

        if row:
            dataset_id = row[0]
            cur.execute(
                """UPDATE datasets SET
                    name = %s,
                    description = %s,
                    updatedAt = %s,
                    metadataEditedAt = %s,
                    metadataEditedByUserId = %s,
                    dataEditedAt = %s,
                    dataEditedByUserId = %s,
                    version = %s,
                    catalogPath = %s
                WHERE id = %s""",
                (title, description, now, now, user_id, now, user_id, version, catalog_path, dataset_id),
            )
        else:
            cur.execute(
                """INSERT INTO datasets
                    (name, description, namespace, shortName, version, catalogPath,
                     createdAt, updatedAt, createdByUserId,
                     metadataEditedAt, metadataEditedByUserId,
                     dataEditedAt, dataEditedByUserId,
                     isPrivate, nonRedistributable, isArchived)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0)""",
                (
                    title,
                    description,
                    namespace,
                    short_name,
                    version,
                    catalog_path,
                    now,
                    now,
                    user_id,
                    now,
                    user_id,
                    now,
                    user_id,
                ),
            )
            dataset_id = cur.lastrowid

        conn.commit()
        return dataset_id

    finally:
        conn.close()
