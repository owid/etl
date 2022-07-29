"""This module was copied from https://github.com/owid/importers/blob/master/db_utils.py"""

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

from MySQLdb.cursors import Cursor
from unidecode import unidecode

UNMODIFIED = 0
INSERT = 1
UPDATE = 2


def normalize_entity_name(entity_name: str) -> str:
    return unidecode(entity_name.lower().strip())


class NotOne(ValueError):
    pass


class DBUtils:

    # TODO create bulk inserts for every create? what type should they return?

    def __init__(self, cursor: Cursor):
        self.cursor = cursor
        self.counts = {
            "tags_inserted": 0,
            "tags_updated": 0,
            "entities_inserted": 0,
            "datasets_inserted": 0,
            "datasets_updated": 0,
            "namespaces_inserted": 0,
            "namespaces_updated": 0,
            "variables_inserted": 0,
            "variables_updated": 0,
            "sources_inserted": 0,
            "sources_updated": 0,
        }
        self.entity_id_by_normalised_name: Dict[str, int] = {}

    def get_counts(self) -> Dict[str, int]:
        return self.counts

    def get_entity_cache(self) -> Dict[str, int]:
        return self.entity_id_by_normalised_name

    def fetch_one_or_none(self, *args: Any, **kwargs: Any) -> Any:
        self.cursor.execute(*args, **kwargs)
        rows = self.cursor.fetchall()
        if len(rows) > 1:
            raise NotOne("Expected 1 or 0 rows but received %d" % (len(rows)))
        elif len(rows) == 1:
            return rows[0]
        else:
            return None

    def fetch_one(self, *args: Any, **kwargs: Any) -> Any:
        result = self.fetch_one_or_none(*args, **kwargs)
        if result is None:
            raise NotOne("Expected 1 row but received 0")
        else:
            return result

    def fetch_many(self, *args: Any, **kwargs: Any) -> List[Any]:
        self.cursor.execute(*args, **kwargs)
        return cast(List[Any], self.cursor.fetchall())

    def insert_one(self, *args: Any, **kwargs: Any) -> int:
        self.cursor.execute(*args, **kwargs)
        return int(self.cursor.lastrowid)

    def upsert_one(self, *args: Any, **kwargs: Any) -> Optional[int]:
        self.cursor.execute(*args, **kwargs)
        if self.cursor.rowcount == 0:
            return UNMODIFIED
        if self.cursor.rowcount == 1:
            return INSERT
        if self.cursor.rowcount == 2:
            return UPDATE
        return None

    def upsert_many(self, query: str, tuples: Iterable[Tuple[Any, ...]]) -> None:
        self.cursor.executemany(query, tuples)

    def execute_until_empty(self, *args: Any, **kwargs: Any) -> None:
        first = True
        while first or self.cursor.rowcount > 0:
            first = False
            self.cursor.execute(*args, **kwargs)

    def __fetch_parent_tag(self, name: str) -> int:
        (tag_id,) = self.fetch_one(
            """
            SELECT id FROM tags
            WHERE name = %s
            AND isBulkImport = TRUE
            AND parentId IS NULL
            LIMIT 1
        """,
            [name],
        )
        return cast(int, tag_id)

    def upsert_parent_tag(self, name: str) -> int:
        try:
            return self.__fetch_parent_tag(name)
        except NotOne:
            self.upsert_one(
                """
                INSERT INTO tags (name, createdAt, updatedAt, isBulkImport)
                VALUES (%s, NOW(), NOW(), TRUE)
            """,
                [name],
            )
            self.counts["tags_inserted"] += 1
            return self.__fetch_parent_tag(name)

    def upsert_tag(self, name: str, parent_id: int) -> int:
        operation = self.upsert_one(
            """
            INSERT INTO
                tags (name, parentId, createdAt, updatedAt, isBulkImport)
            VALUES
                (%s, %s, NOW(), NOW(), TRUE)
            ON DUPLICATE KEY UPDATE
                updatedAt = VALUES(updatedAt),
                isBulkImport = VALUES(isBulkImport)
        """,
            [name, parent_id],
        )

        if operation == INSERT:
            self.counts["tags_inserted"] += 1
        elif operation == UPDATE:
            self.counts["tags_updated"] += 1

        (tag_id,) = self.fetch_one(
            """
            SELECT id FROM tags
            WHERE name = %s
            AND parentId = %s
        """,
            [name, parent_id],
        )

        return cast(int, tag_id)

    def associate_dataset_tag(self, dataset_id: int, tag_id: int) -> None:
        self.upsert_one(
            """
            INSERT INTO dataset_tags
                (datasetId, tagId)
            VALUES
                (%s, %s)
            ON DUPLICATE KEY UPDATE
                tagId = VALUES(tagId)
        """,
            [dataset_id, tag_id],
        )
        # ON DUPLICATE here only avoids error, it intentionally updates nothing

    def upsert_dataset(
        self,
        name: str,
        namespace: str,
        user_id: int,
        tag_id: Optional[int] = None,
        source_checksum: Optional[str] = None,
        description: str = "This is a dataset imported by the automated fetcher",
    ) -> int:
        operation = self.upsert_one(
            """
            INSERT INTO datasets
                (name, description, namespace, sourceChecksum, createdAt, createdByUserId, updatedAt, metadataEditedAt, metadataEditedByUserId, dataEditedAt, dataEditedByUserId)
            VALUES
                (%s, %s, %s, %s, NOW(), %s, NOW(), NOW(), %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                description = VALUES(description),
                namespace = VALUES(namespace),
                sourceChecksum = VALUES(sourceChecksum),
                updatedAt = VALUES(updatedAt),
                metadataEditedAt = VALUES(metadataEditedAt),
                metadataEditedByUserId = VALUES(metadataEditedByUserId),
                dataEditedAt = VALUES(dataEditedAt),
                dataEditedByUserId = VALUES(dataEditedByUserId)
        """,
            [name, description, namespace, source_checksum, user_id, user_id, user_id],
        )
        (v,) = self.fetch_one(
            """
            SELECT id FROM datasets
            WHERE name = %s
            AND namespace = %s
        """,
            [name, namespace],
        )
        dataset_id = cast(int, v)

        if operation == INSERT:
            self.counts["datasets_inserted"] += 1
        if operation == UPDATE:
            self.counts["datasets_updated"] += 1

        if tag_id is not None:
            self.associate_dataset_tag(dataset_id, tag_id)

        return dataset_id

    def upsert_namespace(self, name: str, description: str) -> int:
        operation = self.upsert_one(
            """
            INSERT INTO namespaces
                (name, description)
            VALUES
                (%s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                description = VALUES(description)
        """,
            [name, description],
        )
        (namespace_id,) = self.fetch_one(
            """
            SELECT id FROM namespaces
            WHERE name = %s
        """,
            [name],
        )

        if operation == INSERT:
            self.counts["namespaces_inserted"] += 1
        if operation == UPDATE:
            self.counts["namespaces_updated"] += 1

        return cast(int, namespace_id)

    def upsert_source(self, name: str, description: str, dataset_id: int) -> int:
        """Upsert source into DB. If source with the same name and dataset id already exists,
        update it.
        There is no UNIQUE key constraint we can rely on to prevent duplicates
        so we have to do a SELECT before INSERT...
        """
        query = """
           SELECT id FROM sources
           WHERE name = %(name)s
             AND datasetId = %(dataset_id)s
        """
        row = self.fetch_one_or_none(
            query,
            {
                "name": name,
                "dataset_id": dataset_id,
            },
        )

        if row is None:
            self.upsert_one(
                """
                INSERT INTO sources (name, description, datasetId, createdAt, updatedAt)
                VALUES (%s, %s, %s, NOW(), NOW())
                """,
                [name, description, dataset_id],
            )
            self.counts["sources_inserted"] += 1
            row = self.fetch_one(
                query,
                {
                    "name": name,
                    "dataset_id": dataset_id,
                },
            )
        else:
            self.cursor.execute(
                """
                UPDATE sources
                SET updatedAt = NOW(),
                    description = %(description)s
                WHERE id = %(id)s
            """,
                {"description": description, "id": row[0]},
            )
            self.counts["sources_updated"] += 1

        return cast(int, row[0])

    def upsert_variable(
        self,
        name: str,
        code: Optional[str],
        unit: Optional[str],
        short_unit: Optional[str],
        source_id: int,
        dataset_id: int,
        description: Optional[str] = None,
        timespan: str = "",
        coverage: str = "",
        display: Optional[Dict[str, Any]] = None,
        original_metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        if display is None:
            display = {}
        if original_metadata is None:
            original_metadata = {}

        operation = self.upsert_one(
            """
            INSERT INTO variables (
                name, code, description, unit, shortUnit, timespan, coverage,
                display, originalMetadata, sourceId, datasetId, createdAt,
                updatedAt
            )
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                code = VALUES(code),
                description = VALUES(description),
                unit = VALUES(unit),
                shortUnit = VALUES(shortUnit),
                timespan = VALUES(timespan),
                coverage = VALUES(coverage),
                display = VALUES(display),
                originalMetadata = VALUES(originalMetadata),
                datasetId = VALUES(datasetId),
                sourceId = VALUES(sourceId),
                datasetId = VALUES(datasetId),
                updatedAt = VALUES(updatedAt)
        """,
            [
                name,
                code,
                description,
                unit,
                short_unit,
                timespan,
                coverage,
                json.dumps(display),
                json.dumps(original_metadata),
                source_id,
                dataset_id,
            ],
        )

        if operation == INSERT:
            self.counts["variables_inserted"] += 1
        elif operation == UPDATE:
            self.counts["variables_updated"] += 1

        (var_id,) = self.fetch_one(
            """
            SELECT id FROM variables
            WHERE (name = %s OR code = %s)
            AND datasetId = %s
            AND sourceId = %s
        """,
            [name, code, dataset_id, source_id],
        )

        return cast(int, var_id)

    def touch_variable(self, var_id: int) -> None:
        self.cursor.execute(
            """
            UPDATE variables
            SET updatedAt = NOW()
            WHERE id = %s
        """,
            [var_id],
        )
        self.counts["variables_updated"] += self.cursor.rowcount

    def note_import(
        self, import_type: str, import_notes: str, import_state: str
    ) -> None:
        self.upsert_one(
            """
            INSERT INTO importer_importhistory (import_type, import_time, import_notes, import_state)
            VALUES (%s, NOW(), %s, %s)
        """,
            [import_type, import_notes, import_state],
        )

    def __get_cached_entity_id(self, name: str) -> Optional[int]:
        normalised_name = normalize_entity_name(name)
        if normalised_name in self.entity_id_by_normalised_name:
            return self.entity_id_by_normalised_name[normalised_name]
        else:
            return None

    def get_or_create_entity(self, name: str) -> int:
        # Serve from cache if available
        entity_id = self.__get_cached_entity_id(name)
        if entity_id is not None:
            return entity_id
        # Populate cache from database
        self.prefill_entity_cache([name])
        entity_id = self.__get_cached_entity_id(name)
        if entity_id is not None:
            return entity_id
        # If still not in cache, it's a new entity and we have to insert it
        else:
            self.upsert_one(
                """
                INSERT INTO entities
                    (name, displayName, validated, createdAt, updatedAt)
                VALUES
                    (%s, '', FALSE, NOW(), NOW())
            """,
                [name],
            )
            self.counts["entities_inserted"] += 1
            (entity_id,) = self.fetch_one(
                """
                SELECT id FROM entities
                WHERE name = %s
            """,
                [name],
            )
            # Cache the newly created entity
            self.entity_id_by_normalised_name[normalize_entity_name(name)] = entity_id
            return cast(int, entity_id)

    def prefill_entity_cache(self, names: List[str]) -> None:
        rows = self.fetch_many(
            """
            SELECT
                LOWER(country_name),
                LOWER(entities.name),
                entities.id AS id
            FROM entities
            LEFT JOIN
                country_name_tool_countrydata
                ON country_name_tool_countrydata.owid_name = entities.name
            LEFT JOIN
                country_name_tool_countryname
                ON country_name_tool_countryname.owid_country = country_name_tool_countrydata.id
            WHERE
                LOWER(country_name) IN %(country_names)s
                OR LOWER(entities.name) IN %(country_names)s
            ORDER BY entities.id ASC
        """,
            {"country_names": [normalize_entity_name(x) for x in names]},
        )
        # Merge the two dicts
        self.entity_id_by_normalised_name.update(
            {
                # entityName → entityId
                **dict((row[1], row[2]) for row in rows if row[1]),
                # country_tool_name → entityId
                # the country tool name should take precedence
                **dict((row[0], row[2]) for row in rows if row[0]),
            }
        )
