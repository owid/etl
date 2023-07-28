"""This module was inspired by https://github.com/owid/importers/blob/master/db_utils.py. It is not meant
to be extended, but slowly replaced by etl/grapher_model.py"""

from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

import structlog
from MySQLdb.cursors import Cursor
from unidecode import unidecode

log = structlog.get_logger()

UNMODIFIED = 0
INSERT = 1
UPDATE = 2


def normalize_entity_name(entity_name: str) -> str:
    return unidecode(entity_name.strip())


class NotOne(ValueError):
    pass


class DBUtils:
    def __init__(self, cursor: Cursor):
        self.cursor = cursor
        self.entity_id_by_normalised_name: Dict[str, int] = {}

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
        self.cursor.executemany(query, list(tuples))

    def execute_until_empty(self, *args: Any, **kwargs: Any) -> None:
        first = True
        while first or self.cursor.rowcount > 0:
            first = False
            self.cursor.execute(*args, **kwargs)

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
                name,
                id
            FROM entities
            WHERE
                entities.name IN %(country_names)s
            ORDER BY entities.id ASC
        """,
            {"country_names": [normalize_entity_name(x) for x in names]},
        )
        # Merge the two dicts
        self.entity_id_by_normalised_name.update(
            {
                # entityName → entityId
                **dict((row[0], row[1]) for row in rows if row[1]),
            }
        )
