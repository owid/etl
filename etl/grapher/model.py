"""This schema was generated using https://github.com/agronholm/sqlacodegen library with the following command:
```
sqlacodegen --generator dataclasses --options use_inflect mysql://root:owid@localhost:3306/owid
```
or
```
sqlacodegen --generator dataclasses --options use_inflect mysql://owid:@staging-site-branch:3306/owid
```

If you want to add a new table to ORM, add --tables mytable to the command above.

Another option is to run `show create table mytable;` in MySQL and then ask ChatGPT to convert it to SQLAlchemy 2 ORM.

It is often necessary to add `default=None` or `init=False` to make pyright happy.

You might have to run `uv pip install mysqlclient` to install missing MySQLDb.
"""

import copy
import io
import json
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Union, get_args, overload

import humps
import numpy as np
import pandas as pd
import requests
import structlog
from deprecated import deprecated
from owid import catalog
from owid.catalog.meta import VARIABLE_TYPE
from pyarrow import feather
from sqlalchemy import (
    CHAR,
    BigInteger,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
    String,
    and_,
    func,
    or_,
    select,
    text,
)
from sqlalchemy import JSON as _JSON
from sqlalchemy.dialects.mysql import (
    ENUM,
    LONGBLOB,
    LONGTEXT,
    TEXT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (  # type: ignore
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
    relationship,
)
from sqlalchemy.sql import Select
from typing_extensions import Self, TypedDict

from etl import config, paths
from etl.db import read_sql

log = structlog.get_logger()

S3_PATH_TYP = Literal["s3", "http"]


# persist the value None as a SQL NULL value, not the JSON encoding of null
JSON = _JSON(none_as_null=True)


class Base(MappedAsDataclass, DeclarativeBase):
    __table_args__ = {"extend_existing": True}

    def dict(self) -> Dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in self.__table__.c}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """Create an object from dictionary. This method is a workaround for cls(**d)
        when you want to initialize it with `id`. Typically `id` field has init=False which
        raises an error. This method creates an object and sets the id later.

        There might be a more native way to do this, but I haven't found it yet.
        """

        set_after_init = {}
        for k, field in cls.__dataclass_fields__.items():
            if not field.init and k in d:
                set_after_init[k] = d.pop(k)

        x = cls(**d)

        for k, v in set_after_init.items():
            setattr(x, k, v)

        return x

    @classmethod
    def create_table(cls, engine: Engine, if_exists: Literal["fail", "replace", "skip"] = "fail") -> None:
        if if_exists == "replace":
            # Drop the table if it exists and create a new one
            cls.__table__.drop(engine, checkfirst=True)  # type: ignore
            cls.__table__.create(engine, checkfirst=False)  # type: ignore
        elif if_exists == "skip":
            # Create the table only if it doesn't already exist
            cls.__table__.create(engine, checkfirst=True)  # type: ignore
        elif if_exists == "fail":
            # Attempt to create the table; fail if it already exists
            cls.__table__.create(engine, checkfirst=False)  # type: ignore
        else:
            raise ValueError(f"Unrecognized value for if_exists: {if_exists}")


class HousekeeperReview(Base):
    __tablename__ = "housekeeper_reviews"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        init=False,
        # autoincrement=True,
        comment="Identifier of the review",
    )
    suggestedAt: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=True,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Date when the review was suggested",
    )
    objectType: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Type of the object to review (e.g., 'chart', 'dataset', etc.)",
    )

    objectId: Mapped[int] = mapped_column(Integer, nullable=False)

    @classmethod
    def load_reviews(cls, session: Session, object_type: Optional[str] = None) -> list["HousekeeperReview"]:
        if object_type is None:
            vars = session.scalars(select(cls)).all()
            return list(vars)
        else:
            vars = session.scalars(select(cls).where(cls.objectType == object_type)).all()
            return list(vars)

    @classmethod
    def load_reviews_object_id(cls, session: Session, object_type: str) -> list[int]:
        vars = session.scalars(select(cls.objectId).where(cls.objectType == object_type)).all()
        return list(vars)

    @classmethod
    def add_review(cls, session: Session, object_type: str, object_id: int):
        new_review = cls(objectType=object_type, objectId=object_id, suggestedAt=datetime.now(timezone.utc))
        session.add(new_review)
        session.commit()
        # return new_review


class Entity(Base):
    __tablename__ = "entities"
    __table_args__ = (Index("code", "code", unique=True), Index("name", "name", unique=True))

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    name: Mapped[str] = mapped_column(VARCHAR(255))
    validated: Mapped[int] = mapped_column(TINYINT(1))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    code: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)

    @classmethod
    def load_entity_mapping(cls, session: Session, entity_ids: Optional[List[int]] = None) -> Dict[int, str]:
        q = text(
            """
        select
            *
        from entities
        where id in :entity_ids
        """
        )
        if entity_ids is not None:
            q = text(
                """
            select
                *
            from entities
            where id in :entity_ids
            """
            )
            # Use a dictionary to pass parameters
            stm = select(Entity).from_statement(q).params(entity_ids=entity_ids)
        else:
            q = text(
                """
            select
                *
            from entities
            """
            )
            stm = select(Entity).from_statement(q)
        rows = session.execute(stm).scalars().all()

        # Convert the list of rows to a dictionary with id as key
        entity_dict = {entity.id: entity.name for entity in rows}

        return entity_dict


class Namespace(Base):
    __tablename__ = "namespaces"
    __table_args__ = (Index("namespaces_name_uq", "name", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    name: Mapped[str] = mapped_column(VARCHAR(255))
    isArchived: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'0'"), default=0)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(255), default=None)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)

    def upsert(self, session: Session) -> "Namespace":
        cls = self.__class__
        q = select(cls).where(
            cls.name == self.name,
        )
        ns = session.scalar(q)

        if ns is None:
            ns = self
        else:
            ns.description = self.description

        session.add(ns)

        # select added object to get its id
        q = select(cls).where(
            cls.name == self.name,
        )
        return session.scalars(q).one()


class Tag(Base):
    __tablename__ = "tags"
    __table_args__ = (
        ForeignKeyConstraint(["parentId"], ["tags.id"], ondelete="RESTRICT", onupdate="RESTRICT", name="tags_ibfk_1"),
        Index("dataset_subcategories_name_fk_dst_cat_id_6ce1cc36_uniq", "name", "parentId", unique=True),
        Index("parentId", "parentId"),
        Index("slug", "slug", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    name: Mapped[str] = mapped_column(VARCHAR(255))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    parentId: Mapped[Optional[int]] = mapped_column(Integer)
    specialType: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    slug: Mapped[Optional[str]] = mapped_column(VARCHAR(512))

    @classmethod
    def load_tags(cls, session: Session) -> List["Tag"]:
        return list(session.scalars(select(cls).where(cls.slug.isnot(None))).all())

    @classmethod
    def load_tags_by_names(cls, session: Session, tag_names: List[str]) -> List["Tag"]:
        """Load topic tags by their names in the order given in `tag_names`."""
        tags = session.scalars(select(Tag).where(Tag.name.in_(tag_names))).all()

        if len(tags) != len(tag_names):
            found_tags = [tag.name for tag in tags]
            missing_tags = [tag for tag in tag_names if tag not in found_tags]
            tag_names = found_tags
            log.warning("create_links.missing_tags", tags=missing_tags)

        tags = [next(tag for tag in tags if tag.name == ordered_name) for ordered_name in tag_names]
        return tags


class User(Base):
    __tablename__ = "users"
    __table_args__ = (Index("email", "email", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    isSuperuser: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'0'"))
    email: Mapped[str] = mapped_column(VARCHAR(255))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    isActive: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'1'"))
    fullName: Mapped[str] = mapped_column(VARCHAR(255))
    githubUsername: Mapped[str] = mapped_column(VARCHAR(255))
    password: Mapped[Optional[str]] = mapped_column(VARCHAR(128))
    lastLogin: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    lastSeen: Mapped[Optional[datetime]] = mapped_column(DateTime)

    @classmethod
    def load_user(cls, session: Session, id: Optional[int] = None, github_username: Optional[str] = None) -> "User":
        if id:
            return session.scalars(select(cls).where(cls.id == id)).one()
        elif github_username:
            return session.scalars(select(cls).where(cls.githubUsername == github_username)).one()
        else:
            raise ValueError("Either id or github_username must be provided")


class ChartRevisions(Base):
    __tablename__ = "chart_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["userId"], ["users.id"], ondelete="RESTRICT", onupdate="RESTRICT", name="chart_revisions_userId"
        ),
        Index("chartId", "chartId"),
        Index("chart_revisions_userId", "userId"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    chartId: Mapped[Optional[int]] = mapped_column(Integer)
    userId: Mapped[Optional[int]] = mapped_column(Integer)
    config: Mapped[Optional[dict]] = mapped_column(JSON)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)

    @classmethod
    def get_latest(cls, session: Session, chart_id: int, updatedAt=None) -> "ChartRevisions":
        """query should be: SELECT * FROM chart_revisions WHERE chartId = {self.chart_id} AND updatedAt <= '{timestamp}' ORDER BY updatedAt DESC LIMIT 1 if timestamp is given!"""
        revision = session.scalars(
            select(cls)
            .where(and_(cls.chartId == chart_id, cls.updatedAt <= updatedAt) if updatedAt else cls.chartId == chart_id)
            .order_by(cls.updatedAt.desc())
            .limit(1)
        ).one_or_none()

        if revision is None:
            raise NoResultFound()

        return revision


class ChartConfig(Base):
    __tablename__ = "chart_configs"
    __table_args__ = (Index("idx_chart_configs_slug", "slug"),)

    id: Mapped[bytes] = mapped_column(CHAR(36), primary_key=True)
    patch: Mapped[dict] = mapped_column(JSON, nullable=False)
    full: Mapped[dict] = mapped_column(JSON, nullable=False)
    fullMd5: Mapped[str] = mapped_column(CHAR(24), Computed("(to_base64(unhex(md5(full))))", persisted=True))
    slug: Mapped[Optional[str]] = mapped_column(
        String(255), Computed("(json_unquote(json_extract(`full`, '$.slug')))", persisted=True)
    )
    chartType: Mapped[Optional[str]] = mapped_column(
        String(255),
        Computed(
            "(CASE WHEN full ->> '$.chartTypes' IS NULL THEN 'LineChart' ELSE full ->> '$.chartTypes[0]' END)",
            persisted=True,
        ),
    )
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), nullable=False)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=func.current_timestamp())

    chartss: Mapped[List["Chart"]] = relationship("Chart", back_populates="chart_config")


class Chart(Base):
    __tablename__ = "charts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["configId"], ["chart_configs.id"], ondelete="RESTRICT", onupdate="RESTRICT", name="charts_configId"
        ),
        ForeignKeyConstraint(
            ["lastEditedByUserId"],
            ["users.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="charts_lastEditedByUserId",
        ),
        ForeignKeyConstraint(
            ["publishedByUserId"],
            ["users.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="charts_publishedByUserId",
        ),
        Index("charts_lastEditedByUserId", "lastEditedByUserId"),
        Index("charts_publishedByUserId", "publishedByUserId"),
        Index("configId", "configId", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    configId: Mapped[bytes] = mapped_column(CHAR(36))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    lastEditedAt: Mapped[datetime] = mapped_column(DateTime)
    lastEditedByUserId: Mapped[int] = mapped_column(Integer)
    isIndexable: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'0'"))
    _updatedAt: Mapped[datetime] = mapped_column("updatedAt", DateTime, init=False)
    publishedAt: Mapped[Optional[datetime]] = mapped_column(DateTime)
    publishedByUserId: Mapped[Optional[int]] = mapped_column(Integer)

    chart_config: Mapped["ChartConfig"] = relationship("ChartConfig", back_populates="chartss", lazy="joined")

    @hybrid_property
    def updatedAt(self) -> datetime:  # type: ignore
        # updatedAt is None if the chart is new, it only gets populated once we save it
        return self._updatedAt or self.createdAt

    @updatedAt.setter
    def updatedAt(self, value: datetime):
        self._updatedAt = value

    @hybrid_property
    def config(self) -> dict[str, Any]:  # type: ignore
        return self.chart_config.full

    @config.expression
    def config(cls):
        return select(ChartConfig.full).where(ChartConfig.id == cls.configId).scalar_subquery()

    @hybrid_property
    def slug(self) -> Optional[str]:  # type: ignore
        return self.chart_config.slug

    @slug.expression
    def slug(cls):
        return select(ChartConfig.slug).where(ChartConfig.id == cls.configId).scalar_subquery()

    @classmethod
    def load_chart(cls, session: Session, chart_id: Optional[int] = None, slug: Optional[str] = None) -> "Chart":
        """Load chart with id `chart_id`."""
        if chart_id:
            cond = cls.id == chart_id
        elif slug:
            cond = cls.slug == slug
        else:
            raise ValueError("Either chart_id or slug must be provided")
        charts = session.scalars(select(cls).where(cond)).all()

        # there can be multiple charts with the same slug, pick the published one
        if len(charts) > 1:
            charts = [c for c in charts if c.publishedAt is not None]
        elif len(charts) == 0:
            raise NoResultFound()

        return charts[0]

    @classmethod
    def load_charts(cls, session: Session, chart_ids: List[int]) -> List["Chart"]:
        """Load charts with id in `chart_ids`."""
        cond = cls.id.in_(chart_ids)
        charts = session.scalars(select(cls).where(cond)).all()

        if len(charts) == 0:
            raise NoResultFound()

        return list(charts)

    @classmethod
    def load_charts_using_variables(cls, session: Session, variable_ids: List[int]) -> List["Chart"]:
        """Load charts that use any of the given variables in `variable_ids`."""
        # Find IDs of charts
        chart_ids = (
            session.scalars(select(ChartDimensions.chartId).where(ChartDimensions.variableId.in_(variable_ids)))
            .unique()
            .all()
        )
        # Find charts
        return list(session.scalars(select(Chart).where(Chart.id.in_(chart_ids))).all())

    def load_chart_variables(self, session: Session) -> Dict[int, "Variable"]:
        q = text(
            """
        select
            v.*
        from chart_dimensions as cd
        join variables as v on v.id = cd.variableId
        where cd.chartId = :chart_id
        """
        )
        stm = select(Variable).from_statement(q).params(chart_id=self.id)
        rows = session.execute(stm).scalars().all()
        variables = {r.id: r for r in rows}

        # NOTE: columnSlug must always exist in dimensions and in chart_dimensions, so there's
        # no need to include columnSlug
        # add columnSlug if present
        # column_slug = self.config.get("map", {}).get("columnSlug")
        # if column_slug:
        #     try:
        #         variables[int(column_slug)] = Variable.load_variable(session, column_slug)
        #     except NoResultFound:
        #         raise ValueError(f"columnSlug variable {column_slug} for chart {self.id} not found")

        return variables

    @classmethod
    def load_variables_checksums(cls, session: Session, chart_ids: List[int]) -> pd.DataFrame:
        """Load checksums for all variables from the chart and return them as a list of dicts."""
        q = """
        select
            cd.chartId,
            v.catalogPath,
            v.dataChecksum,
            v.metadataChecksum
        from chart_dimensions as cd
        join variables as v on v.id = cd.variableId
        where cd.chartId in %(chart_id)s
        """
        return read_sql(q, session, params={"chart_id": chart_ids}).set_index(["chartId", "catalogPath"])

    def load_variable_checksums(self, session: Session) -> pd.DataFrame:
        """Load checksums for all variables from the chart and return them as a list of dicts."""
        q = """
        select
            v.catalogPath,
            v.dataChecksum,
            v.metadataChecksum
        from chart_dimensions as cd
        join variables as v on v.id = cd.variableId
        where cd.chartId = %(chart_id)s
        """
        return read_sql(q, session, params={"chart_id": self.id}).set_index("catalogPath")

    def migrate_config(self, source_session: Session, target_session: Session) -> Dict[str, Any]:
        """Remap variable ids from source to target session. Variable in source is uniquely identified
        by its catalogPath if available, or by name and datasetId otherwise. It is looked up
        by this identifier in the target session to get the new variable id.

        Once we get the `source variable id -> target variable id` mapping, we remap all variables in the
        chart config.
        """
        assert self.id, "Chart must come from a database"
        source_variables = self.load_chart_variables(source_session)

        remap_ids = {}
        for source_var_id, source_var in source_variables.items():
            if source_var.catalogPath:
                try:
                    target_var = Variable.from_catalog_path(target_session, source_var.catalogPath)
                except NoResultFound:
                    raise ValueError(f"variables.catalogPath not found in target: {source_var.catalogPath}")
            # old style variable, match it on name and dataset id
            else:
                try:
                    target_var = target_session.scalars(
                        select(Variable).where(
                            Variable.name == source_var.name, Variable.datasetId == source_var.datasetId
                        )
                    ).one()

                except NoResultFound:
                    raise ValueError(
                        f"variable with name `{source_var.name}` and datasetId `{source_var.datasetId}` not found in target"
                    )

            # log.debug("remap_variables", old_name=source_var.name, new_name=target_var.name)
            remap_ids[source_var_id] = target_var.id

        # copy chart as a new object
        config = copy.deepcopy(self.config)
        try:
            config = _remap_variable_ids(config, remap_ids)
        except KeyError as e:
            # This should not be happening - it means that there's a chart with a variable that doesn't exist in
            # chart_dimensions and possibly not even in variables table. It's possible that you see it admin, but
            # only because it is cached.
            raise ValueError(f"Issue with chart {self.id} - variable id not found in chart_dimensions table: {e}")

        return config

    def tags(self, session: Session) -> List[Dict[str, Any]]:
        """Return tags in a format suitable for Admin API."""
        assert self.id, "Chart must come from a database"
        q = text(
            """
        select
            tagId as id,
            t.name,
            ct.isApproved,
            ct.keyChartLevel
        from chart_tags as ct
        join tags as t on ct.tagId = t.id
        where ct.chartId = :chart_id
        """
        )
        rows = session.execute(q, params={"chart_id": self.id}).mappings().all()
        return list(map(dict, rows))

    def remove_nonexisting_map_column_slug(self, session: Session) -> None:
        """Remove map.columnSlug if the variable doesn't exist. It'd be better
        to fix the root cause and make sure we don't have such charts in our database.
        """
        column_slug = self.config.get("map", {}).get("columnSlug", None)
        if column_slug:
            try:
                Variable.load_variable(session, int(column_slug))
            except NoResultFound:
                # When there are multiple indicators in a chart and it also has a map then this field tells the map which indicator to use.
                # If the chart doesn't have the map tab active then it can be invalid quite often
                log.warning(
                    "chart_sync.remove_missing_map_column_slug",
                    chart_id=self.id,
                    column_slug=column_slug,
                )
                self.config["map"].pop("columnSlug")


class Dataset(Base):
    """Example
        {
        'id': 5357,
        'name': 'World Development Indicators - World Bank (2021.07.30)',
        'description': 'This is a dataset imported by the automated fetcher',
        'createdAt': Timestamp('2021-08-09 06:23:31'),
        'updatedAt': Timestamp('2021-08-10 01:58:59'),
        'namespace': 'worldbank_wdi@2021.07.30',
        'isPrivate': 0,
        'createdByUserId': 47,
        'metadataEditedAt': Timestamp('2021-08-10 01:58:59'),
        'metadataEditedByUserId': 47,
        'dataEditedAt': Timestamp('2021-08-10 01:58:59'),
        'dataEditedByUserId': 47,
        'nonRedistributable': 0
    }
    """

    __tablename__ = "datasets"
    __table_args__ = (
        ForeignKeyConstraint(
            ["createdByUserId"], ["users.id"], ondelete="RESTRICT", onupdate="RESTRICT", name="datasets_createdByUserId"
        ),
        ForeignKeyConstraint(
            ["dataEditedByUserId"],
            ["users.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="datasets_dataEditedByUserId",
        ),
        ForeignKeyConstraint(
            ["metadataEditedByUserId"],
            ["users.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="datasets_metadataEditedByUserId",
        ),
        Index("datasets_createdByUserId", "createdByUserId"),
        Index("datasets_dataEditedByUserId", "dataEditedByUserId"),
        Index("datasets_metadataEditedByUserId", "metadataEditedByUserId"),
        Index("datasets_catalogpath", "catalogPath", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    name: Mapped[str] = mapped_column(VARCHAR(512))
    description: Mapped[str] = mapped_column(LONGTEXT)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    namespace: Mapped[str] = mapped_column(VARCHAR(255))
    isPrivate: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'0'"))
    createdByUserId: Mapped[int] = mapped_column(Integer)
    metadataEditedByUserId: Mapped[int] = mapped_column(Integer)
    dataEditedByUserId: Mapped[int] = mapped_column(Integer)
    nonRedistributable: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'0'"))
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    shortName: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    version: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    updatePeriodDays: Mapped[Optional[int]] = mapped_column(Integer)
    dataEditedAt: Mapped[datetime] = mapped_column(DateTime, default=func.utc_timestamp())
    metadataEditedAt: Mapped[datetime] = mapped_column(DateTime, default=func.utc_timestamp())
    isArchived: Mapped[Optional[int]] = mapped_column(TINYINT(1), server_default=text("'0'"), default=0)
    sourceChecksum: Mapped[Optional[str]] = mapped_column(VARCHAR(64), default=None)
    catalogPath: Mapped[Optional[str]] = mapped_column(VARCHAR(767), default=None)
    tables: Mapped[Optional[list]] = mapped_column(JSON, default=None)

    def upsert(self, session: Session) -> "Dataset":
        cls = self.__class__
        q = select(cls).where(
            cls.catalogPath == self.catalogPath,
        )
        ds = session.scalar(q)
        if not ds:
            ds = self
        else:
            ds.name = self.name
            ds.description = self.description
            ds.metadataEditedByUserId = self.metadataEditedByUserId
            ds.dataEditedByUserId = self.dataEditedByUserId
            ds.createdByUserId = self.createdByUserId
            ds.isPrivate = self.isPrivate
            ds.updatePeriodDays = self.updatePeriodDays
            ds.nonRedistributable = self.nonRedistributable
            ds.catalogPath = self.catalogPath
            ds.tables = self.tables
            ds.updatedAt = datetime.utcnow()
            ds.metadataEditedAt = datetime.utcnow()
            ds.dataEditedAt = datetime.utcnow()

        # null checksum to label it as undone
        ds.sourceChecksum = None

        session.add(ds)
        session.flush()  # Ensure the object is written to the database and its ID is generated
        return ds

    @classmethod
    def from_dataset_metadata(
        cls, metadata: catalog.DatasetMeta, namespace: str, user_id: int, table_names: List[str]
    ) -> "Dataset":
        assert metadata.title
        return cls(
            shortName=metadata.short_name,
            name=metadata.title,
            version=metadata.version,
            namespace=namespace,
            metadataEditedByUserId=user_id,
            dataEditedByUserId=user_id,
            createdByUserId=user_id,
            description=metadata.description or "",
            isPrivate=not metadata.is_public,
            updatePeriodDays=metadata.update_period_days,
            nonRedistributable=metadata.non_redistributable,
            catalogPath=f"{namespace}/{metadata.version}/{metadata.short_name}",
            tables=table_names,
        )

    @classmethod
    def load_dataset(cls, session: Session, dataset_id: int) -> "Dataset":
        return session.scalars(select(cls).where(cls.id == dataset_id)).one()

    @classmethod
    def load_with_path(cls, session: Session, namespace: str, short_name: str, version: str) -> "Dataset":
        return session.scalars(
            select(cls).where(cls.namespace == namespace, cls.shortName == short_name, cls.version == version)
        ).one()

    @classmethod
    def load_variables_for_dataset(cls, session: Session, dataset_id: int) -> list["Variable"]:
        vars = session.scalars(select(Variable).where(Variable.datasetId == dataset_id)).all()
        assert vars, f"Dataset {dataset_id} has no variables"
        return list(vars)

    @classmethod
    def load_datasets_uri(cls, session: Session):
        query = """SELECT dataset_uri, createdAt
        FROM (
            SELECT
                namespace,
                version,
                shortName,
                createdAt,
                CONCAT('grapher/', namespace, '/', version, '/', shortName) AS dataset_uri
            FROM
                datasets d
        ) AS derived
        WHERE dataset_uri IS NOT NULL
        ORDER BY createdAt DESC;
        """
        return read_sql(query, session)

    @classmethod
    def load_all_datasets(cls, columns: Optional[list[str]] = None) -> pd.DataFrame:
        """Get all the content of the grapher `datasets` table in DB as a dataframe."""
        if not columns:
            columns = ["*"]
        return read_sql(f"select {','.join(columns)} from datasets")


class SourceDescription(TypedDict, total=False):
    link: Optional[str]
    retrievedDate: Optional[str]
    additionalInfo: Optional[str]
    dataPublishedBy: Optional[str]
    dataPublisherSource: Optional[str]


class Source(Base):
    """Example:
    {
        "id": 21261,
        "name": "OWID based on Boix et al. (2013), V-Dem (v12), and Lührmann et al. (2018)",
        "description": {
            "link": "https://sites.google.com/site/mkmtwo/data?authuser=0; http://v-dem.net/vdemds.html",
            "retrievedDate": "February 9, 2022; March 2, 2022",
            "additionalInfo": "This dataset provides information on political regimes, ...",
            "dataPublishedBy": "Our World in Data, Bastian Herre",
            "dataPublisherSource": "Data comes from Boix et al. (2013), ..."
        }
        "createdAt": "2021-11-30 15:14:13",
        "updatedAt": "2021-11-30 15:14:13",
        "datasetId": 5426
    }
    """

    __tablename__ = "sources"
    __table_args__ = (
        ForeignKeyConstraint(
            ["datasetId"], ["datasets.id"], ondelete="RESTRICT", onupdate="RESTRICT", name="sources_datasetId"
        ),
        Index("sources_datasetId", "datasetId"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    description: Mapped[SourceDescription] = mapped_column(JSON)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    name: Mapped[Optional[str]] = mapped_column(VARCHAR(512), default=None)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    datasetId: Mapped[Optional[int]] = mapped_column(Integer, default=None)

    @property
    def _upsert_select(self) -> Select:
        cls = self.__class__
        # NOTE: we match on both name and additionalInfo (source's description) so that we can
        # have sources with the same name, but different descriptions
        conds = [
            cls.name == self.name,
            cls.datasetId == self.datasetId,
            _json_is(cls.description, "additionalInfo", self.description.get("additionalInfo")),
            _json_is(cls.description, "dataPublishedBy", self.description.get("dataPublishedBy")),
        ]
        return select(cls).where(*conds)

    def upsert(self, session: Session) -> "Source":
        ds = session.scalars(self._upsert_select).one_or_none()

        if not ds:
            ds = self
        else:
            ds.updatedAt = datetime.utcnow()
            ds.description = self.description

        session.add(ds)
        session.flush()  # Ensure the object is written to the database and its ID is generated
        return ds

    @classmethod
    def from_catalog_source(cls, source: catalog.Source, dataset_id: int) -> "Source":
        if source.name is None:
            raise ValueError("Source name was None - please fix this in the metadata.")

        return Source(
            name=source.name,
            datasetId=dataset_id,
            description=SourceDescription(
                link=source.url,
                retrievedDate=source.date_accessed,
                # NOTE: published_by should be non-empty as it is shown in the Sources tab in admin
                dataPublishedBy=source.published_by or source.name,
                # NOTE: we remap `description` to additionalInfo since that is what is shown as `Description` in
                # the admin UI. Clean this up with the new data model
                additionalInfo=source.description,
            ),
        )

    @classmethod
    def load_source(cls, session: Session, source_id: int) -> "Source":
        return session.scalars(select(cls).where(cls.id == source_id)).one()

    @classmethod
    def load_sources(
        cls,
        session: Session,
        source_ids: list[int] = [],
        dataset_id: Optional[int] = None,
        variable_ids: list[int] = [],
    ) -> list["Source"]:
        """Load sources for given dataset & variable ids & source ids."""
        q = """
        select distinct * from (
            select * from sources where datasetId = %(datasetId)s
            union
            select * from sources where id in (
                select sourceId from variables where id in %(variableIds)s
            ) or id in %(sourceIds)s
        ) t
        order by t.id
        """
        sources = read_sql(
            q,
            session,
            params={
                "datasetId": dataset_id,
                # NOTE: query doesn't work with empty list so we use a dummy value
                "variableIds": variable_ids or [-1],
                "sourceIds": source_ids or [-1],
            },
        )
        sources.description = sources.description.map(json.loads)

        # sources are rarely missing datasetId (that is most likely a bug)
        if sources.datasetId.isnull().any():
            log.warning(
                "load_sources.sources_missing_datasetId",
                source_ids=sources.id[sources.datasetId.isnull()].tolist(),
            )
            sources.datasetId = sources.datasetId.fillna(dataset_id).astype(int)

        return [cls.from_dict(d) for d in sources.to_dict(orient="records")]  # type: ignore


class DimensionFilter(TypedDict):
    name: str
    value: Any


class Dimensions(TypedDict):
    originalShortName: str
    originalName: str
    filters: List[DimensionFilter]


class PostsGdocs(Base):
    __tablename__ = "posts_gdocs"
    __table_args__ = (Index("idx_posts_gdocs_type", "type"), Index("idx_updatedAt", "updatedAt"))

    id: Mapped[str] = mapped_column(VARCHAR(255), primary_key=True)
    slug: Mapped[str] = mapped_column(VARCHAR(255))
    content: Mapped[dict] = mapped_column(JSON)
    published: Mapped[int] = mapped_column(TINYINT)
    createdAt: Mapped[datetime] = mapped_column(DateTime, init=False)
    publicationContext: Mapped[str] = mapped_column(ENUM("unlisted", "listed"), server_default=text("'unlisted'"))
    type: Mapped[Optional[str]] = mapped_column(
        VARCHAR(255), Computed("(json_unquote(json_extract(`content`,_utf8mb4'$.type')))", persisted=False)
    )
    publishedAt: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    revisionId: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    manualBreadcrumbs: Mapped[Optional[dict]] = mapped_column(JSON)
    markdown: Mapped[Optional[str]] = mapped_column(LONGTEXT)


class OriginsVariablesLink(Base):
    __tablename__ = "origins_variables"
    __table_args__ = (
        ForeignKeyConstraint(["originId"], ["origins.id"], name="origins_variables_ibfk_1"),
        ForeignKeyConstraint(["variableId"], ["variables.id"], name="origins_variables_ibfk_2"),
        Index("variableId", "variableId"),
    )

    originId: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), primary_key=True)
    variableId: Mapped[int] = mapped_column(Integer, ForeignKey("variables.id"), primary_key=True)
    displayOrder: Mapped[int] = mapped_column(SmallInteger, server_default=text("'0'"))

    @classmethod
    def link_with_variable(cls, session: Session, variable_id: int, new_origin_ids: List[int]) -> None:
        """Link the given Variable ID with the given Origin IDs."""
        # Fetch current linked Origins for the given Variable ID
        existing_links = session.query(cls.originId, cls.displayOrder).filter(cls.variableId == variable_id).all()

        existing_origins = {(link.originId, link.displayOrder) for link in existing_links}
        new_origins = {(origin_id, i) for i, origin_id in enumerate(new_origin_ids)}

        # Find the Origin IDs to delete and the IDs to add
        to_delete = existing_origins - new_origins
        to_add = new_origins - existing_origins

        # Delete the obsolete Origin-Variable links
        for origin_id, display_order in to_delete:
            session.query(cls).filter(
                cls.variableId == variable_id,
                cls.originId == origin_id,
                cls.displayOrder == display_order,
            ).delete(synchronize_session="fetch")

        # Add the new Origin-Variable links
        if to_add:
            session.add_all(
                [
                    cls(originId=origin_id, variableId=variable_id, displayOrder=display_order)
                    for origin_id, display_order in to_add
                ]
            )


class PostsGdocsVariablesFaqsLink(Base):
    __tablename__ = "posts_gdocs_variables_faqs"
    __table_args__ = (
        ForeignKeyConstraint(["gdocId"], ["posts_gdocs.id"], name="posts_gdocs_variables_faqs_ibfk_1"),
        ForeignKeyConstraint(["variableId"], ["variables.id"], name="posts_gdocs_variables_faqs_ibfk_2"),
        Index("variableId", "variableId"),
    )

    gdocId: Mapped[str] = mapped_column(VARCHAR(255), primary_key=True)
    variableId: Mapped[int] = mapped_column(Integer, primary_key=True)
    fragmentId: Mapped[str] = mapped_column(VARCHAR(255), primary_key=True)
    displayOrder: Mapped[int] = mapped_column(SmallInteger, server_default=text("'0'"))

    @classmethod
    def link_with_variable(cls, session: Session, variable_id: int, new_faqs: List[catalog.FaqLink]) -> None:
        """Link the given Variable ID with Faqs"""
        # Fetch current linked Faqs for the given Variable ID
        existing_faqs = session.query(cls).filter(cls.variableId == variable_id).all()

        # Work with tuples instead
        existing_gdoc_fragment = {(f.gdocId, f.fragmentId, f.displayOrder) for f in existing_faqs}
        new_gdoc_fragment = {(f.gdoc_id, f.fragment_id, i) for i, f in enumerate(new_faqs)}

        to_delete = existing_gdoc_fragment - new_gdoc_fragment
        to_add = new_gdoc_fragment - existing_gdoc_fragment

        # Delete the obsolete links
        for gdoc_id, fragment_id, display_order in to_delete:
            session.query(cls).filter(
                cls.variableId == variable_id,
                cls.gdocId == gdoc_id,
                cls.fragmentId == fragment_id,
                cls.displayOrder == display_order,
            ).delete(synchronize_session="fetch")

        # Add the new links
        if to_add:
            session.add_all(
                [
                    cls(gdocId=gdoc_id, fragmentId=fragment_id, displayOrder=display_order, variableId=variable_id)
                    for gdoc_id, fragment_id, display_order in to_add
                ]
            )


class TagsVariablesTopicTagsLink(Base):
    __tablename__ = "tags_variables_topic_tags"
    __table_args__ = (
        ForeignKeyConstraint(["tagId"], ["tags.id"], name="tags_variables_topic_tags_ibfk_1"),
        ForeignKeyConstraint(["variableId"], ["variables.id"], name="tags_variables_topic_tags_ibfk_2"),
        Index("variableId", "variableId"),
    )

    tagId: Mapped[int] = mapped_column(Integer, primary_key=True)
    variableId: Mapped[int] = mapped_column(Integer, primary_key=True)
    displayOrder: Mapped[int] = mapped_column(SmallInteger, server_default=text("'0'"))

    @classmethod
    def link_with_variable(cls, session: Session, variable_id: int, new_tag_ids: List[int]) -> None:
        """Link the given Variable ID with the given Tag IDs."""
        assert len(new_tag_ids) == len(set(new_tag_ids)), "Tag IDs must be unique"

        # Fetch current linked tags for the given Variable ID
        existing_links = session.query(cls.tagId, cls.displayOrder).filter(cls.variableId == variable_id).all()

        existing_tags = {(link.tagId, link.displayOrder) for link in existing_links}
        new_tags = {(tag_id, i) for i, tag_id in enumerate(new_tag_ids)}

        # Find the tag IDs to delete and the IDs to add
        to_delete = existing_tags - new_tags
        to_add = new_tags - existing_tags

        # Delete the obsolete links
        for tag_id, display_order in to_delete:
            session.query(cls).filter(
                cls.variableId == variable_id,
                cls.tagId == tag_id,
                cls.displayOrder == display_order,
            ).delete(synchronize_session="fetch")

        # Add the new links
        if to_add:
            session.add_all(
                [
                    cls(tagId=tag_id, variableId=variable_id, displayOrder=display_order)
                    for tag_id, display_order in to_add
                ]
            )


class Variable(Base):
    """Example:
    {
        'id': 157342,
        'name': 'Agricultural machinery, tractors',
        'unit': '',
        'description': 'Agricultural machinery refers to the number of wheel and crawler tractors...',
        'createdAt': Timestamp('2021-08-10 01:59:02'),
        'updatedAt': Timestamp('2021-08-10 01:59:02'),
        'code': 'AG.AGR.TRAC.NO',
        'coverage': '',
        'timespan': '1961-2009',
        'datasetId': 5357,
        'sourceId': 18106,
        'shortUnit': '',
        'display': '{}',
        'columnOrder': 0,
        'originalMetadata': '{}',
    }
    """

    __tablename__ = "variables"
    __table_args__ = (
        ForeignKeyConstraint(
            ["datasetId"],
            ["datasets.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="variables_datasetId_50a98bfd_fk_datasets_id",
        ),
        ForeignKeyConstraint(
            ["sourceId"],
            ["sources.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="variables_sourceId_31fce80a_fk_sources_id",
        ),
        Index("idx_catalogPath", "catalogPath", unique=True),
        Index("variables_code_fk_dst_id_7bde8c2a_uniq", "code", "datasetId", unique=True),
        Index("variables_datasetId_50a98bfd_fk_datasets_id", "datasetId"),
        Index("idx_name_dataset", "name", "datasetId"),
        Index("variables_sourceId_31fce80a_fk_sources_id", "sourceId"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    description: Mapped[Optional[str]] = mapped_column(LONGTEXT)
    unit: Mapped[str] = mapped_column(VARCHAR(255))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    coverage: Mapped[str] = mapped_column(VARCHAR(255))
    timespan: Mapped[str] = mapped_column(VARCHAR(255))
    datasetId: Mapped[int] = mapped_column(Integer)
    display: Mapped[dict] = mapped_column(JSON)
    columnOrder: Mapped[int] = mapped_column(Integer, server_default=text("'0'"), default=0)
    schemaVersion: Mapped[int] = mapped_column(Integer, server_default=text("'1'"), default=1)
    name: Mapped[Optional[str]] = mapped_column(VARCHAR(750), default=None)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    code: Mapped[Optional[str]] = mapped_column(VARCHAR(255), default=None)
    sourceId: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    shortUnit: Mapped[Optional[str]] = mapped_column(VARCHAR(255), default=None)
    originalMetadata: Mapped[Optional[dict]] = mapped_column(JSON, default=None)
    shortName: Mapped[Optional[str]] = mapped_column(VARCHAR(255), default=None)
    catalogPath: Mapped[Optional[str]] = mapped_column(VARCHAR(767), default=None)
    dimensions: Mapped[Optional[Dimensions]] = mapped_column(JSON, default=None)
    processingLevel: Mapped[Optional[catalog.meta.PROCESSING_LEVELS]] = mapped_column(VARCHAR(30), default=None)
    processingLog: Mapped[Optional[dict]] = mapped_column(JSON, default=None)
    titlePublic: Mapped[Optional[str]] = mapped_column(VARCHAR(512), default=None)
    titleVariant: Mapped[Optional[str]] = mapped_column(VARCHAR(255), default=None)
    attributionShort: Mapped[Optional[str]] = mapped_column(VARCHAR(512), default=None)
    attribution: Mapped[Optional[str]] = mapped_column(TEXT, default=None)
    descriptionShort: Mapped[Optional[str]] = mapped_column(TEXT, default=None)
    descriptionFromProducer: Mapped[Optional[str]] = mapped_column(TEXT, default=None)
    descriptionKey: Mapped[Optional[list[str]]] = mapped_column(JSON, default=None)
    descriptionProcessing: Mapped[Optional[str]] = mapped_column(TEXT, default=None)
    # NOTE: Use of `licenses` is discouraged, they should be captured in origins.
    licenses: Mapped[Optional[list[dict]]] = mapped_column(JSON, default=None)
    # NOTE: License should be the resulting license, given all licenses of the indicator’s origins and given the indicator’s processing level.
    license: Mapped[Optional[dict]] = mapped_column(JSON, default=None)
    type: Mapped[Optional[VARIABLE_TYPE]] = mapped_column(ENUM(*get_args(VARIABLE_TYPE)), default=None)
    sort: Mapped[Optional[list[str]]] = mapped_column(JSON, default=None)
    grapherConfigIdAdmin: Mapped[Optional[str]] = mapped_column(VARCHAR(32), default=None)
    grapherConfigIdETL: Mapped[Optional[bytes]] = mapped_column(CHAR(32), default=None)
    dataChecksum: Mapped[Optional[str]] = mapped_column(VARCHAR(64), default=None)
    metadataChecksum: Mapped[Optional[str]] = mapped_column(VARCHAR(64), default=None)

    def upsert(self, session: Session) -> "Variable":
        assert self.catalogPath
        assert self.shortName

        cls = self.__class__

        q = select(cls).where(
            cls.catalogPath == self.catalogPath,
            cls.datasetId == self.datasetId,
        )
        ds = session.scalars(q).one_or_none()

        if not ds:
            ds = self
        else:
            ds.shortName = self.shortName
            ds.name = self.name
            ds.description = self.description
            ds.unit = self.unit
            ds.shortUnit = self.shortUnit
            ds.sourceId = self.sourceId
            ds.timespan = self.timespan
            ds.coverage = self.coverage
            ds.display = self.display
            ds.catalogPath = self.catalogPath
            ds.dimensions = self.dimensions
            ds.schemaVersion = self.schemaVersion
            ds.processingLevel = self.processingLevel
            ds.processingLog = self.processingLog
            ds.titlePublic = self.titlePublic
            ds.titleVariant = self.titleVariant
            ds.attributionShort = self.attributionShort
            ds.attribution = self.attribution
            ds.descriptionShort = self.descriptionShort
            ds.descriptionFromProducer = self.descriptionFromProducer
            ds.descriptionKey = self.descriptionKey
            ds.descriptionProcessing = self.descriptionProcessing
            ds.licenses = self.licenses
            ds.license = self.license
            ds.type = self.type
            ds.updatedAt = datetime.utcnow()
            # do not update these fields unless they're specified
            if self.columnOrder is not None:
                ds.columnOrder = self.columnOrder
            if self.code is not None:
                ds.code = self.code
            if self.originalMetadata is not None:
                ds.originalMetadata = self.originalMetadata
            if self.sort is not None:
                ds.sort = self.sort

        session.add(ds)
        session.flush()  # Ensure the object is written to the database and its ID is generated
        return ds

    @classmethod
    def from_variable_metadata(
        cls,
        metadata: catalog.VariableMeta,
        short_name: str,
        timespan: str,
        dataset_id: int,
        source_id: Optional[int],
        catalog_path: str,
        dimensions: Optional[Dimensions],
    ) -> "Variable":
        # `unit` can be an empty string, but cannot be null
        assert metadata.unit is not None

        if metadata.presentation:
            presentation_dict = metadata.presentation.to_dict()  # type: ignore
            # convert all fields from snake_case to camelCase
            presentation_dict = humps.camelize(presentation_dict)
        else:
            presentation_dict = {}

        # field that are processed elsewhere and are not part of the class
        presentation_dict.pop("topicTags", None)
        presentation_dict.pop("faqs", None)

        if metadata.description_key:
            assert isinstance(metadata.description_key, list), "descriptionKey should be a list of bullet points"

        return cls(
            shortName=short_name,
            name=metadata.title,
            sourceId=source_id,
            datasetId=dataset_id,
            description=metadata.description,
            unit=metadata.unit,
            shortUnit=metadata.short_unit,
            timespan=timespan,
            coverage="",
            display=metadata.display or {},
            catalogPath=catalog_path,
            dimensions=dimensions,
            schemaVersion=metadata.schema_version,
            processingLevel=metadata.processing_level,
            descriptionShort=metadata.description_short,
            descriptionFromProducer=metadata.description_from_producer,
            descriptionKey=metadata.description_key,
            descriptionProcessing=metadata.description_processing,
            licenses=[license.to_dict() for license in metadata.licenses] if metadata.licenses else None,
            license=metadata.license.to_dict() if metadata.license else None,
            type=metadata.type,
            sort=metadata.sort,
            **presentation_dict,
        )

    @classmethod
    def load_variables_in_datasets(
        cls,
        session: Session,
        dataset_uris: Optional[List[str]] = None,
        dataset_ids: Optional[List[int]] = None,
    ) -> List["Variable"]:
        if dataset_uris is not None:
            conditions = [cls.catalogPath.startswith(uri) for uri in dataset_uris]
            query = select(cls).where(or_(*conditions))
        elif dataset_ids is not None:
            query = select(cls).where(cls.datasetId.in_(dataset_ids))
        else:
            raise ValueError("Either dataset_uris or dataset_ids must be provided")
        results = session.scalars(query).all()
        return list(results)

    @classmethod
    def load_variables_in_chart(
        cls,
        session: Session,
        chart_id: int,
    ) -> List["Variable"]:
        chart = Chart.load_chart(session, chart_id)
        variables = chart.load_chart_variables(session)
        return list(variables.values())

    @classmethod
    @deprecated("Use from_id_or_path instead")
    def load_variable(cls, session: Session, variable_id: int) -> "Variable":
        """D"""
        return session.scalars(select(cls).where(cls.id == variable_id)).one()

    @classmethod
    @deprecated("Use from_id_or_path instead")
    def load_variables(cls, session: Session, variables_id: List[int]) -> List["Variable"]:
        return session.scalars(select(cls).where(cls.id.in_(variables_id))).all()  # type: ignore

    @overload
    @classmethod
    def from_id_or_path(
        cls, session: Session, id_or_path: str | int, columns: Optional[List[str]] = None
    ) -> "Variable": ...

    @overload
    @classmethod
    def from_id_or_path(
        cls, session: Session, id_or_path: List[str | int], columns: Optional[List[str]] = None
    ) -> List["Variable"]: ...

    @classmethod
    def from_id_or_path(
        cls,
        session: Session,
        id_or_path: int | str | List[str | int],
        columns: Optional[List[str]] = None,
    ) -> "Variable" | List["Variable"]:
        """Load a variable from the database by its catalog path or variable ID."""
        # Single id
        if isinstance(id_or_path, int):
            return cls.from_id(session=session, variable_id=id_or_path, columns=columns)
        # Single path
        elif isinstance(id_or_path, str):
            return cls.from_catalog_path(session=session, catalog_path=id_or_path, columns=columns)

        # Multiple path or id
        elif isinstance(id_or_path, list):
            # Filter the list to ensure only integers are passed
            int_ids = [i for i in id_or_path if isinstance(i, (int, np.integer))]
            str_ids = [i for i in id_or_path if isinstance(i, str)]
            # Multiple IDs
            if len(int_ids) == len(id_or_path):
                return cls.from_id(session=session, variable_id=int_ids, columns=columns)
            # Multiple paths
            elif len(str_ids) == len(id_or_path):
                return cls.from_catalog_path(session=session, catalog_path=str_ids, columns=columns)
            else:
                raise TypeError("All elements in the list must be integers")

        # # Ensure mutual exclusivity of catalog_path and variable_id
        # if (catalog_path is not None) and (variable_id is not None):
        #     raise ValueError("Only one of catalog_path or variable_id can be provided")

        # if (catalog_path is not None) & isinstance(catalog_path, (str, list)):
        #     return cls.from_catalog_path(session=session, catalog_path=catalog_path)
        # elif isinstance(catalog_path, (int, list)):
        #     return cls.from_id(session=session, variable_id=variable_id)
        # else:
        #     raise ValueError("Either catalog_path or variable_id must be provided")

    @overload
    @classmethod
    def from_catalog_path(
        cls, session: Session, catalog_path: str, columns: Optional[List[str]] = None
    ) -> "Variable": ...

    @overload
    @classmethod
    def from_catalog_path(
        cls, session: Session, catalog_path: List[str], columns: Optional[List[str]] = None
    ) -> List["Variable"]: ...

    @classmethod
    def from_catalog_path(
        cls, session: Session, catalog_path: str | List[str], columns: Optional[List[str]] = None
    ) -> "Variable" | List["Variable"]:
        """Load a variable from the DB by its catalog path."""
        assert "#" in catalog_path, "catalog_path should end with #indicator_short_name"
        # Return Variable if columns is None and return Row object if columns is provided
        execute = session.execute if columns else session.scalars
        if isinstance(catalog_path, str):
            return execute(_select_columns(cls, columns).where(cls.catalogPath == catalog_path)).one()  # type: ignore
        elif isinstance(catalog_path, list):
            return execute(_select_columns(cls, columns).where(cls.catalogPath.in_(catalog_path))).all()  # type: ignore

    @overload
    @classmethod
    def from_id(cls, session: Session, variable_id: int, columns: Optional[List[str]] = None) -> "Variable": ...

    @overload
    @classmethod
    def from_id(
        cls, session: Session, variable_id: List[int], columns: Optional[List[str]] = None
    ) -> List["Variable"]: ...

    @classmethod
    def from_id(
        cls, session: Session, variable_id: int | List[int], columns: Optional[List[str]] = None
    ) -> "Variable" | List["Variable"]:
        """Load a variable (or list of variables) from the DB by its ID path."""
        # Return Variable if columns is None and return Row object if columns is provided
        execute = session.execute if columns else session.scalars

        if isinstance(variable_id, int):
            return execute(_select_columns(cls, columns).where(cls.id == variable_id)).one()  # type: ignore
        elif isinstance(variable_id, list):
            return execute(_select_columns(cls, columns).where(cls.id.in_(variable_id))).all()  # type: ignore

    @classmethod
    def catalog_paths_to_variable_ids(cls, session: Session, catalog_paths: List[str]) -> Dict[str, int]:
        """Return a mapping from catalog paths to variable IDs."""
        query = select(Variable).where(Variable.catalogPath.in_(catalog_paths))
        return {var.catalogPath: var.id for var in session.scalars(query).all()}  # type: ignore

    @classmethod
    def infer_type(cls, values: pd.Series) -> VARIABLE_TYPE:
        """Set type and sort fields based on indicator values."""
        return _infer_variable_type(values)

    def update_links(
        self, session: Session, db_origins: List["Origin"], faqs: List[catalog.FaqLink], tag_names: List[str]
    ) -> None:
        """
        Establishes relationships between the current variable and a list of origins and a list of posts.
        """
        assert self.id

        # establish relationships between variables and origins
        OriginsVariablesLink.link_with_variable(session, self.id, [origin.id for origin in db_origins])

        # establish relationships between variables and posts
        required_gdoc_ids = {faq.gdoc_id for faq in faqs}
        query = select(PostsGdocs).where(PostsGdocs.id.in_(required_gdoc_ids))
        gdoc_posts = session.scalars(query).all()
        existing_gdoc_ids = {gdoc_post.id for gdoc_post in gdoc_posts}
        missing_gdoc_ids = required_gdoc_ids - existing_gdoc_ids
        if missing_gdoc_ids:
            log.warning("create_links.missing_faqs", missing_gdoc_ids=missing_gdoc_ids)
        PostsGdocsVariablesFaqsLink.link_with_variable(
            session, self.id, [faq for faq in faqs if faq.gdoc_id in existing_gdoc_ids]
        )

        # establish relationships between variables and tags
        tags = Tag.load_tags_by_names(session, tag_names)

        TagsVariablesTopicTagsLink.link_with_variable(session, self.id, [tag.id for tag in tags])

    def s3_data_path(self, typ: S3_PATH_TYP = "s3") -> str:
        """Path to S3 with data in JSON format for Grapher. Typically
        s3://owid-api/v1/indicators/123.data.json."""
        if typ == "s3":
            return f"{config.BAKED_VARIABLES_PATH}/{self.id}.data.json"
        elif typ == "http":
            return f"{config.DATA_API_URL}/{self.id}.data.json"
        else:
            raise NotImplementedError()

    def s3_metadata_path(self, typ: S3_PATH_TYP = "s3") -> str:
        """Path to S3 with metadata in JSON format for Grapher. Typically
        s3://owid-api/v1/indicators/123.metadata.json or
        s3://owid-api-staging/name/v1/indicators/123.metadata.json
        ."""
        if typ == "s3":
            return f"{config.BAKED_VARIABLES_PATH}/{self.id}.metadata.json"
        elif typ == "http":
            return f"{config.DATA_API_URL}/{self.id}.metadata.json"
        else:
            raise NotImplementedError()

    @property
    def table_name(self) -> str:
        assert self.catalogPath
        return self.catalogPath.split("#")[0].rsplit("/", 1)[1]

    @property
    def step_path(self) -> Path:
        """Return path to indicator step file."""
        assert self.catalogPath
        base_path = paths.STEP_DIR / "data" / self.catalogPath.split("#")[0].rsplit("/", 1)[0]
        return base_path.with_suffix(".py")

    @property
    def override_yaml_path(self) -> Path:
        """Return path to indicator YAML file."""
        return self.step_path.with_suffix(".meta.override.yml")

    def get_data(self, session: Optional[Session] = None) -> pd.DataFrame:
        """Get variable data from S3.

        If session is given, entity codes are replaced with entity names.
        """
        data = requests.get(self.s3_data_path(typ="http")).json()
        df = pd.DataFrame(data)

        if session is not None:
            df = add_entity_name(session=session, df=df, col_id="entities", col_name="entity")

        return df

    def get_metadata(self) -> Dict[str, Any]:
        metadata = requests.get(self.s3_metadata_path(typ="http")).json()

        return metadata


class ChartDimensions(Base):
    __tablename__ = "chart_dimensions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["chartId"],
            ["charts.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="chart_dimensions_chartId_78d6a092_fk_charts_id",
        ),
        ForeignKeyConstraint(
            ["variableId"],
            ["variables.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="chart_dimensions_variableId_9ba778e6_fk_variables_id",
        ),
        Index("chart_dimensions_chartId_78d6a092_fk_charts_id", "chartId"),
        Index("chart_dimensions_variableId_9ba778e6_fk_variables_id", "variableId"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    order: Mapped[int] = mapped_column(Integer)
    property: Mapped[str] = mapped_column(VARCHAR(255))
    chartId: Mapped[int] = mapped_column(Integer)
    variableId: Mapped[int] = mapped_column(Integer)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)

    @classmethod
    def chart_ids_with_indicators(cls, session: Session, indicator_ids: List[int]) -> List[int]:
        """Return a list of chart IDs that have any of the given indicators."""
        query = select(cls.chartId).where(cls.variableId.in_(indicator_ids))
        return list(session.scalars(query).all())

    @classmethod
    def indicators_in_charts(cls, session: Session, chart_ids: List[int]) -> Set[int]:
        """Return a list of indicator IDs that are in any of the given charts."""
        query = select(cls.variableId).where(cls.chartId.in_(chart_ids))
        return set(session.scalars(query).all())

    @classmethod
    def filter_indicators_used_in_charts(cls, session: Session, indicator_ids: List[int]) -> List[int]:
        """Reduce the input list of indicator IDs to only those used in charts."""
        query = select(cls.variableId).where(cls.variableId.in_(indicator_ids))
        return list(set(session.scalars(query).all()))


class Origin(Base):
    """Get CREATE TABLE statement for origins table with
    ```
    from sqlalchemy.schema import CreateTable
    from etl.grapher.model import Origin
    print(str(CreateTable(Origin.__table__).compile(engine)))
    ```
    """

    __tablename__ = "origins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    titleSnapshot: Mapped[Optional[str]] = mapped_column(VARCHAR(512))
    title: Mapped[Optional[str]] = mapped_column(VARCHAR(512))
    descriptionSnapshot: Mapped[Optional[str]] = mapped_column(TEXT)
    description: Mapped[Optional[str]] = mapped_column(TEXT)
    producer: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    citationFull: Mapped[Optional[str]] = mapped_column(TEXT)
    attribution: Mapped[Optional[str]] = mapped_column(TEXT)
    attributionShort: Mapped[Optional[str]] = mapped_column(VARCHAR(512))
    versionProducer: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    urlMain: Mapped[Optional[str]] = mapped_column(TEXT)
    urlDownload: Mapped[Optional[str]] = mapped_column(TEXT)
    dateAccessed: Mapped[Optional[date]] = mapped_column(Date)
    datePublished: Mapped[Optional[str]] = mapped_column(VARCHAR(10))
    license: Mapped[Optional[dict]] = mapped_column(JSON)

    @classmethod
    def from_origin(
        cls,
        origin: catalog.Origin,
    ) -> "Origin":
        return cls(
            producer=origin.producer,
            citationFull=origin.citation_full,
            titleSnapshot=origin.title_snapshot,
            title=origin.title,
            attribution=origin.attribution,
            attributionShort=origin.attribution_short,
            versionProducer=origin.version_producer,
            license=origin.license.to_dict() if origin.license else None,
            urlMain=origin.url_main,
            urlDownload=origin.url_download,
            descriptionSnapshot=origin.description_snapshot,
            description=origin.description,
            datePublished=origin.date_published,
            dateAccessed=origin.date_accessed,  # type: ignore
        )

    @property
    def _upsert_select(self) -> Select:
        # match on all fields for now, otherwise we could get an origin from a different dataset
        # and modify it, which would make it out of sync with origin from its recipe
        # NOTE: we don't match on license because it's JSON and hard to compare
        cls = self.__class__
        return select(cls).where(
            cls.producer == self.producer,
            cls.citationFull == self.citationFull,
            cls.titleSnapshot == self.titleSnapshot,
            cls.title == self.title,
            cls.attribution == self.attribution,
            cls.attributionShort == self.attributionShort,
            cls.versionProducer == self.versionProducer,
            cls.urlMain == self.urlMain,
            cls.urlDownload == self.urlDownload,
            cls.descriptionSnapshot == self.descriptionSnapshot,
            cls.description == self.description,
            cls.datePublished == self.datePublished,
            cls.dateAccessed == self.dateAccessed,
        )

    def upsert(self, session: Session) -> "Origin":
        """
        # NOTE: this would be an ideal solution if we only stored unique rows in
        # origins table, but there are weird race conditions and we cannot have
        # index on all columns because it would be too long.
        # Storing duplicate origins is not a big deal though

        origin = session.scalars(self._upsert_select).one_or_none()
        if origin is None:
            # create new origin
            origin = self
        else:
            # we match on all fields, so there's nothing to update
            pass

        session.add(origin)

        # select added object to get its id
        return session.scalars(self._upsert_select).one()
        """

        origins = session.scalars(self._upsert_select).all()
        if not origins:
            # create new origin
            origin = self
            session.add(origin)
        else:
            # we match on all fields, so there's nothing to update
            # just pick any origin
            origin = origins[0]

        return origin


class ChartStatus(Enum):
    APPROVED = "approved"
    PENDING = "pending"
    REJECTED = "rejected"


CHART_DIFF_STATUS = Literal[
    ChartStatus.APPROVED,
    ChartStatus.PENDING,
    ChartStatus.REJECTED,
]


class ChartDiffApprovals(Base):
    __tablename__ = "chart_diff_approvals"
    __table_args__ = (
        ForeignKeyConstraint(
            ["chartId"], ["charts.id"], ondelete="CASCADE", onupdate="CASCADE", name="chart_diff_approvals_ibfk_1"
        ),
        Index("chartId", "chartId"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    chartId: Mapped[int] = mapped_column(Integer)
    sourceUpdatedAt: Mapped[datetime] = mapped_column(DateTime)
    status: Mapped[CHART_DIFF_STATUS] = mapped_column(VARCHAR(255))
    targetUpdatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, default=func.utc_timestamp())

    @classmethod
    def latest_chart_approval_batch(
        cls, session: Session, chart_ids: list[int], source_updated_ats: list, target_updated_ats: list
    ) -> List[Optional["ChartDiffApprovals"]]:
        """Load the latest approval of the charts.

        Returns: List of same length of chart_ids. First item in returned list corresponds to first chart_id in input list.
        """
        if not (len(chart_ids) == len(source_updated_ats) == len(target_updated_ats)):
            raise ValueError("All input lists must have the same length")

        # Get matches from DB
        criteria = list(zip(chart_ids, source_updated_ats, target_updated_ats))

        raw_results = session.scalars(
            select(cls)
            .where(
                or_(
                    *[
                        and_(
                            cls.chartId == chart_id,
                            cls.sourceUpdatedAt == source_updated_at,
                            cls.targetUpdatedAt == target_updated_at,
                        )
                        for chart_id, source_updated_at, target_updated_at in criteria
                    ]
                )
            )
            .order_by(cls.updatedAt.desc())
        ).all()

        # Take the latest approval for each chart - note that it's sorted by updatedAt, so we take just the
        # first element
        results = {}
        for r in raw_results:
            if r.chartId not in results:
                results[r.chartId] = r

        # List with approval objects corresponding to charts specified by IDs in chart_ids
        approvals = []
        for chart_id in chart_ids:
            approval = None
            if chart_id in results:
                approval = results[chart_id]
            approvals.append(approval)

        assert len(chart_ids) == len(approvals), "Length of chart_ids and approvals must be the same."

        return approvals

    @classmethod
    def get_all(cls, session: Session, chart_id: int) -> List["ChartDiffApprovals"]:
        """Get history of values."""
        result = session.scalars(
            select(cls)
            .where(
                cls.chartId == chart_id,
            )
            .order_by(cls.updatedAt.desc())
        ).fetchall()
        return list(result)


class ChartDiffConflicts(Base):
    __tablename__ = "chart_diff_conflicts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["chartId"], ["charts.id"], ondelete="CASCADE", onupdate="CASCADE", name="chart_diff_conflicts_ibfk_1"
        ),
        Index("chartId", "chartId"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    chartId: Mapped[int] = mapped_column(Integer)
    targetUpdatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime)
    conflict: Mapped[Literal["resolved", "pending"]] = mapped_column(VARCHAR(255))
    updatedAt: Mapped[datetime] = mapped_column(DateTime, default=func.utc_timestamp())

    @classmethod
    def get_conflict_batch(
        cls, session: Session, chart_ids: list[int], target_updated_ats: list
    ) -> List[Optional["ChartDiffConflicts"]]:
        """Load the latest conflict of charts (if exist).

        Returns: List of same length of chart_ids. First item in returned list corresponds to first chart_id in input list.
        """
        if not (len(chart_ids) == len(target_updated_ats)):
            raise ValueError("All input lists must have the same length")

        # Get matches from DB
        criteria = list(zip(chart_ids, target_updated_ats))

        raw_results = session.scalars(
            select(cls)
            .where(
                or_(
                    *[
                        and_(
                            cls.chartId == chart_id,
                            cls.targetUpdatedAt == target_updated_at,
                        )
                        for chart_id, target_updated_at in criteria
                    ]
                )
            )
            .order_by(cls.updatedAt.desc())
        ).all()

        # Take the latest conflict for each chart - note that it's sorted by updatedAt, so we take just the
        # first element
        results = {}
        for r in raw_results:
            if r.chartId not in results:
                results[r.chartId] = r

        # List with conflicts objects corresponding to charts specified by IDs in chart_ids and timestamp in target_updated_ats
        conflicts = []
        for chart_id in chart_ids:
            conflict = None
            if chart_id in results:
                conflict = results[chart_id]
            conflicts.append(conflict)

        assert len(chart_ids) == len(conflicts), "Length of chart_ids and conflicts must be the same."

        return conflicts


class MultiDimDataPage(Base):
    __tablename__ = "multi_dim_data_pages"

    slug: Mapped[str] = mapped_column(VARCHAR(255), primary_key=True)
    config: Mapped[dict] = mapped_column(JSON)
    published: Mapped[int] = mapped_column(TINYINT, server_default=text("'0'"), init=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    updatedAt: Mapped[datetime] = mapped_column(
        DateTime, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), init=False
    )

    def upsert(self, session: Session) -> "MultiDimDataPage":
        cls = self.__class__
        existing = session.scalars(select(cls).where(cls.slug == self.slug)).one_or_none()
        if existing:
            existing.config = self.config
            return existing
        else:
            session.add(self)
            return self


class Anomaly(Base):
    __tablename__ = "anomalies"
    # __table_args__ = (Index("catalogPath", "catalogPath"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    updatedAt: Mapped[datetime] = mapped_column(
        DateTime, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), init=False
    )
    datasetId: Mapped[int] = mapped_column(Integer)
    datasetSourceChecksum: Mapped[Optional[str]] = mapped_column(VARCHAR(64), default=None)
    anomalyType: Mapped[str] = mapped_column(VARCHAR(255), default=str)
    path_file: Mapped[Optional[str]] = mapped_column(VARCHAR(255), default=None)
    _dfScore: Mapped[Optional[bytes]] = mapped_column("dfScore", LONGBLOB, default=None)
    _dfReduced: Mapped[Optional[bytes]] = mapped_column("dfReduced", LONGBLOB, default=None)
    # catalogPath: Mapped[str] = mapped_column(VARCHAR(255), default=None)
    # NOTE: why do we need indicatorChecksum?
    # Answer: This can be useful to assign an anomaly to a specific snapshot of the indicator. Unclear if we need it atm, but maybe in the future...
    # indicatorChecksum: Mapped[str] = mapped_column(VARCHAR(255), default=None)
    # globalScore: Mapped[float] = mapped_column(Float, default=None, nullable=True)
    # gptInfo: Mapped[Optional[dict]] = mapped_column(JSON, default=None, nullable=True)
    # entity: Mapped[str] = mapped_column(VARCHAR(255))
    # year: Mapped[int] = mapped_column(Integer)
    # rawScore: Mapped[float] = mapped_column(Float)

    def __repr__(self) -> str:
        return (
            f"<Anomaly(id={self.id}, createdAt={self.createdAt}, updatedAt={self.updatedAt}, "
            f"datasetId={self.datasetId}, anomalyType={self.anomalyType})>"
        )

    @classmethod
    def load(cls, session: Session, dataset_id: int, anomaly_type: str) -> "Anomaly":
        return session.scalars(select(cls).where(cls.datasetId == dataset_id, cls.anomalyType == anomaly_type)).one()

    @hybrid_property
    def dfScore(self) -> Optional[pd.DataFrame]:  # type: ignore
        if self._dfScore is None:
            return None
        buffer = io.BytesIO(self._dfScore)
        return feather.read_feather(buffer)

    @dfScore.setter
    def dfScore(self, value: Optional[pd.DataFrame]) -> None:
        if value is None:
            self._dfScore = None
        else:
            buffer = io.BytesIO()
            feather.write_feather(value, buffer, compression="zstd")
            buffer.seek(0)
            self._dfScore = buffer.read()

    @hybrid_property
    def dfReduced(self) -> Optional[pd.DataFrame]:  # type: ignore
        if self._dfReduced is None:
            return None
        buffer = io.BytesIO(self._dfReduced)
        return feather.read_feather(buffer)

    @dfReduced.setter
    def dfReduced(self, value: Optional[pd.DataFrame]) -> None:
        if value is None:
            self._dfReduced = None
        else:
            buffer = io.BytesIO()
            feather.write_feather(value, buffer, compression="zstd")
            buffer.seek(0)
            self._dfReduced = buffer.read()

    @classmethod
    def load_anomalies(cls, session: Session, dataset_id: List[int]) -> List["Anomaly"]:
        return session.scalars(select(cls).where(cls.datasetId.in_(dataset_id))).all()  # type: ignore


def _json_is(json_field: Any, key: str, val: Any) -> Any:
    """SQLAlchemy condition for checking if a JSON field has a key with a given value. Works for null."""
    if val is None:
        return text(f"JSON_VALUE({json_field.key}, '$.{key}') IS NULL")
    else:
        return json_field[key] == val


def _remap_variable_ids(config: Union[List, Dict[str, Any], Any], remap_ids: Dict[int, int]) -> Any:
    """Replace variableIds from chart config using `remap_ids` mapping."""
    if isinstance(config, dict):
        out = {}
        for k, v in config.items():
            if k == "variableId":
                out[k] = remap_ids[int(v)]
            # columnSlug is actually a variable id, but stored as a string (it wasn't a great decision)
            elif k in ("columnSlug", "sortColumnSlug"):
                # sometimes columnSlug stays in config, but is deleted from dimensions. Ignore it
                if int(v) in remap_ids:
                    out[k] = str(remap_ids[int(v)])
            # if new fields with variable ids are added, try to handle them and raise a warning
            elif isinstance(v, int) and v in remap_ids:
                log.warning("remap_variable_ids.new_field", field=k, value=v)
                out[k] = remap_ids[v]
            elif isinstance(v, str) and v.isdigit() and int(v) in remap_ids:
                log.warning("remap_variable_ids.new_field", field=k, value=v)
                out[k] = str(remap_ids[int(v)])
            else:
                out[k] = _remap_variable_ids(v, remap_ids)
        return out
    elif isinstance(config, list):
        return [_remap_variable_ids(item, remap_ids) for item in config]
    else:
        return config


def _infer_variable_type(values: pd.Series) -> VARIABLE_TYPE:
    # values don't contain null values
    assert values.notnull().all(), "values must not contain nulls"
    assert values.map(lambda x: isinstance(x, str)).all(), "only works for strings"
    if values.empty:
        return "mixed"
    try:
        values = pd.to_numeric(values)
        inferred_type = pd.api.types.infer_dtype(values)
        if inferred_type == "floating":
            return "float"
        elif inferred_type == "integer":
            return "int"
        else:
            raise NotImplementedError()
    except ValueError:
        if values.map(_is_float).any():
            return "mixed"
        else:
            return "string"


def _is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True


def add_entity_name(
    session: Session,
    df: pd.DataFrame,
    col_id: str,
    col_name: str = "entity",
    col_code: Optional[str] = None,
    remove_id: bool = True,
) -> pd.DataFrame:
    # Initialize
    if df.empty:
        df[col_name] = []
        if col_code is not None:
            df[col_code] = []
        return df

    # Get entity names
    unique_entities = df[col_id].unique()
    entities = _fetch_entities(session, list(unique_entities), col_id, col_name, col_code)

    # Sanity check
    if set(unique_entities) - set(entities[col_id]):
        missing_entities = set(unique_entities) - set(entities[col_id])
        raise ValueError(f"Missing entities in the database: {missing_entities}")

    # Set dtypes
    dtypes = {col_name: "category", col_id: int}
    if col_code is not None:
        dtypes[col_code] = "category"
    df = pd.merge(df, entities.astype(dtypes), on=col_id)

    # Remove entity IDs
    if remove_id:
        df = df.drop(columns=[col_id])

    return df


def _fetch_entities(
    session: Session,
    entity_ids: List[int],
    col_id: Optional[str] = None,
    col_name: Optional[str] = None,
    col_code: Optional[str] = None,
) -> pd.DataFrame:
    # Query entities from the database
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in %(entity_ids)s
    """
    df = read_sql(q, session, params={"entity_ids": entity_ids})

    # Rename columns
    column_renames = {}
    if col_id is not None:
        column_renames["entityId"] = col_id
    if col_name is not None:
        column_renames["entityName"] = col_name
    if col_code is not None:
        column_renames["entityCode"] = col_code
    else:
        df = df.drop(columns=["entityCode"])

    df = df.rename(columns=column_renames)
    return df


def _select_columns(cls, columns: Optional[list[str]] = None) -> Select:
    # Select only the specified columns, or all if not specified
    if columns:
        # Use getattr to dynamically select the columns
        columns_to_select = [getattr(cls, col) for col in columns]
        return select(*columns_to_select)
    else:
        return select(cls)
