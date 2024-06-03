"""This schema was generated using https://github.com/agronholm/sqlacodegen library with the following command:
```
sqlacodegen --generator dataclasses --options use_inflect mysql://root:owid@localhost:3306/owid
```
If you want to add a new table to ORM, add --tables mytable to the command above.

Another option is to run `show create table mytable;` in MySQL and then ask ChatGPT to convert it to SQLAlchemy 2 ORM.

It is often necessary to add `default=None` or `init=False` to make pyright happy.
"""

import json
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Union, get_args

import humps
import pandas as pd
import structlog
from owid import catalog
from owid.catalog.meta import VARIABLE_TYPE
from sqlalchemy import JSON as _JSON
from sqlalchemy import (
    BigInteger,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
    func,
    or_,
    select,
    text,
)
from sqlalchemy.dialects.mysql import (
    ENUM,
    LONGTEXT,
    TEXT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, Session, mapped_column  # type: ignore
from sqlalchemy.sql import Select
from typing_extensions import Self, TypedDict

from etl import config, paths
from etl.config import GRAPHER_USER_ID
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


class Entity(Base):
    __tablename__ = "entities"
    __table_args__ = (Index("code", "code", unique=True), Index("name", "name", unique=True))

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    name: Mapped[str] = mapped_column(VARCHAR(255))
    validated: Mapped[int] = mapped_column(TINYINT(1))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    displayName: Mapped[str] = mapped_column(VARCHAR(255))
    code: Mapped[Optional[str]] = mapped_column(VARCHAR(255))
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)


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
        tags = session.scalars(select(Tag).where(Tag.name.in_(tag_names), Tag.slug.isnot(None))).all()

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
    password: Mapped[Optional[str]] = mapped_column(VARCHAR(128))
    lastLogin: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, init=False)
    lastSeen: Mapped[Optional[datetime]] = mapped_column(DateTime)


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


class Chart(Base):
    __tablename__ = "charts"
    __table_args__ = (
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
        Index("charts_slug", "slug"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    config: Mapped[dict] = mapped_column(JSON)
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"), init=False)
    lastEditedAt: Mapped[datetime] = mapped_column(DateTime)
    lastEditedByUserId: Mapped[int] = mapped_column(Integer)
    is_indexable: Mapped[int] = mapped_column(TINYINT(1), server_default=text("'0'"))
    slug: Mapped[str] = mapped_column(
        VARCHAR(255), Computed("(json_unquote(json_extract(`config`,_utf8mb4'$.slug')))", persisted=False)
    )
    type: Mapped[Optional[str]] = mapped_column(
        VARCHAR(255),
        Computed(
            "(coalesce(json_unquote(json_extract(`config`,_utf8mb4'$.type')),_utf8mb4'LineChart'))", persisted=False
        ),
    )
    updatedAt: Mapped[datetime] = mapped_column(DateTime, init=False)
    publishedAt: Mapped[Optional[datetime]] = mapped_column(DateTime)
    publishedByUserId: Mapped[Optional[int]] = mapped_column(Integer)

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

        # add columnSlug if present
        column_slug = self.config.get("map", {}).get("columnSlug")
        if column_slug:
            try:
                variables[int(column_slug)] = Variable.load_variable(session, column_slug)
            except NoResultFound:
                raise ValueError(f"columnSlug variable {column_slug} for chart {self.id} not found")

        return variables

    def migrate_to_db(self, source_session: Session, target_session: Session) -> "Chart":
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
                    target_var = Variable.load_from_catalog_path(target_session, source_var.catalogPath)
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
        target_chart = Chart.from_dict(self.dict())
        del target_chart.id
        target_chart.config = _remap_variable_ids(target_chart.config, remap_ids)

        # set proper GRAPHER_USER_ID
        assert GRAPHER_USER_ID
        target_chart.lastEditedByUserId = int(GRAPHER_USER_ID)

        return target_chart

    def tags(self, session: Session) -> List[Dict[str, Any]]:
        """Return tags in a format suitable for Admin API."""
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
        Index("unique_short_name_version_namespace", "shortName", "version", "namespace", unique=True),
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

    def upsert(self, session: Session) -> "Dataset":
        cls = self.__class__
        q = select(cls).where(
            cls.shortName == self.shortName,
            cls.version == self.version,
            cls.namespace == self.namespace,
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
            ds.updatedAt = datetime.utcnow()
            ds.metadataEditedAt = datetime.utcnow()
            ds.dataEditedAt = datetime.utcnow()

        # null checksum to label it as undone
        ds.sourceChecksum = None

        session.add(ds)

        # select added object to get its id
        q = select(cls).where(
            cls.shortName == self.shortName,
            cls.version == self.version,
            cls.namespace == self.namespace,
        )
        return session.scalars(q).one()

    @classmethod
    def from_dataset_metadata(cls, metadata: catalog.DatasetMeta, namespace: str, user_id: int) -> "Dataset":
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

        # select added object to get its id
        return session.scalars(self._upsert_select).one()

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


class SuggestedChartRevisions(Base):
    __tablename__ = "suggested_chart_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["chartId"],
            ["charts.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="suggested_chart_revisions_ibfk_1",
        ),
        ForeignKeyConstraint(
            ["createdBy"],
            ["users.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="suggested_chart_revisions_ibfk_2",
        ),
        ForeignKeyConstraint(
            ["updatedBy"],
            ["users.id"],
            ondelete="RESTRICT",
            onupdate="RESTRICT",
            name="suggested_chart_revisions_ibfk_3",
        ),
        Index("chartId", "chartId", "originalVersion", "suggestedVersion", "isPendingOrFlagged", unique=True),
        Index("createdBy", "createdBy"),
        Index("updatedBy", "updatedBy"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, init=False)
    chartId: Mapped[int] = mapped_column(Integer)
    createdBy: Mapped[int] = mapped_column(Integer)
    originalConfig: Mapped[dict] = mapped_column(JSON)
    suggestedConfig: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(VARCHAR(8))
    createdAt: Mapped[datetime] = mapped_column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    originalVersion: Mapped[int] = mapped_column(
        Integer,
        Computed("(json_unquote(json_extract(`originalConfig`,_utf8mb4'$.version')))", persisted=False),
        init=False,
    )
    suggestedVersion: Mapped[int] = mapped_column(
        Integer,
        Computed("(json_unquote(json_extract(`suggestedConfig`,_utf8mb4'$.version')))", persisted=False),
        init=False,
    )
    updatedBy: Mapped[Optional[int]] = mapped_column(Integer)
    updatedAt: Mapped[datetime] = mapped_column(DateTime)
    suggestedReason: Mapped[Optional[str]] = mapped_column(VARCHAR(512), default=None)
    decisionReason: Mapped[Optional[str]] = mapped_column(VARCHAR(512), default=None)
    isPendingOrFlagged: Mapped[Optional[int]] = mapped_column(
        TINYINT(1),
        Computed("(if((`status` in (_utf8mb4'pending',_utf8mb4'flagged')),true,NULL))", persisted=False),
        init=False,
    )
    changesInDataSummary: Mapped[Optional[str]] = mapped_column(TEXT, default="")
    experimental: Mapped[Optional[dict]] = mapped_column(JSON, default=None)

    @classmethod
    def load_pending(cls, session: Session, user_id: Optional[int] = None) -> List["SuggestedChartRevisions"]:
        if user_id is None:
            return list(
                session.scalars(
                    select(SuggestedChartRevisions).where((SuggestedChartRevisions.status == "pending"))
                ).all()
            )
        else:
            return list(
                session.scalars(
                    select(SuggestedChartRevisions)
                    .where(SuggestedChartRevisions.status == "pending")
                    .where(SuggestedChartRevisions.createdBy == user_id)
                ).all()
            )

    @classmethod
    def load_revisions(cls, session: Session, chart_id: int) -> List["SuggestedChartRevisions"]:
        return list(
            session.scalars(select(SuggestedChartRevisions).where(SuggestedChartRevisions.chartId == chart_id)).all()
        )


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
    breadcrumbs: Mapped[Optional[dict]] = mapped_column(JSON)
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
        'grapherConfigAdmin': None
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
        Index("unique_short_name_per_dataset", "shortName", "datasetId", unique=True),
        Index("variables_code_fk_dst_id_7bde8c2a_uniq", "code", "datasetId", unique=True),
        Index("variables_datasetId_50a98bfd_fk_datasets_id", "datasetId"),
        Index("variables_name_fk_dst_id_f7453c33_uniq", "name", "datasetId", unique=True),
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
    grapherConfigAdmin: Mapped[Optional[dict]] = mapped_column(JSON, default=None)
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
    grapherConfigETL: Mapped[Optional[dict]] = mapped_column(JSON, default=None)
    type: Mapped[Optional[VARIABLE_TYPE]] = mapped_column(ENUM(*get_args(VARIABLE_TYPE)), default=None)
    sort: Mapped[Optional[list[str]]] = mapped_column(JSON, default=None)

    def upsert(self, session: Session) -> "Variable":
        assert self.shortName

        cls = self.__class__

        # try matching on shortName first
        q = select(cls).where(
            or_(
                cls.shortName == self.shortName,
                # NOTE: we used to slugify shortName which replaced double underscore by a single underscore
                # this was a bug, we should have kept the double underscore
                # match even those variables and correct their shortName
                cls.shortName == self.shortName.replace("__", "_"),
            ),
            cls.datasetId == self.datasetId,
        )
        ds = session.scalars(q).one_or_none()

        # try matching on name if there was no match on shortName
        if not ds:
            q = select(cls).where(
                cls.name == self.name,
                cls.datasetId == self.datasetId,
            )
            ds = session.scalars(q).one_or_none()

        # there's a unique index on `name` which can cause conflict if we swap names of two variables
        # in that case, we append "(conflict)" to the name of the conflicting variable (it will be cleaned
        # after all variables are upserted)
        # we wouldn't need this if we dropped the requirement for unique index on `name`, but I'm afraid
        # of other functions in owid-grapher that could rely on it
        if ds and ds.shortName:
            q = select(cls).where(
                cls.name == self.name,
                cls.shortName != self.shortName,
                cls.datasetId == self.datasetId,
            )
            conflict = session.scalars(q).one_or_none()
            if conflict:
                conflict.name = f"{conflict.name} (conflict)"
                session.add(conflict)
                session.commit()

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
            if self.grapherConfigETL is not None:
                ds.grapherConfigETL = self.grapherConfigETL
            if self.sort is not None:
                ds.sort = self.sort
            assert self.grapherConfigAdmin is None, "grapherConfigETL should be used instead of grapherConfigAdmin"

        session.add(ds)

        # select added object to get its id
        q = select(cls).where(
            cls.shortName == self.shortName,
            cls.datasetId == self.datasetId,
        )
        return session.scalars(q).one()

    @classmethod
    def from_variable_metadata(
        cls,
        metadata: catalog.VariableMeta,
        short_name: str,
        timespan: str,
        dataset_id: int,
        source_id: Optional[int],
        catalog_path: Optional[str],
        dimensions: Optional[Dimensions],
    ) -> "Variable":
        # `unit` can be an empty string, but cannot be null
        assert metadata.unit is not None
        if catalog_path:
            assert "#" in catalog_path, "catalog_path should end with #indicator_short_name"

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

        # rename grapherConfig to grapherConfigETL
        if "grapherConfig" in presentation_dict:
            presentation_dict["grapherConfigETL"] = presentation_dict.pop("grapherConfig")

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
    def load_variable(cls, session: Session, variable_id: int) -> "Variable":
        return session.scalars(select(cls).where(cls.id == variable_id)).one()

    @classmethod
    def load_variables(cls, session: Session, variables_id: List[int]) -> List["Variable"]:
        return session.scalars(select(cls).where(cls.id.in_(variables_id))).all()  # type: ignore

    @classmethod
    def load_from_catalog_path(cls, session: Session, catalog_path: str) -> "Variable":
        assert "#" in catalog_path, "catalog_path should end with #indicator_short_name"
        return session.scalars(select(cls).where(cls.catalogPath == catalog_path)).one()

    def infer_type(self, values: pd.Series) -> VARIABLE_TYPE:
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


class Origin(Base):
    """Get CREATE TABLE statement for origins table with
    ```
    from sqlalchemy.schema import CreateTable
    from etl.grapher_model import Origin
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
    def latest_chart_status(cls, session: Session, chart_id: int, source_updated_at, target_updated_at) -> str:
        """Load the latest approval of the chart. If there's none, return ChartStatus.PENDING."""
        result = session.scalars(
            select(cls)
            .where(
                cls.chartId == chart_id,
                cls.sourceUpdatedAt == source_updated_at,
                cls.targetUpdatedAt == target_updated_at,
            )
            .order_by(cls.updatedAt.desc())
            .limit(1)
        ).first()
        if result:
            return result.status  # type: ignore
        else:
            return ChartStatus.PENDING.value

    @classmethod
    def get_all(cls, session: Session, chart_id: int) -> List["ChartDiffApprovals"]:
        """Get history of values."""
        result = session.scalars(
            select(cls)
            .where(
                cls.chartId == chart_id,
                # cls.sourceUpdatedAt == source_updated_at,
                # cls.targetUpdatedAt == target_updated_at,
            )
            .order_by(cls.updatedAt.desc())
        ).fetchall()
        return list(result)


def _json_is(json_field: Any, key: str, val: Any) -> Any:
    """SQLAlchemy condition for checking if a JSON field has a key with a given value. Works for null."""
    if val is None:
        return text(f"JSON_VALUE({json_field.key}, '$.{key}') IS NULL")
    else:
        return json_field[key] == val


def _remap_variable_ids(config: Union[List, Dict[str, Any]], remap_ids: Dict[int, int]) -> Any:
    """Replace variableIds from chart config using `remap_ids` mapping."""
    if isinstance(config, dict):
        out = {}
        for k, v in config.items():
            if k == "variableId":
                out[k] = remap_ids[int(v)]
            # columnSlug is actually a variable id, but stored as a string (it wasn't a great decision)
            elif k == "columnSlug":
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
