"""This schema was generated using https://github.com/agronholm/sqlacodegen library with the following command:
```
sqlacodegen --generator sqlmodels mysql://root@localhost:3306/owid
```
It has been slightly modified since then.
"""
import json
from datetime import date, datetime
from typing import Annotated, Any, Dict, List, Optional, Set, TypedDict
from urllib.parse import quote

import humps
import pandas as pd
import structlog
from owid import catalog
from sqlalchemy import (
    BigInteger,
    Computed,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.mysql import (
    ENUM,
    LONGBLOB,
    LONGTEXT,
    MEDIUMTEXT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.future import Engine as _FutureEngine
from sqlmodel import JSON as _JSON
from sqlmodel import (
    Column,
    Field,
    Relationship,
    Session,
    SQLModel,
    col,
    create_engine,
    or_,
    select,
)
from sqlmodel.sql.expression import Select, SelectOfScalar

from etl import config

log = structlog.get_logger()

# get rid of warning from https://github.com/tiangolo/sqlmodel/issues/189
SelectOfScalar.inherit_cache = True  # type: ignore
Select.inherit_cache = True  # type: ignore


metadata = SQLModel.metadata


# persist the value None as a SQL NULL value, not the JSON encoding of null
JSON = _JSON(none_as_null=True)


def get_engine() -> _FutureEngine:
    return create_engine(
        f"mysql://{config.DB_USER}:{quote(config.DB_PASS)}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}",
        future=False,
    )


t_active_datasets = Table(
    "active_datasets",
    metadata,
    Column("id", Integer, server_default=text("'0'")),
    Column("name", String(512)),
    Column("description", LONGTEXT),
    Column("createdAt", DateTime),
    Column("updatedAt", DateTime),
    Column("namespace", String(255)),
    Column("isPrivate", TINYINT(1), server_default=text("'0'")),
    Column("createdByUserId", Integer),
    Column("metadataEditedAt", DateTime),
    Column("metadataEditedByUserId", Integer),
    Column("dataEditedAt", DateTime),
    Column("dataEditedByUserId", Integer),
    Column("nonRedistributable", TINYINT(1), server_default=text("'0'")),
    Column("isArchived", TINYINT(1), server_default=text("'0'")),
    Column("sourceChecksum", String(64)),
    extend_existing=True,
)


class Entity(SQLModel, table=True):
    __tablename__: str = "entities"  # type: ignore
    __table_args__ = (Index("code", "code", unique=True), Index("name", "name", unique=True), {"extend_existing": True})

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    name: str = Field(sa_column=Column("name", VARCHAR(255), nullable=False))
    validated: int = Field(sa_column=Column("validated", TINYINT(1), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    displayName: str = Field(sa_column=Column("displayName", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    code: Optional[str] = Field(default=None, sa_column=Column("code", String(255, "utf8mb4_0900_as_cs")))


class Namespace(SQLModel, table=True):
    __tablename__: str = "namespaces"  # type: ignore
    __table_args__ = (
        Index("namespaces_name_uq", "name", unique=True),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=Column("name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    isArchived: int = Field(
        default=0, sa_column=Column("isArchived", TINYINT(1), nullable=False, server_default=text("'0'"))
    )
    description: Optional[str] = Field(default=None, sa_column=Column("description", String(255, "utf8mb4_0900_as_cs")))

    def upsert(self, session: Session) -> "Namespace":
        cls = self.__class__
        q = select(cls).where(
            cls.name == self.name,
        )
        ns = session.exec(q).one_or_none()
        if ns is None:
            ns = self
        else:
            ns.description = self.description

        session.add(ns)

        # select added object to get its id
        q = select(cls).where(
            cls.name == self.name,
        )
        return session.exec(q).one()


class Posts(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    title: str = Field(sa_column=Column("title", MEDIUMTEXT, nullable=False))
    slug: str = Field(sa_column=Column("slug", MEDIUMTEXT, nullable=False))
    type: str = Field(sa_column=Column("type", MEDIUMTEXT, nullable=False))
    status: str = Field(sa_column=Column("status", MEDIUMTEXT, nullable=False))
    content: str = Field(sa_column=Column("content", LONGTEXT, nullable=False))
    updated_at: datetime = Field(sa_column=Column("updated_at", DateTime, nullable=False))
    published_at: Optional[datetime] = Field(default=None, sa_column=Column("published_at", DateTime))

    tag: List["Tag"] = Relationship(back_populates="post")


t_post_tags = Table(
    "post_tags",
    metadata,
    Column("post_id", Integer, primary_key=True, nullable=False),
    Column("tag_id", Integer, primary_key=True, nullable=False),
    ForeignKeyConstraint(["post_id"], ["posts.id"], ondelete="CASCADE", name="FK_post_tags_post_id"),
    ForeignKeyConstraint(["tag_id"], ["tags.id"], ondelete="CASCADE", name="FK_post_tags_tag_id"),
    Index("FK_post_tags_tag_id", "tag_id"),
    extend_existing=True,
)


class Tag(SQLModel, table=True):
    __tablename__: str = "tags"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["parentId"], ["tags.id"], name="tags_ibfk_1"),
        Index("dataset_subcategories_name_fk_dst_cat_id_6ce1cc36_uniq", "name", "parentId", unique=True),
        Index("parentId", "parentId"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    name: str = Field(sa_column=Column("name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    isBulkImport: int = Field(sa_column=Column("isBulkImport", TINYINT(1), nullable=False, server_default=text("'0'")))
    parentId: Optional[int] = Field(default=None, sa_column=Column("parentId", Integer))
    specialType: Optional[str] = Field(default=None, sa_column=Column("specialType", String(255, "utf8mb4_0900_as_cs")))

    post: List["Posts"] = Relationship(back_populates="tag")
    tags: Optional["Tag"] = Relationship(back_populates="tags_reverse")
    tags_reverse: List["Tag"] = Relationship(back_populates="tags")
    datasets: List["Dataset"] = Relationship(back_populates="tags")
    chart_tags: List["ChartTags"] = Relationship(back_populates="tags")


class User(SQLModel, table=True):
    __tablename__: str = "users"  # type: ignore
    __table_args__ = (
        Index("email", "email", unique=True),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    password: str = Field(sa_column=Column("password", String(128, "utf8mb4_0900_as_cs"), nullable=False))
    isSuperuser: int = Field(sa_column=Column("isSuperuser", TINYINT(1), nullable=False, server_default=text("'0'")))
    email: str = Field(sa_column=Column("email", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    isActive: int = Field(sa_column=Column("isActive", TINYINT(1), nullable=False, server_default=text("'1'")))
    fullName: str = Field(sa_column=Column("fullName", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    lastLogin: Optional[datetime] = Field(default=None, sa_column=Column("lastLogin", DateTime))
    lastSeen: Optional[datetime] = Field(default=None, sa_column=Column("lastSeen", DateTime))

    chart_revisions: List["ChartRevisions"] = Relationship(back_populates="users")
    charts: List["Chart"] = Relationship(back_populates="users")
    charts_: List["Chart"] = Relationship(back_populates="users_")
    datasets: List["Dataset"] = Relationship(back_populates="users")
    datasets_: List["Dataset"] = Relationship(back_populates="users_")
    datasets1: List["Dataset"] = Relationship(back_populates="users1")
    suggested_chart_revisions: List["SuggestedChartRevisions"] = Relationship(back_populates="users")
    suggested_chart_revisions_: List["SuggestedChartRevisions"] = Relationship(back_populates="users_")


class ChartRevisions(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["userId"], ["users.id"], name="chart_revisions_userId"),
        Index("chartId", "chartId"),
        Index("chart_revisions_userId", "userId"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", BigInteger, primary_key=True))
    chartId: Optional[int] = Field(default=None, sa_column=Column("chartId", Integer))
    userId: Optional[int] = Field(default=None, sa_column=Column("userId", Integer))
    config: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("config", JSON))
    createdAt: Optional[datetime] = Field(default=None, sa_column=Column("createdAt", DateTime))
    updatedAt: Optional[datetime] = Field(default=None, sa_column=Column("updatedAt", DateTime))

    users: Optional["User"] = Relationship(back_populates="chart_revisions")


class Chart(SQLModel, table=True):
    __tablename__: str = "charts"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["lastEditedByUserId"], ["users.id"], name="charts_lastEditedByUserId"),
        ForeignKeyConstraint(["publishedByUserId"], ["users.id"], name="charts_publishedByUserId"),
        Index("charts_lastEditedByUserId", "lastEditedByUserId"),
        Index("charts_publishedByUserId", "publishedByUserId"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    config: Dict[Any, Any] = Field(sa_column=Column("config", JSON, nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    lastEditedAt: datetime = Field(sa_column=Column("lastEditedAt", DateTime, nullable=False))
    lastEditedByUserId: int = Field(sa_column=Column("lastEditedByUserId", Integer, nullable=False))
    is_indexable: int = Field(sa_column=Column("is_indexable", TINYINT(1), nullable=False, server_default=text("'0'")))
    isExplorable: int = Field(sa_column=Column("isExplorable", TINYINT(1), nullable=False, server_default=text("'0'")))
    publishedAt: Optional[datetime] = Field(default=None, sa_column=Column("publishedAt", DateTime))
    publishedByUserId: Optional[int] = Field(default=None, sa_column=Column("publishedByUserId", Integer))

    users: Optional["User"] = Relationship(back_populates="charts")
    users_: Optional["User"] = Relationship(back_populates="charts_")
    chart_slug_redirects: List["ChartSlugRedirects"] = Relationship(back_populates="chart")
    chart_tags: List["ChartTags"] = Relationship(back_populates="charts")
    suggested_chart_revisions: List["SuggestedChartRevisions"] = Relationship(back_populates="charts")
    chart_dimensions: List["ChartDimensions"] = Relationship(back_populates="charts")

    @classmethod
    def load_chart(cls, session: Session, chart_id: int) -> "Chart":
        """Load chart with id `chart_id`."""
        return session.exec(select(cls).where(cls.id == chart_id)).one()

    @classmethod
    def load_charts_using_variables(cls, session: Session, variable_ids: List[int]) -> List["Chart"]:
        """Load charts that use any of the given variables in `variable_ids`."""
        # Find IDs of charts
        chart_ids = (
            session.exec(select(ChartDimensions.chartId).where(ChartDimensions.variableId.in_(variable_ids)))  # type: ignore
            .unique()
            .all()
        )
        # Find charts
        return session.exec(select(Chart).where(Chart.id.in_(chart_ids))).all()  # type: ignore


class Dataset(SQLModel, table=True):
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

    __tablename__: str = "datasets"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["createdByUserId"], ["users.id"], name="datasets_createdByUserId"),
        ForeignKeyConstraint(["dataEditedByUserId"], ["users.id"], name="datasets_dataEditedByUserId"),
        ForeignKeyConstraint(["metadataEditedByUserId"], ["users.id"], name="datasets_metadataEditedByUserId"),
        Index("datasets_createdByUserId", "createdByUserId"),
        Index("datasets_dataEditedByUserId", "dataEditedByUserId"),
        Index("datasets_metadataEditedByUserId", "metadataEditedByUserId"),
        Index("datasets_name_namespace_d3d60d22_uniq", "name", "namespace", unique=True),
        Index("unique_short_name_version_namespace", "shortName", "version", "namespace", unique=True),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    # NOTE: name allows nulls in MySQL, but there are none in reality
    name: str = Field(sa_column=Column("name", String(512, "utf8mb4_0900_as_cs")))
    shortName: Optional[str] = Field(default=None, sa_column=Column("shortName", String(255, "utf8mb4_0900_as_cs")))
    version: Optional[str] = Field(default=None, sa_column=Column("version", String(255, "utf8mb4_0900_as_cs")))
    namespace: str = Field(sa_column=Column("namespace", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    description: str = Field(sa_column=Column("description", LONGTEXT, nullable=False))
    createdAt: Optional[datetime] = Field(
        default_factory=datetime.utcnow, sa_column=Column("createdAt", DateTime, nullable=False)
    )
    updatedAt: Optional[datetime] = Field(
        default_factory=datetime.utcnow, sa_column=Column("updatedAt", DateTime, nullable=False)
    )
    isPrivate: Optional[int] = Field(
        default=0, sa_column=Column("isPrivate", TINYINT(1), nullable=False, server_default=text("'0'"))
    )
    createdByUserId: int = Field(sa_column=Column("createdByUserId", Integer, nullable=False))
    metadataEditedAt: datetime = Field(
        default_factory=datetime.utcnow, sa_column=Column("metadataEditedAt", DateTime, nullable=False)
    )
    metadataEditedByUserId: int = Field(sa_column=Column("metadataEditedByUserId", Integer, nullable=False))
    dataEditedAt: datetime = Field(
        default_factory=datetime.utcnow, sa_column=Column("dataEditedAt", DateTime, nullable=False)
    )
    dataEditedByUserId: int = Field(sa_column=Column("dataEditedByUserId", Integer, nullable=False))
    nonRedistributable: int = Field(
        default=0, sa_column=Column("nonRedistributable", TINYINT(1), nullable=False, server_default=text("'0'"))
    )
    isArchived: Optional[int] = Field(
        default=0, sa_column=Column("isArchived", TINYINT(1), nullable=False, server_default=text("'0'"))
    )
    sourceChecksum: Optional[str] = Field(
        default=None, sa_column=Column("sourceChecksum", String(64, "utf8mb4_0900_as_cs"))
    )
    updatePeriodDays: Optional[int] = Field(sa_column=Column("updatePeriodDays", Integer, nullable=True))

    users: Optional["User"] = Relationship(back_populates="datasets")
    users_: Optional["User"] = Relationship(back_populates="datasets_")
    users1: Optional["User"] = Relationship(back_populates="datasets1")
    tags: List["Tag"] = Relationship(back_populates="datasets")
    sources: List["Source"] = Relationship(back_populates="datasets")
    variables: List["Variable"] = Relationship(back_populates="datasets")

    def upsert(self, session: Session) -> "Dataset":
        cls = self.__class__
        q = select(cls).where(
            cls.shortName == self.shortName,
            cls.version == self.version,
            cls.namespace == self.namespace,
        )
        ds = session.exec(q).one_or_none()
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
        return session.exec(q).one()

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
        )

    @classmethod
    def load_dataset(cls, session: Session, dataset_id: int) -> "Dataset":
        return session.exec(select(cls).where(cls.id == dataset_id)).one()

    @classmethod
    def load_with_path(cls, session: Session, namespace: str, short_name: str, version: str) -> "Dataset":
        return session.exec(
            select(cls).where(cls.namespace == namespace, cls.shortName == short_name, cls.version == version)
        ).one()

    @classmethod
    def load_variables_for_dataset(cls, session: Session, dataset_id: int) -> list["Variable"]:
        vars = session.exec(select(Variable).where(Variable.datasetId == dataset_id)).all()
        assert vars, f"Dataset {dataset_id} has no variables"
        return vars


class ChartSlugRedirects(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["chart_id"], ["charts.id"], name="chart_slug_redirects_chart_id"),
        Index("chart_slug_redirects_chart_id", "chart_id"),
        Index("slug", "slug", unique=True),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    slug: str = Field(sa_column=Column("slug", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    chart_id: int = Field(sa_column=Column("chart_id", Integer, nullable=False))

    chart: Optional["Chart"] = Relationship(back_populates="chart_slug_redirects")


class ChartTags(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["chartId"], ["charts.id"], ondelete="CASCADE", name="FK_chart_tags_chartId"),
        ForeignKeyConstraint(["tagId"], ["tags.id"], name="FK_chart_tags_tagId"),
        Index("FK_chart_tags_tagId", "tagId"),
        {"extend_existing": True},
    )

    chartId: int = Field(sa_column=Column("chartId", Integer, primary_key=True, nullable=False))
    tagId: int = Field(sa_column=Column("tagId", Integer, primary_key=True, nullable=False))
    isKey: Optional[int] = Field(default=None, sa_column=Column("isKey", TINYINT))

    charts: Optional["Chart"] = Relationship(back_populates="chart_tags")
    tags: Optional["Tag"] = Relationship(back_populates="chart_tags")


t_dataset_files = Table(
    "dataset_files",
    metadata,
    Column("datasetId", Integer, nullable=False),
    Column("filename", String(255, "utf8mb4_0900_as_cs"), nullable=False),
    Column("file", LONGBLOB, nullable=False),
    ForeignKeyConstraint(["datasetId"], ["datasets.id"], name="dataset_files_datasetId"),
    Index("dataset_files_datasetId", "datasetId"),
    extend_existing=True,
)


t_dataset_tags = Table(
    "dataset_tags",
    metadata,
    Column("datasetId", Integer, primary_key=True, nullable=False),
    Column("tagId", Integer, primary_key=True, nullable=False),
    ForeignKeyConstraint(["datasetId"], ["datasets.id"], ondelete="CASCADE", name="FK_fa434de5c36953f4efce6b073b3"),
    ForeignKeyConstraint(["tagId"], ["tags.id"], ondelete="CASCADE", name="FK_2e330c9e1074b457d1d238b2dac"),
    Index("FK_2e330c9e1074b457d1d238b2dac", "tagId"),
    extend_existing=True,
)


class SourceDescription(TypedDict, total=False):
    link: Optional[str]
    retrievedDate: Optional[str]
    additionalInfo: Optional[str]
    dataPublishedBy: Optional[str]
    dataPublisherSource: Optional[str]


class Source(SQLModel, table=True):
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

    __tablename__: str = "sources"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["datasetId"], ["datasets.id"], name="sources_datasetId"),
        Index("sources_datasetId", "datasetId"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    # NOTE: nested models are not supported yet in SQLModel so we use TypedDict instead
    # https://github.com/tiangolo/sqlmodel/issues/63
    description: SourceDescription = Field(sa_column=Column(JSON), nullable=False)
    createdAt: Optional[datetime] = Field(
        default_factory=datetime.utcnow, sa_column=Column("createdAt", DateTime, nullable=False)
    )
    updatedAt: Optional[datetime] = Field(
        default_factory=datetime.utcnow, sa_column=Column("updatedAt", DateTime, nullable=False)
    )
    name: Optional[str] = Field(default=None, sa_column=Column("name", String(512, "utf8mb4_0900_as_cs")))
    datasetId: Optional[int] = Field(default=None, sa_column=Column("datasetId", Integer))

    datasets: Optional["Dataset"] = Relationship(back_populates="sources")
    variables: List["Variable"] = Relationship(back_populates="sources")

    @property
    def _upsert_select(self) -> SelectOfScalar["Source"]:
        cls = self.__class__
        # NOTE: we match on both name and additionalInfo (source's description) so that we can
        # have sources with the same name, but different descriptions
        conds = [
            cls.name == self.name,
            cls.datasetId == self.datasetId,
            _json_is(cls.description, "additionalInfo", self.description.get("additionalInfo")),
            _json_is(cls.description, "dataPublishedBy", self.description.get("dataPublishedBy")),
        ]
        return select(cls).where(*conds)  # type: ignore

    def upsert(self, session: Session) -> "Source":
        ds = session.exec(self._upsert_select).one_or_none()

        if not ds:
            ds = self
        else:
            ds.updatedAt = datetime.utcnow()
            ds.description = self.description

        session.add(ds)

        # select added object to get its id
        return session.exec(self._upsert_select).one()

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
        return session.exec(select(cls).where(cls.id == source_id)).one()

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
        sources = pd.read_sql(
            q,
            session.bind,
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

        return [cls(**d) for d in sources.to_dict(orient="records") if cls.validate(d)]


class SuggestedChartRevisions(SQLModel, table=True):
    __tablename__: str = "suggested_chart_revisions"
    __table_args__ = (
        ForeignKeyConstraint(["chartId"], ["charts.id"], name="suggested_chart_revisions_ibfk_1"),
        ForeignKeyConstraint(["createdBy"], ["users.id"], name="suggested_chart_revisions_ibfk_2"),
        ForeignKeyConstraint(["updatedBy"], ["users.id"], name="suggested_chart_revisions_ibfk_3"),
        Index("chartId", "chartId", "originalVersion", "suggestedVersion", "isPendingOrFlagged", unique=True),
        Index("createdBy", "createdBy"),
        Index("updatedBy", "updatedBy"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", BigInteger, primary_key=True))
    chartId: int = Field(sa_column=Column("chartId", Integer, nullable=False))
    createdBy: int = Field(sa_column=Column("createdBy", Integer, nullable=False))
    originalConfig: Dict[Any, Any] = Field(sa_column=Column("originalConfig", JSON, nullable=False))
    suggestedConfig: Dict[Any, Any] = Field(sa_column=Column("suggestedConfig", JSON, nullable=False))
    status: str = Field(sa_column=Column("status", String(8, "utf8mb4_0900_as_cs"), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    updatedBy: Optional[int] = Field(default=None, sa_column=Column("updatedBy", Integer))
    suggestedReason: Optional[str] = Field(
        default=None, sa_column=Column("suggestedReason", String(512, "utf8mb4_0900_as_cs"))
    )
    decisionReason: Optional[str] = Field(
        default=None, sa_column=Column("decisionReason", String(512, "utf8mb4_0900_as_cs"))
    )
    originalVersion: Optional[int] = Field(
        default=None,
        sa_column=Column(
            "originalVersion",
            Integer,
            Computed("(json_unquote(json_extract(`originalConfig`,_utf8mb4'$.version')))", persisted=False),
        ),
    )
    suggestedVersion: Optional[int] = Field(
        default=None,
        sa_column=Column(
            "suggestedVersion",
            Integer,
            Computed("(json_unquote(json_extract(`suggestedConfig`,_utf8mb4'$.version')))", persisted=False),
        ),
    )
    isPendingOrFlagged: Optional[int] = Field(
        default=None,
        sa_column=Column(
            "isPendingOrFlagged",
            TINYINT(1),
            Computed("(if((`status` in (_utf8mb4'pending',_utf8mb4'flagged')),true,NULL))", persisted=False),
        ),
    )
    changesInDataSummary: Optional[str] = Field(
        default=None, sa_column=Column("changesInDataSummary", String(512, "utf8mb4_0900_as_cs"))
    )
    experimental: Optional[Dict[Any, Any]] = Field(sa_column=Column("experimental", JSON, nullable=False))

    charts: Optional["Chart"] = Relationship(back_populates="suggested_chart_revisions")
    users: Optional["User"] = Relationship(back_populates="suggested_chart_revisions")
    users_: Optional["User"] = Relationship(back_populates="suggested_chart_revisions_")

    @classmethod
    def load_pending(cls, session: Session, user_id: Optional[int] = None):
        if user_id is None:
            return session.exec(
                select(SuggestedChartRevisions).where((SuggestedChartRevisions.status == "pending"))
            ).all()
        else:
            return session.exec(
                select(SuggestedChartRevisions)
                .where(SuggestedChartRevisions.status == "pending")
                .where(SuggestedChartRevisions.createdBy == user_id)
            ).all()


class DimensionFilter(TypedDict):
    name: str
    value: Any


class Dimensions(TypedDict):
    originalShortName: str
    originalName: str
    filters: List[DimensionFilter]


class PostsGdocs(SQLModel, table=True):
    __tablename__ = "posts_gdocs"  # type: ignore

    id: Optional[str] = Field(default=None, sa_column=Column("id", VARCHAR(255), primary_key=True))
    slug: str = Field(sa_column=Column("slug", VARCHAR(255), nullable=False))
    content: dict = Field(sa_column=Column("content", JSON, nullable=False))
    published: int = Field(sa_column=Column("published", TINYINT, nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    publicationContext: str = Field(
        sa_column=Column(
            "publicationContext", ENUM("unlisted", "listed"), nullable=False, server_default=text("'unlisted'")
        )
    )
    publishedAt: Optional[datetime] = Field(default=None, sa_column=Column("publishedAt", DateTime))
    updatedAt: Optional[datetime] = Field(default=None, sa_column=Column("updatedAt", DateTime))
    revisionId: Optional[str] = Field(default=None, sa_column=Column("revisionId", VARCHAR(255)))


class OriginsVariablesLink(SQLModel, table=True):
    __tablename__: str = "origins_variables"  # type: ignore

    originId: Optional[int] = Field(default=None, foreign_key="origins.id", primary_key=True)
    variableId: Optional[int] = Field(default=None, foreign_key="variables.id", primary_key=True)

    @classmethod
    def link_with_variable(cls, session: Session, variable_id: int, new_origin_ids: Set[int]) -> None:
        """Link the given Variable ID with the given Origin IDs."""
        # Fetch current linked Origins for the given Variable ID
        existing_links = session.query(cls.originId).filter(cls.variableId == variable_id).all()

        existing_origin_ids = {link.originId for link in existing_links}

        # Find the Origin IDs to delete and the IDs to add
        to_delete_ids = existing_origin_ids - new_origin_ids
        to_add_ids = new_origin_ids - existing_origin_ids

        # Delete the obsolete Origin-Variable links
        if to_delete_ids:
            session.query(cls).filter(cls.variableId == variable_id, col(cls.originId).in_(to_delete_ids)).delete(
                synchronize_session="fetch"
            )

        # Add the new Origin-Variable links
        if to_add_ids:
            session.add_all([cls(originId=origin_id, variableId=variable_id) for origin_id in to_add_ids])


class PostsGdocsVariablesFaqsLink(SQLModel, table=True):
    __tablename__ = "posts_gdocs_variables_faqs"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["gdocId"], ["posts_gdocs.id"], name="posts_gdocs_variables_faqs_ibfk_1"),
        ForeignKeyConstraint(["variableId"], ["variables.id"], name="posts_gdocs_variables_faqs_ibfk_2"),
        Index("variableId", "variableId"),
    )

    gdocId: Optional[str] = Field(
        default=None, sa_column=Column("gdocId", VARCHAR(255), primary_key=True, nullable=False)
    )
    variableId: int = Field(sa_column=Column("variableId", Integer, primary_key=True, nullable=False))
    fragmentId: Optional[str] = Field(
        default=None, sa_column=Column("fragmentId", String(255), primary_key=True, nullable=False)
    )

    @classmethod
    def link_with_variable(cls, session: Session, variable_id: int, new_faqs: List[catalog.FaqLink]) -> None:
        """Link the given Variable ID with Faqs"""
        # Fetch current linked Faqs for the given Variable ID
        existing_faqs = session.query(cls).filter(cls.variableId == variable_id).all()

        # Work with tuples instead
        existing_gdoc_fragment = {(f.gdocId, f.fragmentId) for f in existing_faqs}
        new_gdoc_fragment = {(f.gdoc_id, f.fragment_id) for f in new_faqs}

        to_delete = existing_gdoc_fragment - new_gdoc_fragment
        to_add = new_gdoc_fragment - existing_gdoc_fragment

        # Delete the obsolete links
        for gdoc_id, fragment_id in to_delete:
            session.query(cls).filter(
                cls.variableId == variable_id, cls.gdocId == gdoc_id, cls.fragmentId == fragment_id
            ).delete(synchronize_session="fetch")

        # Add the new links
        if to_add:
            session.add_all(
                [cls(gdocId=gdoc_id, fragmentId=fragment_id, variableId=variable_id) for gdoc_id, fragment_id in to_add]
            )


class TagsVariablesTopicTagsLink(SQLModel, table=True):
    __tablename__ = "tags_variables_topic_tags"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["tagId"], ["tags.id"], name="tags_variables_topic_tags_ibfk_1"),
        ForeignKeyConstraint(["variableId"], ["variables.id"], name="tags_variables_topic_tags_ibfk_2"),
        Index("variableId", "variableId"),
    )

    tagId: Optional[str] = Field(default=None, sa_column=Column("tagId", Integer, primary_key=True, nullable=False))
    variableId: int = Field(sa_column=Column("variableId", Integer, primary_key=True, nullable=False))

    @classmethod
    def link_with_variable(cls, session: Session, variable_id: int, new_tag_ids: Set[str]) -> None:
        """Link the given Variable ID with the given Tag IDs."""
        # Fetch current linked tags for the given Variable ID
        existing_links = session.query(cls.tagId).filter(cls.variableId == variable_id).all()

        existing_tag_ids = {link.tagId for link in existing_links}

        # Find the tag IDs to delete and the IDs to add
        to_delete_ids = existing_tag_ids - new_tag_ids
        to_add_ids = new_tag_ids - existing_tag_ids

        # Delete the obsolete links
        if to_delete_ids:
            session.query(cls).filter(cls.variableId == variable_id, col(cls.tagId).in_(to_delete_ids)).delete(
                synchronize_session="fetch"
            )

        # Add the new links
        if to_add_ids:
            session.add_all([cls(tagId=tag_id, variableId=variable_id) for tag_id in to_add_ids])


class Variable(SQLModel, table=True):
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

    __tablename__: str = "variables"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["datasetId"], ["datasets.id"], name="variables_datasetId_50a98bfd_fk_datasets_id"),
        ForeignKeyConstraint(["sourceId"], ["sources.id"], name="variables_sourceId_31fce80a_fk_sources_id"),
        Index("unique_short_name_per_dataset", "shortName", "datasetId", unique=True),
        Index("variables_code_fk_dst_id_7bde8c2a_uniq", "code", "datasetId", unique=True),
        Index("variables_datasetId_50a98bfd_fk_datasets_id", "datasetId"),
        Index("variables_name_fk_dst_id_f7453c33_uniq", "name", "datasetId", unique=True),
        Index("variables_sourceId_31fce80a_fk_sources_id", "sourceId"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    datasetId: int = Field(sa_column=Column("datasetId", Integer, nullable=False))
    name: Optional[str] = Field(default=None, sa_column=Column("name", String(750, "utf8mb4_0900_as_cs")))
    shortName: Optional[str] = Field(default=None, sa_column=Column("shortName", String(255, "utf8mb4_0900_as_cs")))
    description: Optional[str] = Field(default=None, sa_column=Column("description", LONGTEXT))
    unit: str = Field(sa_column=Column("unit", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    createdAt: datetime = Field(
        default_factory=datetime.utcnow, sa_column=Column("createdAt", DateTime, nullable=False)
    )
    updatedAt: datetime = Field(
        default_factory=datetime.utcnow, sa_column=Column("updatedAt", DateTime, nullable=False)
    )
    coverage: str = Field(sa_column=Column("coverage", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    timespan: str = Field(sa_column=Column("timespan", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    sourceId: Optional[int] = Field(sa_column=Column("sourceId", Integer, nullable=True))
    display: Dict[str, Any] = Field(sa_column=Column("display", JSON, nullable=False))
    columnOrder: int = Field(
        default=0, sa_column=Column("columnOrder", Integer, nullable=False, server_default=text("'0'"))
    )
    code: Optional[str] = Field(default=None, sa_column=Column("code", String(255, "utf8mb4_0900_as_cs")))
    shortUnit: Optional[str] = Field(default=None, sa_column=Column("shortUnit", String(255, "utf8mb4_0900_as_cs")))
    originalMetadata: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("originalMetadata", JSON))
    grapherConfigAdmin: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("grapherConfigAdmin", JSON))
    grapherConfigETL: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("grapherConfigETL", JSON))
    catalogPath: Optional[str] = Field(default=None, sa_column=Column("catalogPath", LONGTEXT))
    dimensions: Optional[Dimensions] = Field(sa_column=Column("dimensions", JSON, nullable=True))

    schemaVersion: Optional[int] = Field(default=None, sa_column=Column("schemaVersion", Integer))
    # processingLevel: Optional[str] = Field(
    #     default=None, sa_column=Column("processingLevel", ENUM("minor", "medium", "major"))
    # )
    processingLevel: Optional[Optional[Annotated[str, catalog.meta.PROCESSING_LEVELS]]] = Field(default=None)
    processingLog: Optional[dict] = Field(default=None, sa_column=Column("processingLog", JSON))
    titlePublic: Optional[str] = Field(default=None, sa_column=Column("titlePublic", LONGTEXT))
    titleVariant: Optional[str] = Field(default=None, sa_column=Column("titleVariant", LONGTEXT))
    attributionShort: Optional[str] = Field(default=None, sa_column=Column("attributionShort", LONGTEXT))
    attribution: Optional[str] = Field(default=None, sa_column=Column("attribution", LONGTEXT))
    descriptionShort: Optional[str] = Field(default=None, sa_column=Column("descriptionShort", LONGTEXT))
    descriptionFromProducer: Optional[str] = Field(default=None, sa_column=Column("descriptionFromProducer", LONGTEXT))
    descriptionKey: Optional[List[str]] = Field(default=None, sa_column=Column("descriptionKey", JSON))
    descriptionProcessing: Optional[str] = Field(default=None, sa_column=Column("descriptionProcessing", LONGTEXT))
    # NOTE: Use of `licenses` is discouraged, they should be captured in origins.
    licenses: Optional[List[dict]] = Field(default=None, sa_column=Column("licenses", JSON))
    # NOTE: License should be the resulting license, given all licenses of the indicator’s origins and given the indicator’s processing level.
    license: Optional[dict] = Field(default=None, sa_column=Column("license", JSON))

    datasets: Optional["Dataset"] = Relationship(back_populates="variables")
    sources: Optional["Source"] = Relationship(back_populates="variables")
    chart_dimensions: List["ChartDimensions"] = Relationship(back_populates="variables")
    origins: List["Origin"] = Relationship(back_populates="origins", link_model=OriginsVariablesLink)
    posts_gdocs: List["PostsGdocs"] = Relationship(back_populates="posts_gdocs", link_model=PostsGdocsVariablesFaqsLink)

    def upsert(self, session: Session) -> "Variable":
        assert self.shortName

        cls = self.__class__

        # try matching on shortName first
        q = select(cls).where(
            or_(
                cls.shortName == self.shortName,  # type: ignore
                # NOTE: we used to slugify shortName which replaced double underscore by a single underscore
                # this was a bug, we should have kept the double underscore
                # match even those variables and correct their shortName
                cls.shortName == self.shortName.replace("__", "_"),  # type: ignore
            ),
            cls.datasetId == self.datasetId,
        )
        ds = session.exec(q).one_or_none()

        # try matching on name if there was no match on shortName
        if not ds:
            q = select(cls).where(
                cls.name == self.name,
                cls.datasetId == self.datasetId,
            )
            ds = session.exec(q).one_or_none()

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
            assert self.grapherConfigAdmin is None, "grapherConfigETL should be used instead of grapherConfigAdmin"

        session.add(ds)

        # select added object to get its id
        q = select(cls).where(
            cls.shortName == self.shortName,
            cls.datasetId == self.datasetId,
        )
        return session.exec(q).one()

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

        if metadata.presentation:
            presentation_dict = metadata.presentation.to_dict()  # type: ignore
            # convert all fields from snake_case to camelCase
            presentation_dict = humps.camelize(presentation_dict)
        else:
            presentation_dict = {}

        # TODO: implement `topicTagsLinks`
        presentation_dict.pop("topicTagsLinks", None)

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
            licenses=[license.to_dict() for license in metadata.licenses] if metadata.licenses else None,
            license=metadata.license.to_dict() if metadata.license else None,
            **presentation_dict,
        )

    @classmethod
    def load_variable(cls, session: Session, variable_id: int) -> "Variable":
        return session.exec(select(cls).where(cls.id == variable_id)).one()

    @classmethod
    def load_variables(cls, session: Session, variables_id: List[int]) -> List["Variable"]:
        return session.exec(select(cls).where(cls.id.in_(variables_id))).all()  # type: ignore

    @classmethod
    def load_from_catalog_path(cls, catalog_path: str, session: Optional[Session] = None) -> "Variable":
        def _run(ses: Session):
            return ses.exec(select(cls).where(cls.catalogPath == catalog_path)).one()

        if session is None:
            with Session(get_engine()) as session:
                variable = _run(session)
        else:
            variable = _run(session)
        return variable

    def update_links(
        self, session: Session, db_origins: List["Origin"], faqs: List[catalog.FaqLink], tag_names: List[str]
    ):
        """
        Establishes relationships between the current variable and a list of origins and a list of posts.
        """
        assert self.id

        # establish relationships between variables and origins
        if db_origins:
            OriginsVariablesLink.link_with_variable(session, self.id, {origin.id for origin in db_origins})  # type: ignore

        # establish relationships between variables and posts
        if faqs:
            required_gdoc_ids = {faq.gdoc_id for faq in faqs}
            query = select(PostsGdocs).where(PostsGdocs.id.in_(required_gdoc_ids))  # type: ignore
            gdoc_posts = session.exec(query).all()
            existing_gdoc_ids = {gdoc_post.id for gdoc_post in gdoc_posts}
            missing_gdoc_ids = required_gdoc_ids - existing_gdoc_ids
            if missing_gdoc_ids:
                log.warning("create_links.missing_faqs", missing_gdoc_ids=missing_gdoc_ids)

            faqs_to_insert = [faq for faq in faqs if faq.gdoc_id in existing_gdoc_ids]

            if faqs_to_insert:
                PostsGdocsVariablesFaqsLink.link_with_variable(session, self.id, faqs_to_insert)

        # establish relationships between variables and tags
        if tag_names:
            # get tags by their name
            tags = session.exec(select(Tag).where(Tag.name.in_(tag_names))).all()  # type: ignore

            # raise a warning if some tags were not found
            if len(tags) != len(tag_names):
                found_tags = [tag.name for tag in tags]
                missing_tags = [tag for tag in tag_names if tag not in found_tags]
                log.warning("create_links.missing_tags", tags=missing_tags)

            tag_ids = {tag.id for tag in tags}
            if tag_ids:
                TagsVariablesTopicTagsLink.link_with_variable(session, self.id, tag_ids)  # type: ignore

    def s3_data_path(self) -> str:
        """Path to S3 with data in JSON format for Grapher. Typically
        s3://owid-api/v1/indicators/123.data.json."""
        return f"{config.BAKED_VARIABLES_PATH}/{self.id}.data.json"

    def s3_metadata_path(self) -> str:
        """Path to S3 with metadata in JSON format for Grapher. Typically
        s3://owid-api/v1/indicators/123.metadata.json."""
        return f"{config.BAKED_VARIABLES_PATH}/{self.id}.metadata.json"


class ChartDimensions(SQLModel, table=True):
    __tablename__: str = "chart_dimensions"
    __table_args__ = (
        ForeignKeyConstraint(["chartId"], ["charts.id"], name="chart_dimensions_chartId_78d6a092_fk_charts_id"),
        ForeignKeyConstraint(
            ["variableId"], ["variables.id"], name="chart_dimensions_variableId_9ba778e6_fk_variables_id"
        ),
        Index("chart_dimensions_chartId_78d6a092_fk_charts_id", "chartId"),
        Index("chart_dimensions_variableId_9ba778e6_fk_variables_id", "variableId"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    order: int = Field(sa_column=Column("order", Integer, nullable=False))
    property: str = Field(sa_column=Column("property", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    chartId: int = Field(sa_column=Column("chartId", Integer, nullable=False))
    variableId: int = Field(sa_column=Column("variableId", Integer, nullable=False))

    charts: Optional["Chart"] = Relationship(back_populates="chart_dimensions")
    variables: Optional["Variable"] = Relationship(back_populates="chart_dimensions")


t_country_latest_data = Table(
    "country_latest_data",
    metadata,
    Column("country_code", String(255, "utf8mb4_0900_as_cs")),
    Column("variable_id", Integer),
    Column("year", Integer),
    Column("value", String(255, "utf8mb4_0900_as_cs")),
    ForeignKeyConstraint(["variable_id"], ["variables.id"], name="country_latest_data_variable_id_foreign"),
    Index("country_latest_data_country_code_variable_id_unique", "country_code", "variable_id", unique=True),
    Index("country_latest_data_variable_id_foreign", "variable_id"),
    extend_existing=True,
)


class Origin(SQLModel, table=True):
    """Get CREATE TABLE statement for origins table with
    ```
    from sqlalchemy.schema import CreateTable
    from etl.grapher_model import Origin
    print(str(CreateTable(Origin.__table__).compile(engine)))
    ```
    """

    __tablename__: str = "origins"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)
    producer: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    titleSnapshot: Optional[str] = None
    descriptionSnapshot: Optional[str] = None
    citationFull: Optional[str] = None
    attribution: Optional[str] = None
    attributionShort: Optional[str] = None
    versionProducer: Optional[str] = None
    urlMain: Optional[str] = None
    urlDownload: Optional[str] = None
    dateAccessed: Optional[date] = None
    datePublished: Optional[date] = None
    license: Optional[dict] = Field(default=None, sa_column=Column("license", JSON))

    variables: list["Variable"] = Relationship(back_populates="origins", link_model=OriginsVariablesLink)

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
            dateAccessed=origin.date_accessed,
        )

    @property
    def _upsert_select(self) -> SelectOfScalar["Origin"]:
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
        )  # type: ignore

    def upsert(self, session: Session) -> "Origin":
        origin = session.exec(self._upsert_select).one_or_none()
        if origin is None:
            # create new origin
            origin = self
        else:
            # we match on all fields, so there's nothing to update
            pass

        session.add(origin)

        # select added object to get its id
        return session.exec(self._upsert_select).one()


def _json_is(json_field: Any, key: str, val: Any) -> Any:
    """SQLAlchemy condition for checking if a JSON field has a key with a given value. Works for null."""
    if val is None:
        return text(f"JSON_VALUE({json_field.key}, '$.{key}') IS NULL")
    else:
        return json_field[key] == val
