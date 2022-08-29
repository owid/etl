"""This schema was generated using https://github.com/agronholm/sqlacodegen library with the following command:
```
sqlacodegen --generator sqlmodels mysql://root@localhost:3306/owid
```
It has been slightly modified since then.
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import pandas as pd
import structlog
from pydantic import BaseModel
from sqlalchemy import (
    JSON,
    TIMESTAMP,
    BigInteger,
    Column,
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
    INTEGER,
    LONGBLOB,
    LONGTEXT,
    MEDIUMTEXT,
    TINYINT,
    VARCHAR,
)
from sqlalchemy.engine import Engine
from sqlalchemy.future import Engine as _FutureEngine
from sqlmodel import (
    JSON,
    Column,
    Field,
    Relationship,
    Session,
    SQLModel,
    create_engine,
    select,
)
from sqlmodel.sql.expression import Select, SelectOfScalar

from etl import config

log = structlog.get_logger()

# get rid of warning from https://github.com/tiangolo/sqlmodel/issues/189
SelectOfScalar.inherit_cache = True  # type: ignore
Select.inherit_cache = True  # type: ignore

metadata = SQLModel.metadata


def get_engine() -> _FutureEngine:
    return create_engine(
        f"mysql://{config.DB_USER}:{quote(config.DB_PASS)}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
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
)


class CountryNameToolContinent(SQLModel, table=True):
    __table_args__ = (
        Index("continent_code", "continent_code", unique=True),
        Index("continent_name", "continent_name", unique=True),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    continent_code: str = Field(sa_column=Column("continent_code", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    continent_name: str = Field(sa_column=Column("continent_name", String(255, "utf8mb4_0900_as_cs"), nullable=False))

    country_name_tool_countrydata: List["CountryNameToolCountrydata"] = Relationship(
        back_populates="country_name_tool_continent"
    )


class Details(SQLModel, table=True):
    __table_args__ = (Index("category", "category", "term", unique=True),)

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    category: str = Field(sa_column=Column("category", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    term: str = Field(sa_column=Column("term", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    title: str = Field(sa_column=Column("title", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    content: str = Field(sa_column=Column("content", String(1023, "utf8mb4_0900_as_cs"), nullable=False))


class Entities(SQLModel, table=True):
    __table_args__ = (Index("code", "code", unique=True), Index("name", "name", unique=True))

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    name: str = Field(sa_column=Column("name", VARCHAR(255), nullable=False))
    validated: int = Field(sa_column=Column("validated", TINYINT(1), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    displayName: str = Field(sa_column=Column("displayName", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    code: Optional[str] = Field(default=None, sa_column=Column("code", String(255, "utf8mb4_0900_as_cs")))

    data_values: List["DataValues"] = Relationship(back_populates="entities")


class ImporterAdditionalcountryinfo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    country_code: str = Field(sa_column=Column("country_code", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    country_name: str = Field(sa_column=Column("country_name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    dataset: str = Field(sa_column=Column("dataset", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    country_wb_region: Optional[str] = Field(
        default=None, sa_column=Column("country_wb_region", String(255, "utf8mb4_0900_as_cs"))
    )
    country_wb_income_group: Optional[str] = Field(
        default=None, sa_column=Column("country_wb_income_group", String(255, "utf8mb4_0900_as_cs"))
    )
    country_special_notes: Optional[str] = Field(default=None, sa_column=Column("country_special_notes", LONGTEXT))
    country_latest_census: Optional[str] = Field(
        default=None, sa_column=Column("country_latest_census", String(255, "utf8mb4_0900_as_cs"))
    )
    country_latest_survey: Optional[str] = Field(
        default=None, sa_column=Column("country_latest_survey", String(255, "utf8mb4_0900_as_cs"))
    )
    country_recent_income_source: Optional[str] = Field(
        default=None, sa_column=Column("country_recent_income_source", String(255, "utf8mb4_0900_as_cs"))
    )


class ImporterImporthistory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    import_type: str = Field(sa_column=Column("import_type", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    import_time: datetime = Field(sa_column=Column("import_time", DateTime, nullable=False))
    import_notes: str = Field(sa_column=Column("import_notes", LONGTEXT, nullable=False))
    import_state: str = Field(sa_column=Column("import_state", LONGTEXT, nullable=False))


class KnexMigrations(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", INTEGER, primary_key=True))
    migration_time: datetime = Field(
        sa_column=Column(
            "migration_time",
            TIMESTAMP,
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        )
    )
    name: Optional[str] = Field(default=None, sa_column=Column("name", String(255, "utf8mb4_0900_as_cs")))
    batch: Optional[int] = Field(default=None, sa_column=Column("batch", Integer))


class KnexMigrationsLock(SQLModel, table=True):
    index: Optional[int] = Field(default=None, sa_column=Column("index", INTEGER, primary_key=True))
    is_locked: Optional[int] = Field(default=None, sa_column=Column("is_locked", Integer))


class Migrations(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    timestamp: int = Field(sa_column=Column("timestamp", BigInteger, nullable=False))
    name: str = Field(sa_column=Column("name", String(255, "utf8mb4_0900_as_cs"), nullable=False))


class Namespaces(SQLModel, table=True):
    __table_args__ = (Index("namespaces_name_uq", "name", unique=True),)

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    name: str = Field(sa_column=Column("name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    isArchived: int = Field(sa_column=Column("isArchived", TINYINT(1), nullable=False, server_default=text("'0'")))
    description: Optional[str] = Field(default=None, sa_column=Column("description", String(255, "utf8mb4_0900_as_cs")))


class Posts(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    title: str = Field(sa_column=Column("title", MEDIUMTEXT, nullable=False))
    slug: str = Field(sa_column=Column("slug", MEDIUMTEXT, nullable=False))
    type: str = Field(sa_column=Column("type", MEDIUMTEXT, nullable=False))
    status: str = Field(sa_column=Column("status", MEDIUMTEXT, nullable=False))
    content: str = Field(sa_column=Column("content", LONGTEXT, nullable=False))
    updated_at: datetime = Field(sa_column=Column("updated_at", DateTime, nullable=False))
    published_at: Optional[datetime] = Field(default=None, sa_column=Column("published_at", DateTime))

    tag: List["Tags"] = Relationship(back_populates="post")


class Sessions(SQLModel, table=True):
    __table_args__ = (Index("django_session_expire_date_a5c62663", "expire_date"),)

    session_key: Optional[str] = Field(
        default=None, sa_column=Column("session_key", String(40, "utf8mb4_0900_as_cs"), primary_key=True)
    )
    session_data: str = Field(sa_column=Column("session_data", LONGTEXT, nullable=False))
    expire_date: datetime = Field(sa_column=Column("expire_date", DateTime, nullable=False))


class Settings(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    meta_name: str = Field(sa_column=Column("meta_name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    meta_value: str = Field(sa_column=Column("meta_value", LONGTEXT, nullable=False))
    created_at: datetime = Field(sa_column=Column("created_at", DateTime, nullable=False))
    updated_at: datetime = Field(sa_column=Column("updated_at", DateTime, nullable=False))


class Tags(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["parentId"], ["tags.id"], name="tags_ibfk_1"),
        Index("dataset_subcategories_name_fk_dst_cat_id_6ce1cc36_uniq", "name", "parentId", unique=True),
        Index("parentId", "parentId"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    name: str = Field(sa_column=Column("name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    isBulkImport: int = Field(sa_column=Column("isBulkImport", TINYINT(1), nullable=False, server_default=text("'0'")))
    parentId: Optional[int] = Field(default=None, sa_column=Column("parentId", Integer))
    specialType: Optional[str] = Field(default=None, sa_column=Column("specialType", String(255, "utf8mb4_0900_as_cs")))

    post: List["Posts"] = Relationship(back_populates="tag")
    tags: Optional["Tags"] = Relationship(back_populates="tags_reverse")
    tags_reverse: List["Tags"] = Relationship(back_populates="tags")
    datasets: List["Datasets"] = Relationship(back_populates="tags")
    chart_tags: List["ChartTags"] = Relationship(back_populates="tags")


class UserInvitations(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    code: str = Field(sa_column=Column("code", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    email: str = Field(sa_column=Column("email", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    validTill: datetime = Field(sa_column=Column("validTill", DateTime, nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))


class Users(SQLModel, table=True):
    __table_args__ = (Index("email", "email", unique=True),)

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
    charts: List["Charts"] = Relationship(back_populates="users")
    charts_: List["Charts"] = Relationship(back_populates="users_")
    datasets: List["Datasets"] = Relationship(back_populates="users")
    datasets_: List["Datasets"] = Relationship(back_populates="users_")
    datasets1: List["Datasets"] = Relationship(back_populates="users1")
    suggested_chart_revisions: List["SuggestedChartRevisions"] = Relationship(back_populates="users")
    suggested_chart_revisions_: List["SuggestedChartRevisions"] = Relationship(back_populates="users_")


class ChartRevisions(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["userId"], ["users.id"], name="chart_revisions_userId"),
        Index("chartId", "chartId"),
        Index("chart_revisions_userId", "userId"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", BigInteger, primary_key=True))
    chartId: Optional[int] = Field(default=None, sa_column=Column("chartId", Integer))
    userId: Optional[int] = Field(default=None, sa_column=Column("userId", Integer))
    config: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("config", JSON))
    createdAt: Optional[datetime] = Field(default=None, sa_column=Column("createdAt", DateTime))
    updatedAt: Optional[datetime] = Field(default=None, sa_column=Column("updatedAt", DateTime))

    users: Optional["Users"] = Relationship(back_populates="chart_revisions")


class Charts(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["lastEditedByUserId"], ["users.id"], name="charts_lastEditedByUserId"),
        ForeignKeyConstraint(["publishedByUserId"], ["users.id"], name="charts_publishedByUserId"),
        Index("charts_lastEditedByUserId", "lastEditedByUserId"),
        Index("charts_publishedByUserId", "publishedByUserId"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    config: Dict[Any, Any]] = Field(sa_column=Column("config", JSON, nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    lastEditedAt: datetime = Field(sa_column=Column("lastEditedAt", DateTime, nullable=False))
    lastEditedByUserId: int = Field(sa_column=Column("lastEditedByUserId", Integer, nullable=False))
    is_indexable: int = Field(sa_column=Column("is_indexable", TINYINT(1), nullable=False, server_default=text("'0'")))
    isExplorable: int = Field(sa_column=Column("isExplorable", TINYINT(1), nullable=False, server_default=text("'0'")))
    publishedAt: Optional[datetime] = Field(default=None, sa_column=Column("publishedAt", DateTime))
    publishedByUserId: Optional[int] = Field(default=None, sa_column=Column("publishedByUserId", Integer))

    users: Optional["Users"] = Relationship(back_populates="charts")
    users_: Optional["Users"] = Relationship(back_populates="charts_")
    chart_slug_redirects: List["ChartSlugRedirects"] = Relationship(back_populates="chart")
    chart_tags: List["ChartTags"] = Relationship(back_populates="charts")
    suggested_chart_revisions: List["SuggestedChartRevisions"] = Relationship(back_populates="charts")
    chart_dimensions: List["ChartDimensions"] = Relationship(back_populates="charts")


class CountryNameToolCountrydata(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(
            ["continent"],
            ["country_name_tool_continent.id"],
            name="country_name_tool_co_continent_217c90d2_fk_country_n",
        ),
        Index("country_name_tool_co_continent_217c90d2_fk_country_n", "continent"),
        Index("cow_code", "cow_code", unique=True),
        Index("cow_letter", "cow_letter", unique=True),
        Index("imf_code", "imf_code", unique=True),
        Index("iso_alpha2", "iso_alpha2", unique=True),
        Index("iso_alpha3", "iso_alpha3", unique=True),
        Index("kansas_code", "kansas_code", unique=True),
        Index("marc_code", "marc_code", unique=True),
        Index("ncd_code", "ncd_code", unique=True),
        Index("owid_name", "owid_name", unique=True),
        Index("penn_code", "penn_code", unique=True),
        Index("unctad_code", "unctad_code", unique=True),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    owid_name: str = Field(sa_column=Column("owid_name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    iso_alpha2: Optional[str] = Field(default=None, sa_column=Column("iso_alpha2", String(255, "utf8mb4_0900_as_cs")))
    iso_alpha3: Optional[str] = Field(default=None, sa_column=Column("iso_alpha3", String(255, "utf8mb4_0900_as_cs")))
    imf_code: Optional[int] = Field(default=None, sa_column=Column("imf_code", Integer))
    cow_letter: Optional[str] = Field(default=None, sa_column=Column("cow_letter", String(255, "utf8mb4_0900_as_cs")))
    cow_code: Optional[int] = Field(default=None, sa_column=Column("cow_code", Integer))
    unctad_code: Optional[str] = Field(default=None, sa_column=Column("unctad_code", String(255, "utf8mb4_0900_as_cs")))
    marc_code: Optional[str] = Field(default=None, sa_column=Column("marc_code", String(255, "utf8mb4_0900_as_cs")))
    ncd_code: Optional[str] = Field(default=None, sa_column=Column("ncd_code", String(255, "utf8mb4_0900_as_cs")))
    kansas_code: Optional[str] = Field(default=None, sa_column=Column("kansas_code", String(255, "utf8mb4_0900_as_cs")))
    penn_code: Optional[str] = Field(default=None, sa_column=Column("penn_code", String(255, "utf8mb4_0900_as_cs")))
    continent: Optional[int] = Field(default=None, sa_column=Column("continent", Integer))

    country_name_tool_continent: Optional["CountryNameToolContinent"] = Relationship(
        back_populates="country_name_tool_countrydata"
    )
    country_name_tool_countryname: List["CountryNameToolCountryname"] = Relationship(
        back_populates="country_name_tool_countrydata"
    )


class Datasets(SQLModel, table=True):
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
    )

    id: int = Field(primary_key=True)
    # NOTE: name allows nulls in MySQL, but there are none in reality
    name: str = Field(sa_column=Column("name", String(512, "utf8mb4_0900_as_cs")))
    description: str = Field(sa_column=Column("description", LONGTEXT, nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    namespace: str = Field(sa_column=Column("namespace", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    isPrivate: Optional[int] = Field(
        default=0, sa_column=Column("isPrivate", TINYINT(1), nullable=False, server_default=text("'0'"))
    )
    createdByUserId: int = Field(sa_column=Column("createdByUserId", Integer, nullable=False))
    metadataEditedAt: datetime = Field(sa_column=Column("metadataEditedAt", DateTime, nullable=False))
    metadataEditedByUserId: int = Field(sa_column=Column("metadataEditedByUserId", Integer, nullable=False))
    dataEditedAt: datetime = Field(sa_column=Column("dataEditedAt", DateTime, nullable=False))
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
    shortName: Optional[str] = Field(default=None, sa_column=Column("shortName", String(255, "utf8mb4_0900_as_cs")))
    version: Optional[str] = Field(default=None, sa_column=Column("version", String(255, "utf8mb4_0900_as_cs")))

    users: Optional["Users"] = Relationship(back_populates="datasets")
    users_: Optional["Users"] = Relationship(back_populates="datasets_")
    users1: Optional["Users"] = Relationship(back_populates="datasets1")
    tags: List["Tags"] = Relationship(back_populates="datasets")
    sources: List["Sources"] = Relationship(back_populates="datasets")
    variables: List["Variables"] = Relationship(back_populates="datasets")

    @classmethod
    def load_dataset(cls, engine: Engine, dataset_id: int) -> "Datasets":
        with Session(engine) as session:
            return session.exec(select(cls).where(cls.id == dataset_id)).one()

    @classmethod
    def load_variables_for_dataset(cls, engine: Engine, dataset_id: int) -> list["Variables"]:
        with Session(engine) as session:
            vars = session.exec(select(Variables).where(Variables.datasetId == dataset_id)).all()
            assert vars
        return vars


t_post_tags = Table(
    "post_tags",
    metadata,
    Column("post_id", Integer, primary_key=True, nullable=False),
    Column("tag_id", Integer, primary_key=True, nullable=False),
    ForeignKeyConstraint(["post_id"], ["posts.id"], ondelete="CASCADE", name="FK_post_tags_post_id"),
    ForeignKeyConstraint(["tag_id"], ["tags.id"], ondelete="CASCADE", name="FK_post_tags_tag_id"),
    Index("FK_post_tags_tag_id", "tag_id"),
)


class ChartSlugRedirects(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["chart_id"], ["charts.id"], name="chart_slug_redirects_chart_id"),
        Index("chart_slug_redirects_chart_id", "chart_id"),
        Index("slug", "slug", unique=True),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    slug: str = Field(sa_column=Column("slug", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    chart_id: int = Field(sa_column=Column("chart_id", Integer, nullable=False))

    chart: Optional["Charts"] = Relationship(back_populates="chart_slug_redirects")


class ChartTags(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["chartId"], ["charts.id"], ondelete="CASCADE", name="FK_chart_tags_chartId"),
        ForeignKeyConstraint(["tagId"], ["tags.id"], name="FK_chart_tags_tagId"),
        Index("FK_chart_tags_tagId", "tagId"),
    )

    chartId: int = Field(sa_column=Column("chartId", Integer, primary_key=True, nullable=False))
    tagId: int = Field(sa_column=Column("tagId", Integer, primary_key=True, nullable=False))
    isKey: Optional[int] = Field(default=None, sa_column=Column("isKey", TINYINT))

    charts: Optional["Charts"] = Relationship(back_populates="chart_tags")
    tags: Optional["Tags"] = Relationship(back_populates="chart_tags")


class CountryNameToolCountryname(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(
            ["owid_country"],
            ["country_name_tool_countrydata.id"],
            name="country_name_tool_co_owid_country_fefc8efa_fk_country_n",
        ),
        Index("country_name", "country_name", unique=True),
        Index("country_name_tool_co_owid_country_fefc8efa_fk_country_n", "owid_country"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    country_name: str = Field(sa_column=Column("country_name", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    owid_country: int = Field(sa_column=Column("owid_country", Integer, nullable=False))

    country_name_tool_countrydata: Optional["CountryNameToolCountrydata"] = Relationship(
        back_populates="country_name_tool_countryname"
    )


t_dataset_files = Table(
    "dataset_files",
    metadata,
    Column("datasetId", Integer, nullable=False),
    Column("filename", String(255, "utf8mb4_0900_as_cs"), nullable=False),
    Column("file", LONGBLOB, nullable=False),
    ForeignKeyConstraint(["datasetId"], ["datasets.id"], name="dataset_files_datasetId"),
    Index("dataset_files_datasetId", "datasetId"),
)


t_dataset_tags = Table(
    "dataset_tags",
    metadata,
    Column("datasetId", Integer, primary_key=True, nullable=False),
    Column("tagId", Integer, primary_key=True, nullable=False),
    ForeignKeyConstraint(["datasetId"], ["datasets.id"], ondelete="CASCADE", name="FK_fa434de5c36953f4efce6b073b3"),
    ForeignKeyConstraint(["tagId"], ["tags.id"], ondelete="CASCADE", name="FK_2e330c9e1074b457d1d238b2dac"),
    Index("FK_2e330c9e1074b457d1d238b2dac", "tagId"),
)


class GrapherSourceDescription(BaseModel):
    link: Optional[str] = None
    retrievedDate: Optional[str] = None
    additionalInfo: Optional[str] = None
    dataPublishedBy: Optional[str] = None
    dataPublisherSource: Optional[str] = None


class Sources(SQLModel, table=True):
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

    __table_args__ = (
        ForeignKeyConstraint(["datasetId"], ["datasets.id"], name="sources_datasetId"),
        Index("sources_datasetId", "datasetId"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    # NOTE: description is not converted into GrapherSourceDescription object automatically, I haven't
    # found an easy solution how to do it, but there's some momentum https://github.com/tiangolo/sqlmodel/issues/63
    description: GrapherSourceDescription = Field(sa_column=Column(JSON), nullable=False)
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    name: Optional[str] = Field(default=None, sa_column=Column("name", String(512, "utf8mb4_0900_as_cs")))
    datasetId: Optional[int] = Field(default=None, sa_column=Column("datasetId", Integer))

    datasets: Optional["Datasets"] = Relationship(back_populates="sources")
    variables: List["Variables"] = Relationship(back_populates="sources")

    @classmethod
    def load_source(cls, engine: Engine, source_id: int) -> "Sources":
        with Session(engine) as session:
            source = session.exec(select(cls).where(cls.id == source_id)).one()
        GrapherSourceDescription.validate(source.description)
        source.description = GrapherSourceDescription(**source.description)  # type: ignore
        return source

    @classmethod
    def load_sources(
        cls,
        engine: Engine,
        source_ids: list[int] = [],
        dataset_id: Optional[int] = None,
        variable_ids: list[int] = [],
    ) -> list["Sources"]:
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
            engine,
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
    __table_args__ = (
        ForeignKeyConstraint(["chartId"], ["charts.id"], name="suggested_chart_revisions_ibfk_1"),
        ForeignKeyConstraint(["createdBy"], ["users.id"], name="suggested_chart_revisions_ibfk_2"),
        ForeignKeyConstraint(["updatedBy"], ["users.id"], name="suggested_chart_revisions_ibfk_3"),
        Index("chartId", "chartId", "originalVersion", "suggestedVersion", "isPendingOrFlagged", unique=True),
        Index("createdBy", "createdBy"),
        Index("updatedBy", "updatedBy"),
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

    charts: Optional["Charts"] = Relationship(back_populates="suggested_chart_revisions")
    users: Optional["Users"] = Relationship(back_populates="suggested_chart_revisions")
    users_: Optional["Users"] = Relationship(back_populates="suggested_chart_revisions_")


class Variables(SQLModel, table=True):
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
        'grapherConfig': None
    }
    """

    __table_args__ = (
        ForeignKeyConstraint(["datasetId"], ["datasets.id"], name="variables_datasetId_50a98bfd_fk_datasets_id"),
        ForeignKeyConstraint(["sourceId"], ["sources.id"], name="variables_sourceId_31fce80a_fk_sources_id"),
        Index("unique_short_name_per_dataset", "shortName", "datasetId", unique=True),
        Index("variables_code_fk_dst_id_7bde8c2a_uniq", "code", "datasetId", unique=True),
        Index("variables_datasetId_50a98bfd_fk_datasets_id", "datasetId"),
        Index("variables_name_fk_dst_id_f7453c33_uniq", "name", "datasetId", unique=True),
        Index("variables_sourceId_31fce80a_fk_sources_id", "sourceId"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    unit: str = Field(sa_column=Column("unit", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    createdAt: datetime = Field(sa_column=Column("createdAt", DateTime, nullable=False))
    updatedAt: datetime = Field(sa_column=Column("updatedAt", DateTime, nullable=False))
    coverage: str = Field(sa_column=Column("coverage", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    timespan: str = Field(sa_column=Column("timespan", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    datasetId: int = Field(sa_column=Column("datasetId", Integer, nullable=False))
    sourceId: int = Field(sa_column=Column("sourceId", Integer, nullable=False))
    display: Dict[str, Any] = Field(sa_column=Column("display", JSON, nullable=False))
    columnOrder: int = Field(
        default=0, sa_column=Column("columnOrder", Integer, nullable=False, server_default=text("'0'"))
    )
    name: Optional[str] = Field(default=None, sa_column=Column("name", String(750, "utf8mb4_0900_as_cs")))
    description: Optional[str] = Field(default=None, sa_column=Column("description", LONGTEXT))
    code: Optional[str] = Field(default=None, sa_column=Column("code", String(255, "utf8mb4_0900_as_cs")))
    shortUnit: Optional[str] = Field(default=None, sa_column=Column("shortUnit", String(255, "utf8mb4_0900_as_cs")))
    originalMetadata: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("originalMetadata", JSON))
    grapherConfig: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("grapherConfig", JSON))
    shortName: Optional[str] = Field(default=None, sa_column=Column("shortName", String(255, "utf8mb4_0900_as_cs")))
    catalogPath: Optional[str] = Field(default=None, sa_column=Column("catalogPath", String(255, "utf8mb4_0900_as_cs")))
    dimensions: Optional[Dict[Any, Any]] = Field(default=None, sa_column=Column("dimensions", JSON))

    datasets: Optional["Datasets"] = Relationship(back_populates="variables")
    sources: Optional["Sources"] = Relationship(back_populates="variables")
    chart_dimensions: List["ChartDimensions"] = Relationship(back_populates="variables")
    data_values: List["DataValues"] = Relationship(back_populates="variables")

    @classmethod
    def load_variable(cls, engine: Engine, variable_id: int) -> "Variables":
        with Session(engine) as session:
            return session.exec(select(cls).where(cls.id == variable_id)).one()


class ChartDimensions(SQLModel, table=True):
    __table_args__ = (
        ForeignKeyConstraint(["chartId"], ["charts.id"], name="chart_dimensions_chartId_78d6a092_fk_charts_id"),
        ForeignKeyConstraint(
            ["variableId"], ["variables.id"], name="chart_dimensions_variableId_9ba778e6_fk_variables_id"
        ),
        Index("chart_dimensions_chartId_78d6a092_fk_charts_id", "chartId"),
        Index("chart_dimensions_variableId_9ba778e6_fk_variables_id", "variableId"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column("id", Integer, primary_key=True))
    order: int = Field(sa_column=Column("order", Integer, nullable=False))
    property: str = Field(sa_column=Column("property", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    chartId: int = Field(sa_column=Column("chartId", Integer, nullable=False))
    variableId: int = Field(sa_column=Column("variableId", Integer, nullable=False))

    charts: Optional["Charts"] = Relationship(back_populates="chart_dimensions")
    variables: Optional["Variables"] = Relationship(back_populates="chart_dimensions")


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
)


class DataValues(SQLModel, table=True):
    __tablename__: str = "data_values"  # type: ignore
    __table_args__ = (
        ForeignKeyConstraint(["entityId"], ["entities.id"], name="data_values_entityId_entities_id"),
        ForeignKeyConstraint(["variableId"], ["variables.id"], name="data_values_variableId_variables_id"),
        Index("data_values_fk_ent_id_fk_var_id_year_e0eee895_uniq", "entityId", "variableId", "year", unique=True),
        Index("data_values_variableId_variables_id", "variableId"),
        Index("data_values_year", "year"),
    )

    value: str = Field(sa_column=Column("value", String(255, "utf8mb4_0900_as_cs"), nullable=False))
    year: int = Field(sa_column=Column("year", Integer, primary_key=True, nullable=False))
    entityId: int = Field(sa_column=Column("entityId", Integer, primary_key=True, nullable=False))
    variableId: int = Field(sa_column=Column("variableId", Integer, primary_key=True, nullable=False))

    entities: Optional["Entities"] = Relationship(back_populates="data_values")
    variables: Optional["Variables"] = Relationship(back_populates="data_values")
