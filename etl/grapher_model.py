import datetime as dt
import json
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel
from sqlalchemy.engine import Engine
from sqlmodel import JSON, Column, Field, Session, SQLModel, select
from sqlmodel.sql.expression import Select, SelectOfScalar
import structlog


log = structlog.get_logger()


# get rid of warning from https://github.com/tiangolo/sqlmodel/issues/189
SelectOfScalar.inherit_cache = True  # type: ignore
Select.inherit_cache = True  # type: ignore


class GrapherDatasetModel(SQLModel, table=True):
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

    id: int = Field(primary_key=True)
    name: str
    description: str
    createdAt: dt.datetime
    updatedAt: dt.datetime
    namespace: str
    isPrivate: bool
    metadataEditedAt: dt.datetime
    createdByUserId: int
    metadataEditedByUserId: int
    dataEditedAt: dt.datetime
    dataEditedByUserId: int
    nonRedistributable: bool

    @classmethod
    def load_dataset(cls, engine: Engine, dataset_id: int) -> "GrapherDatasetModel":
        with Session(engine) as session:
            return session.exec(select(cls).where(cls.id == dataset_id)).one()

    @classmethod
    def load_variables_for_dataset(
        cls, engine: Engine, dataset_id: int
    ) -> list["GrapherVariableModel"]:
        with Session(engine) as session:
            vars = session.exec(
                select(GrapherVariableModel).where(
                    GrapherVariableModel.datasetId == dataset_id
                )
            ).all()
            assert vars
        return vars


class GrapherVariableModel(SQLModel, table=True):
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

    __tablename__: str = "variables"  # type: ignore

    id: int = Field(primary_key=True)
    name: str
    unit: str
    description: Optional[str]
    createdAt: dt.datetime
    updatedAt: dt.datetime
    code: Optional[str]
    coverage: str
    timespan: str
    datasetId: int
    sourceId: int = Field(foreign_key="source.id")
    shortUnit: Optional[str]
    display: Dict[str, Any] = Field(sa_column=Column(JSON))
    columnOrder: int
    originalMetadata: Optional[Dict[str, Any]] = Field(sa_column=Column(JSON))
    grapherConfig: Optional[Dict[str, Any]] = Field(sa_column=Column(JSON))

    @classmethod
    def load_variable(cls, engine: Engine, variable_id: int) -> "GrapherVariableModel":
        with Session(engine) as session:
            return session.exec(select(cls).where(cls.id == variable_id)).one()


class GrapherSourceDescription(BaseModel):
    link: Optional[str] = None
    retrievedDate: Optional[str] = None
    additionalInfo: Optional[str] = None
    dataPublishedBy: Optional[str] = None
    dataPublisherSource: Optional[str] = None


class GrapherSourceModel(SQLModel, table=True):
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

    id: int = Field(primary_key=True)
    name: str
    # NOTE: description is not converted into GrapherSourceDescription object automatically, I haven't
    # found an easy solution how to do it, but there's some momentum https://github.com/tiangolo/sqlmodel/issues/63
    description: GrapherSourceDescription = Field(sa_column=Column(JSON))
    createdAt: dt.datetime
    updatedAt: dt.datetime
    datasetId: int = Field(foreign_key="dataset.id")

    @classmethod
    def load_source(cls, engine: Engine, source_id: int) -> "GrapherSourceModel":
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
    ) -> list["GrapherSourceModel"]:
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


class GrapherConfig(BaseModel):
    dataset: GrapherDatasetModel
    variables: list[GrapherVariableModel]
    # NOTE: sources can belong to dataset or variable
    sources: list[GrapherSourceModel]
