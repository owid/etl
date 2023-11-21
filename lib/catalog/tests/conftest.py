from collections import defaultdict

import pandas as pd
import pytest

from owid.catalog.meta import (
    DatasetMeta,
    License,
    Origin,
    Source,
    TableMeta,
    VariableMeta,
    VariablePresentationMeta,
)
from owid.catalog.tables import Table
from owid.catalog.variables import Variable


@pytest.fixture
def sources():
    sources = {
        1: Source(name="Name of Source 1", description="Description of Source 1"),
        2: Source(name="Name of Source 2", description="Description of Source 2"),
        3: Source(name="Name of Source 3", description="Description of Source 3"),
        4: Source(name="Name of Source 4", description="Description of Source 4"),
    }
    return sources


@pytest.fixture
def origins():
    origins = {
        1: Origin(producer="Producer 1", title="Name of Origin 1", description="Description of Origin 1"),
        2: Origin(producer="Producer 2", title="Name of Origin 2", description="Description of Origin 2"),
        3: Origin(producer="Producer 3", title="Name of Origin 3", description="Description of Origin 3"),
        4: Origin(producer="Producer 4", title="Name of Origin 4", description="Description of Origin 4"),
    }
    return origins


@pytest.fixture
def licenses():
    licenses = {
        1: License(name="Name of License 1", url="URL of License 1"),
        2: License(name="Name of License 2", url="URL of License 2"),
        3: License(name="Name of License 3", url="URL of License 3"),
        4: License(name="Name of License 4", url="URL of License 4"),
    }
    return licenses


@pytest.fixture
def variable_1(sources, origins, licenses):
    v1 = Variable(pd.Series([1, 2, 3]), name="Variable 1")
    v1.metadata.title = "Title of Variable 1"
    v1.metadata.description = "Description of Variable 1"
    v1.metadata.unit = "Unit of Variable 1"
    v1.metadata.unit = "Short unit of Variable 1"
    v1.metadata.sources = [sources[2], sources[1]]
    v1.metadata.origins = [origins[2], origins[1]]
    v1.metadata.licenses = [licenses[1]]
    v1.metadata.processing_level = "minor"
    v1.metadata.presentation = VariablePresentationMeta(title_public="Title of Variable 1")
    v1.metadata.display = {"isProjection": True, "numDecimalPlaces": 1}
    return v1


@pytest.fixture
def variable_2(sources, origins, licenses):
    v2 = Variable(pd.Series([4, 5, 6]), name="Variable 2")
    v2.metadata.title = "Title of Variable 2"
    v2.metadata.description = "Description of Variable 2"
    v2.metadata.unit = "Unit of Variable 2"
    v2.metadata.unit = "Short unit of Variable 2"
    v2.metadata.sources = [sources[2], sources[3]]
    v2.metadata.origins = [origins[2], origins[3]]
    v2.metadata.licenses = [licenses[2], licenses[3]]
    v2.metadata.processing_level = "major"
    v2.metadata.presentation = VariablePresentationMeta(title_public="Title of Variable 2")
    v2.metadata.display = {"isProjection": True, "numDecimalPlaces": 1}
    return v2


@pytest.fixture
def table_1(sources, licenses, origins):
    tb1 = Table({"country": ["Spain", "Spain", "France"], "year": [2020, 2021, 2021], "a": [1, 2, 3], "b": [4, 5, 6]})
    tb1.metadata = TableMeta(
        title="Title of Table 1",
        description="Description of Table 1",
        dataset=DatasetMeta(
            sources=[sources[1], sources[2], sources[3]],
            licenses=[licenses[1], licenses[2], licenses[3]],
        ),
    )
    tb1._fields = defaultdict(
        VariableMeta,
        {
            "country": VariableMeta(title="Country Title", description="Description of Table 1 Variable country"),
            "a": VariableMeta(
                title="Title of Table 1 Variable a",
                description="Description of Table 1 Variable a",
                description_short="Short description of Table 1 Variable a",
                description_key=[
                    "Key description point 1 of Variable 1",
                    "Common key description point",
                ],
                description_from_producer="Common description from producer",
                sources=[sources[2], sources[1]],
                origins=[origins[2], origins[1]],
                licenses=[licenses[1]],
                processing_level="minor",
                presentation=VariablePresentationMeta(title_public="Title of Variable a"),
                display={"isProjection": True, "numDecimalPlaces": 1},
            ),
            "b": VariableMeta(
                title="Title of Table 1 Variable b",
                description="Description of Table 1 Variable b",
                description_short="Short description of Table 1 Variable b",
                description_key=[
                    "Key description point 1 of Variable 2",
                    "Common key description point",
                    "Key description point 2 of Variable 2",
                ],
                description_from_producer="Common description from producer",
                sources=[sources[2], sources[3]],
                origins=[origins[2], origins[3]],
                licenses=[licenses[2], licenses[3]],
                processing_level="major",
                presentation=VariablePresentationMeta(title_public="Title of Variable 2"),
                display={"isProjection": True, "numDecimalPlaces": 1},
            ),
        },
    )
    return tb1


@pytest.fixture
def table_2(sources, licenses):
    tb2 = Table(
        {"country": ["Spain", "France", "France"], "year": [2020, 2021, 2022], "a": [10, 20, 30], "c": [40, 50, 60]}
    )
    tb2.metadata = TableMeta(title="Title of Table 2", description="Description of Table 2")
    tb2._fields = defaultdict(
        VariableMeta,
        {
            "country": VariableMeta(
                title="Country Title",
            ),
            "a": VariableMeta(
                title="Title of Table 2 Variable a",
                description="Description of Table 2 Variable a",
                sources=[sources[2]],
                licenses=[licenses[2]],
                processing_level="major",
                presentation=VariablePresentationMeta(title_public="Title of Variable a"),
                display={"isProjection": True, "numDecimalPlaces": 1, "tolerance": 2},
            ),
            "c": VariableMeta(
                title="Title of Table 2 Variable c",
                description="Description of Table 2 Variable c",
                sources=[sources[2], sources[4]],
                licenses=[licenses[4], licenses[2]],
            ),
        },
    )
    return tb2
