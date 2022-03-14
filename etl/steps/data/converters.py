#
#  converters.py
#

from owid.walden import Dataset as WaldenDataset
from owid.catalog import DatasetMeta, Source, License, VariableMeta
from etl.grapher_model import (
    GrapherDatasetModel,
    GrapherVariableModel,
    GrapherSourceModel,
)


def convert_walden_metadata(wd: WaldenDataset) -> DatasetMeta:
    """
    Copy metadata for a dataset directly from what we have in Walden.
    """
    return DatasetMeta(
        short_name=wd.short_name,
        namespace=wd.namespace,
        title=wd.name,
        description=wd.description,
        sources=[
            Source(
                name=wd.source_name,
                # description=wd.source_description,  # XXX no such walden field
                url=wd.url,
                source_data_url=wd.source_data_url,
                owid_data_url=wd.owid_data_url,
                date_accessed=wd.date_accessed,
                publication_date=str(wd.publication_date)
                if wd.publication_date
                else None,
                publication_year=wd.publication_year,
            )
        ],
        licenses=[License(name=wd.license_name, url=wd.license_url)],
    )


def convert_grapher_source(s: GrapherSourceModel) -> Source:
    return Source(
        name=s.name,
        description=s.description.additionalInfo,
        url=s.description.link,
        date_accessed=s.description.retrievedDate,
        published_by=s.description.dataPublishedBy,
        publisher_source=s.description.dataPublisherSource,
    )


def convert_grapher_dataset(
    g: GrapherDatasetModel, sources: list[GrapherSourceModel], short_name: str
) -> DatasetMeta:
    """
    Convert grapher dataset row into DatasetMeta.

    Example:
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
    return DatasetMeta(
        short_name=short_name,
        title=g.name,
        namespace=g.namespace,
        description=g.description,
        sources=[convert_grapher_source(s) for s in sources],
        additional_info={
            "grapher_meta": g.dict(),
        },
    )


def convert_grapher_variable(
    g: GrapherVariableModel, s: GrapherSourceModel
) -> VariableMeta:
    """Convert grapher variable row into VariableMeta.

    Example:
    {
        'id': 157342,
        'name': 'Agricultural machinery, tractors',
        'unit': '',
        'description': 'Agricultural machinery refers to the number of wheel and crawler...',
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
    return VariableMeta(
        title=g.name,
        description=g.description,
        short_unit=g.shortUnit,
        unit=g.unit,
        display=g.display,
        additional_info={
            "grapher_meta": g.dict(),
        },
        sources=[convert_grapher_source(s)],
        # TODO: where to put `code`?
        # TODO: can we get unit from `display` or not?
        # licenses: List[Source] = field(default_factory=list)
    )
