#
#  converters.py
#

import datetime as dt

from owid.catalog import DatasetMeta, License, Origin, Source, VariableMeta
from owid.walden import Dataset as WaldenDataset

from etl import grapher_model as gm
from etl.snapshot import SnapshotMeta


def convert_walden_metadata(wd: WaldenDataset) -> DatasetMeta:
    """
    Copy metadata for a dataset directly from what we have in Walden.
    """
    return DatasetMeta(
        short_name=wd.short_name,
        namespace=wd.namespace,
        title=wd.name,
        version=wd.version,
        description=wd.description,
        sources=[
            Source(
                name=wd.source_name,
                # description=wd.source_description,  # XXX no such walden field
                url=wd.url,
                source_data_url=wd.source_data_url,
                owid_data_url=wd.owid_data_url,
                date_accessed=wd.date_accessed,
                publication_date=str(wd.publication_date) if wd.publication_date else None,
                publication_year=wd.publication_year,
            )
        ],
        licenses=[License(name=wd.license_name, url=wd.license_url)] if wd.license_name or wd.license_url else [],
    )


def convert_snapshot_metadata(snap: SnapshotMeta) -> DatasetMeta:
    """
    Copy metadata for a dataset directly from what we have in Snapshot.
    """
    assert snap.origin
    assert snap.source
    return DatasetMeta(
        short_name=snap.short_name,
        namespace=snap.namespace,
        version=snap.version,
        # dataset title and description are filled from origin
        title=snap.origin.dataset_title_owid,
        description=snap.origin.dataset_description_owid,
        sources=[snap.source],
        origins=[snap.origin],
        licenses=[snap.license] if snap.license else [],
    )


def convert_grapher_source(s: gm.Source) -> Source:
    # append publisher source to description
    description = f"{s.description.get('additionalInfo')}\nPublisher source: {s.description.get('dataPublisherSource')}"

    return Source(
        name=s.name,
        description=description,
        url=s.description.get("link"),
        date_accessed=s.description.get("retrievedDate"),
        published_by=s.description.get("dataPublishedBy"),
    )


def convert_grapher_dataset(g: gm.Dataset, sources: list[gm.Source], short_name: str) -> DatasetMeta:
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
        is_public=not g.isPrivate,
        sources=[convert_grapher_source(s) for s in sources],
        additional_info={
            "grapher_meta": g.dict(),
        },
    )


def convert_grapher_variable(g: gm.Variable, s: gm.Source) -> VariableMeta:
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


def convert_origin_to_source(o: Origin) -> Source:
    # `dataset_title` isn't used, but it is assigned to DatasetMeta.title
    # when propagating Snapshot to meadow dataset. Same for `dataset_description`,
    # though that one is used here as `Source.description`.
    return Source(
        name=o.producer,
        description=o.dataset_description_owid,
        url=o.dataset_url_main,
        source_data_url=o.dataset_url_download,
        date_accessed=str(o.date_accessed) if o.date_accessed else None,
        publication_date=o.date_published,
        published_by=o.citation_producer,
        # excluded fields
        # owid_data_url
        # publication_year
    )


def convert_source_to_origin(s: Source) -> Origin:
    """Inverse of `convert_origin_to_source`."""
    return Origin(
        producer=s.name,
        dataset_description_owid=s.description,
        dataset_url_main=s.url,
        dataset_url_download=s.source_data_url,
        date_accessed=s.date_accessed if s.date_accessed else None,  # type: ignore
        date_published=s.publication_date if s.publication_date else None,  # type: ignore
        citation_producer=s.published_by,
    )
