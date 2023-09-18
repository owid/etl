#
#  converters.py
#

from owid.catalog import DatasetMeta, License, Source, VariableMeta
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
    if snap.origin:
        ds_meta = DatasetMeta(
            short_name=snap.short_name,
            namespace=snap.namespace,
            version=snap.version,
            # dataset title and description are filled from origin
            title=snap.origin.title,
            description=snap.origin.description,
            licenses=[snap.license] if snap.license else [],
        )
    elif snap.source:
        ds_meta = DatasetMeta(
            short_name=snap.short_name,
            namespace=snap.namespace,
            title=snap.name,
            version=snap.version,
            description=snap.description,
            licenses=[snap.license] if snap.license else [],
        )
    else:
        raise ValueError("Snapshot must have either origin or source")

    # we allow both origin and source for backward compatiblity
    if snap.origin:
        ds_meta.origins = [snap.origin]
    if snap.source:
        ds_meta.sources = [snap.source]

    return ds_meta


def convert_grapher_source(s: gm.Source) -> Source:
    description = s.description.get("additionalInfo") or ""

    # append publisher source to description
    if s.description.get("dataPublisherSource"):
        description += f"\nPublisher source: {s.description.get('dataPublisherSource')}"

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
