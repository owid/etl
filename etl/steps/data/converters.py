#
#  converters.py
#

from typing import Any, Dict

from owid.catalog import DatasetMeta, Source, VariableMeta

from etl.snapshot import SnapshotMeta


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


def convert_grapher_source(s: Dict[str, Any]) -> Source:
    description = s["description"].get("additionalInfo") or ""

    # append publisher source to description
    if s["description"].get("dataPublisherSource"):
        description += f"\nPublisher source: {s['description'].get('dataPublisherSource')}"

    return Source(
        name=s["name"],
        description=description,
        url=s["description"].get("link"),
        date_accessed=s["description"].get("retrievedDate"),
        published_by=s["description"].get("dataPublishedBy"),
    )


def convert_grapher_dataset(ds: Dict[str, Any], sources: list[Dict[str, Any]], short_name: str) -> DatasetMeta:
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
        title=ds["name"],
        namespace=ds["namespace"],
        description=ds["description"],
        is_public=not ds["isPrivate"],
        sources=[convert_grapher_source(s) for s in sources],
        additional_info={
            "grapher_meta": ds,
        },
    )


def convert_grapher_variable(v: Dict[str, Any], s: Dict[str, Any]) -> VariableMeta:
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
        title=v["name"],
        description=v["description"],
        short_unit=v["shortUnit"],
        unit=v["unit"],
        display=v["display"],
        additional_info={
            "grapher_meta": v,
        },
        sources=[convert_grapher_source(s)],
        # TODO: where to put `code`?
        # TODO: can we get unit from `display` or not?
        # licenses: List[Source] = field(default_factory=list)
    )
