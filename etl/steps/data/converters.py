#
#  converters.py
#

from owid.walden import Dataset as WaldenDataset
from owid.catalog import DatasetMeta, Source, License


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
                publication_date=str(wd.publication_date),
                publication_year=wd.publication_year,
            )
        ],
        licenses=[License(name=wd.license_name, url=wd.license_url)],
    )
