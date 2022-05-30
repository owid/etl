"""Tools to ingest to Walden and Catalog."""

from typing import Union

from .catalog import WaldenDataset
from .ui import log


def add_to_catalog(
    metadata: Union[dict, WaldenDataset],
    filename: str,
    upload: bool = False,
    public: bool = True,
) -> None:
    """Add metadata to catalog.

    Additionally, it computes the md5 hash of the file, which is added to the metadata file.

    TODO: Add checks of fields.

    Args:
        metadata (dict): Dictionary with metadata.
        local_path (str): Path to local data file. Used to compute the md5 hash.
    """
    # checksum happens in here, copy to cache happens here
    dataset = WaldenDataset.copy_and_create(filename, metadata)

    if upload:
        # add it to our DigitalOcean Space and set `owid_cache_url`
        dataset.upload(public=public)

    # save the JSON to the local index
    dataset.save()

    log("ADDED TO CATALOG", f"{dataset.relative_base}.json")
