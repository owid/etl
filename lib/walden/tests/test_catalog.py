#
#  test_catalog.py
#
#  Unit tests for basic catalog and dataset functionality.
#

from pathlib import Path
import datetime as dt

from jsonschema import Draft7Validator, validate, ValidationError
import pytest

from owid.walden.catalog import INDEX_DIR, Dataset, Catalog, load_schema, iter_docs


def test_schema():
    "Make sure the schema itself is valid."
    schema = load_schema()
    Draft7Validator.check_schema(schema)


def test_catalog_entries():
    "Make sure every catalog entry matches the schema."
    schema = load_schema()
    for filename, doc in iter_docs():
        try:
            validate(doc, schema)
        except ValidationError as e:
            print("Error in file:", Path(filename).relative_to(INDEX_DIR))
            raise


def test_catalog_loads():
    catalog = Catalog()

    # the catalog is not empty
    assert len(catalog) > 0

    # everything in it is a dataset
    for dataset in catalog:
        assert isinstance(dataset, Dataset)


def test_catalog_find():
    catalog = Catalog()
    matches = catalog.find(namespace="irena")
    assert len(matches) >= 2
    assert all(isinstance(d, Dataset) for d in matches)


def test_catalog_find_one_success():
    catalog = Catalog()
    dataset = catalog.find_one("who", "2021-07-01", "gho")
    assert isinstance(dataset, Dataset)


def test_catalog_find_one_too_many():
    catalog = Catalog()
    with pytest.raises(Exception):
        catalog.find_one()

    with pytest.raises(Exception):
        catalog.find_one("who")


def test_catalog_find_one_too_few():
    catalog = Catalog()
    with pytest.raises(Exception):
        catalog.find_one("highly_unlikely_namespace")


def test_catalog_find_latest():
    catalog = Catalog()
    dataset = catalog.find_latest("who", "gho")
    assert isinstance(dataset, Dataset)


def test_metadata_pruning():
    """Make sure we're not losing metadata keys with False values."""
    catalog = Catalog()
    dataset = catalog.find_one("who", "2021-07-01", "gho")
    dataset.is_public = False
    assert dataset.metadata["is_public"] == False


def test_default_dataset_version():
    """Use publication_date as version if not given in metadata."""
    kwargs = dict(
        name="test",
        namespace="test",
        short_name="test",
        description="test",
        source_name="test",
        url="test",
        file_extension="gzip",
    )
    ds = Dataset(publication_date=dt.date(2022, 1, 1), **kwargs)
    assert ds.version == "2022-01-01"

    ds = Dataset(version="2023-01-01", publication_date=dt.date(2022, 1, 1), **kwargs)
    assert ds.version == "2023-01-01"
