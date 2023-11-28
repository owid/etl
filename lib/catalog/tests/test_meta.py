#
#  test_meta.py
#

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest
import yaml
from dataclasses_json import dataclass_json

from owid.catalog import meta


def test_dict_mixin():
    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Dog:
        name: Optional[str] = None
        age: Optional[int] = None

        def to_dict(self) -> Dict[str, Any]:
            ...

    assert Dog(name="fred").to_dict() == {"name": "fred"}
    assert Dog(age=10).to_dict() == {"age": 10}


def test_dict_mixin_nested():
    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Cat:
        name: Optional[str] = None
        age: Optional[int] = None

    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Dog:
        name: Optional[str] = None
        age: Optional[int] = None
        cat: Optional[Cat] = None

        def to_dict(self) -> Dict[str, Any]:
            ...

    assert Dog(name="fred", cat=Cat(name="cred")).to_dict() == {"name": "fred", "cat": {"name": "cred"}}


def test_empty_dataset_metadata():
    d1 = meta.DatasetMeta()
    assert d1.to_dict() == {"is_public": True}


def test_dataset_version():
    s1 = meta.Source(name="s1", publication_date="2022-01-01")
    s2 = meta.Source(name="s2", publication_date="2022-01-02")

    assert meta.DatasetMeta(version="1").version == "1"
    assert meta.DatasetMeta(sources=[s1]).version == "2022-01-01"
    assert meta.DatasetMeta(sources=[s1, s2]).version is None
    assert meta.DatasetMeta(version="1", sources=[s1]).version == "1"


def test_to_json():
    meta.Source(name="s1", publication_date="2022-01-01").to_json()  # type: ignore


def test_update_from_yaml(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    metapath = d / "meta.yml"

    s1 = meta.Source(name="s1")
    s2 = meta.Source(name="s2")

    # save dictionary to yaml using yaml library
    with open(metapath, "w") as f:
        yaml.dump({"dataset": {"sources": [s2.to_dict()]}}, f)

    d1 = meta.DatasetMeta(sources=[s1])
    with pytest.raises(ValueError):
        d1.update_from_yaml(metapath, if_source_exists="fail")

    d1 = meta.DatasetMeta(sources=[s1])
    d1.update_from_yaml(metapath, if_source_exists="replace")
    assert len(d1.sources) == 1

    d1 = meta.DatasetMeta(sources=[s1])
    d1.update_from_yaml(metapath, if_source_exists="append")
    assert len(d1.sources) == 2


def test_load_license_from_dict():
    d = {
        "url": "https://www.unicef.org/legal#terms-of-use",
    }
    license = meta.License.from_dict(d)
    assert license.url == d["url"]


def test_Origin_date_published():
    assert meta.Origin(producer="p", title="a", date_published="2020-01-01").date_published == "2020-01-01"  # type: ignore
    assert meta.Origin(producer="p", title="a", date_published="2020").date_published == "2020"  # type: ignore
    assert meta.Origin(producer="p", title="a", date_published="latest").date_published == "latest"  # type: ignore
    with pytest.raises(ValueError):
        assert meta.Origin(producer="p", title="a", date_published="nope")  # type: ignore
