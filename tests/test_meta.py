#
#  test_meta.py
#

from typing import Optional, Dict, Any
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from etl import meta


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


def test_empty_dataset_metadata():
    d1 = meta.DatasetMeta()
    assert d1.to_dict() == {}
