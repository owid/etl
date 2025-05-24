from pathlib import Path

from etl.collection.core.collection_set import CollectionSet
from etl.collection.core.combine import combine_config_dimensions
from etl.collection.model.core import Collection


def _simple_collection_dict(name: str) -> dict:
    return {
        "dimensions": [{"slug": "dim", "name": "Dim", "choices": [{"slug": "a", "name": "A"}]}],
        "views": [
            {
                "dimensions": {"dim": "a"},
                "indicators": {"y": [{"catalogPath": "table#ind"}]},
            }
        ],
        "catalog_path": f"dataset#{name}",
        "title": {"title": "Title"},
        "default_selection": [],
    }


def test_combine_config_dimensions_overwrite_and_order():
    auto = [
        {"slug": "a", "name": "Auto A", "choices": [{"slug": "a1", "name": "A1"}]},
        {"slug": "b", "name": "Auto B", "choices": [{"slug": "b1", "name": "B1"}]},
    ]
    yaml_conf = [
        {"slug": "b", "name": "Yaml B", "choices": [{"slug": "b1", "name": "B1_yaml"}]},
        {"slug": "c", "name": "Yaml C", "choices": [{"slug": "c1", "name": "C1"}]},
    ]

    combined = combine_config_dimensions(auto, yaml_conf)
    slugs = [d["slug"] for d in combined]
    assert slugs == ["b", "c", "a"]
    assert combined[0]["name"] == "Yaml B"
    assert combined[0]["choices"][0]["name"] == "B1_yaml"
    assert combined[1]["slug"] == "c"


def test_combine_config_dimensions_choices_top():
    auto = [
        {
            "slug": "dim",
            "name": "Dim",
            "choices": [
                {"slug": "a", "name": "A"},
                {"slug": "b", "name": "B"},
            ],
        }
    ]
    yaml_conf = [
        {
            "slug": "dim",
            "name": "DimY",
            "choices": [
                {"slug": "a", "name": "A_yaml"},
                {"slug": "c", "name": "C"},
            ],
        }
    ]

    res_top = combine_config_dimensions(auto, yaml_conf, choices_top=True)
    assert [c["slug"] for c in res_top[0]["choices"]] == ["a", "b", "c"]

    res_bottom = combine_config_dimensions(auto, yaml_conf, choices_top=False)
    assert [c["slug"] for c in res_bottom[0]["choices"]] == ["a", "c", "b"]


def test_collection_set(tmp_path: Path):
    path = tmp_path
    coll1 = Collection.from_dict(_simple_collection_dict("coll1"))
    coll2 = Collection.from_dict(_simple_collection_dict("coll2"))
    coll1.save_file(path / "coll1.config.json")
    coll2.save_file(path / "coll2.config.json")

    cset = CollectionSet(path)
    assert cset.names == ["coll1", "coll2"]

    loaded = cset.read("coll1")
    assert isinstance(loaded, Collection)
    assert loaded.short_name == "coll1"
