import importlib.util
from pathlib import Path
import types
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]


# Provide a minimal stub for the ``deprecated`` package required by the module
def _deprecated(reason):
    def decorator(func):
        return func

    return decorator


sys.modules.setdefault("deprecated", types.SimpleNamespace(deprecated=_deprecated))

# Stub modules required by ``collection.utils`` so that the module can be loaded
# without installing heavy dependencies.
pkg_collection = sys.modules.setdefault("etl.collection", types.ModuleType("etl.collection"))

spec_exc = importlib.util.spec_from_file_location("etl.collection.exceptions", ROOT / "etl/collection/exceptions.py")
exc = importlib.util.module_from_spec(spec_exc)
sys.modules["etl.collection.exceptions"] = exc
spec_exc.loader.exec_module(exc)
pkg_collection.exceptions = exc

sys.modules.setdefault("owid.catalog", types.SimpleNamespace(Dataset=object))
sys.modules.setdefault("sqlalchemy.orm", types.SimpleNamespace(Session=object))
grapher_pkg = types.ModuleType("etl.grapher")
grapher_model = types.SimpleNamespace()
grapher_pkg.model = grapher_model
sys.modules.setdefault("etl.grapher", grapher_pkg)
sys.modules.setdefault("etl.grapher.model", grapher_model)
sys.modules.setdefault("etl.config", types.SimpleNamespace(OWID_ENV=None, OWIDEnv=object))
sys.modules.setdefault("etl.db", types.SimpleNamespace(read_sql=lambda *a, **k: None))
sys.modules.setdefault("etl.files", types.SimpleNamespace(yaml_dump=lambda d: ""))
sys.modules.setdefault("etl.paths", types.SimpleNamespace(DATA_DIR=Path(".")))

# Avoid importing etl.collection package (which has heavy dependencies) by
# loading the modules directly from their file paths.
spec_utils = importlib.util.spec_from_file_location("collection_utils", ROOT / "etl/collection/utils.py")
utils = importlib.util.module_from_spec(spec_utils)
assert spec_utils.loader
spec_utils.loader.exec_module(utils)

expand_combinations = utils.expand_combinations
get_complete_dimensions_filter = utils.get_complete_dimensions_filter
move_field_to_top = utils.move_field_to_top
extract_definitions = utils.extract_definitions
fill_placeholders = utils.fill_placeholders
group_views_legacy = utils.group_views_legacy
unique_records = utils.unique_records
records_to_dictionary = utils.records_to_dictionary
ParamKeyError = exc.ParamKeyError


# ----------------------------------------------------------------------------
# expand_combinations and get_complete_dimensions_filter
# ----------------------------------------------------------------------------


def test_expand_combinations():
    dims = {"a": ["x", "y"], "b": ["1"]}
    combos = expand_combinations(dims)
    assert len(combos) == 2
    assert {tuple(sorted(c.items())) for c in combos} == {
        tuple(sorted({"a": "x", "b": "1"}.items())),
        tuple(sorted({"a": "y", "b": "1"}.items())),
    }


def test_get_complete_dimensions_filter():
    dims_avail = {"metric": {"cases", "deaths"}, "age": {"0-9", "10-19"}}
    dims_filter = {"metric": "cases"}
    result = get_complete_dimensions_filter(dims_avail, dims_filter)
    assert {tuple(sorted(r.items())) for r in result} == {
        tuple(sorted({"metric": "cases", "age": "0-9"}.items())),
        tuple(sorted({"metric": "cases", "age": "10-19"}.items())),
    }
    with pytest.raises(AssertionError):
        get_complete_dimensions_filter(dims_avail, {"metric": "unknown"})


# ----------------------------------------------------------------------------
# move_field_to_top and extract_definitions
# ----------------------------------------------------------------------------


def test_move_field_to_top():
    data = {"b": 2, "a": 1, "c": 3}
    moved = move_field_to_top(data, "a")
    assert list(moved.keys())[:1] == ["a"]
    # Ensure other fields preserved
    assert list(moved.keys()) == ["a", "b", "c"]
    # Field not present: object should be returned unchanged
    same = move_field_to_top(data, "missing")
    assert same is data


def test_extract_definitions_simple():
    config = {"views": [{"indicators": {"y": [{"display": {"additionalInfo": "Line1\\nLine2"}}]}}]}
    out = extract_definitions(config)
    # definitions moved to top
    assert list(out.keys())[0] == "definitions"
    defs = out["definitions"]["additionalInfo"]
    assert isinstance(defs, dict) and len(defs) == 1
    anchor = next(iter(defs))
    assert defs[anchor] == "Line1\nLine2"
    # indicator references the anchor
    assert out["views"][0]["indicators"]["y"][0]["display"]["additionalInfo"] == f"*{anchor}"


# ----------------------------------------------------------------------------
# fill_placeholders
# ----------------------------------------------------------------------------


def test_fill_placeholders():
    data = {
        "a": "{x} is {y}",
        "b": ["{y}", 1],
        "c": {"d": "{x}"},
        "e": ("{x}", "{y}"),
    }
    params = {"x": "foo", "y": "bar"}
    out = fill_placeholders(data, params)
    assert out == {
        "a": "foo is bar",
        "b": ["bar", 1],
        "c": {"d": "foo"},
        "e": ("foo", "bar"),
    }

    with pytest.raises(ParamKeyError):
        fill_placeholders("{x} {z}", {"x": "foo"})


# ----------------------------------------------------------------------------
# group_views_legacy and helpers
# ----------------------------------------------------------------------------


def test_group_views_legacy():
    views = [
        {"dimensions": {"country": "a"}, "indicators": {"y": "ind1"}},
        {"dimensions": {"country": "a"}, "indicators": {"y": "ind2"}},
        {"dimensions": {"country": "b"}, "indicators": {"y": "ind3"}},
    ]
    grouped = group_views_legacy(views, by=["country"])
    assert grouped == [
        {
            "dimensions": {"country": "a"},
            "indicators": {"y": ["ind1", "ind2"]},
        },
        {
            "dimensions": {"country": "b"},
            "indicators": {"y": ["ind3"]},
        },
    ]

    err_view = {"dimensions": {"country": "c"}, "indicators": {"y": ["a", "b"]}}
    with pytest.raises(NotImplementedError):
        group_views_legacy([err_view], by=["country"])


# ----------------------------------------------------------------------------
# records_to_dictionary and unique_records
# ----------------------------------------------------------------------------


def test_records_to_dictionary_and_unique_records():
    recs = [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
        {"id": 1, "v": "a"},
    ]
    dic = records_to_dictionary(recs, "id")
    assert dic == {1: {"v": "a"}, 2: {"v": "b"}}

    uniq = unique_records(recs)
    assert uniq == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
    ]
