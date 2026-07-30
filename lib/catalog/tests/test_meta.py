#
#  test_meta.py
#

from dataclasses import dataclass
from typing import Any

import pytest
import yaml
from dataclasses_json import dataclass_json

from owid.catalog.core import meta


def test_dict_mixin():
    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Dog:
        name: str | None = None
        age: int | None = None

        def to_dict(self) -> dict[str, Any]: ...  # ty: ignore

    assert Dog(name="fred").to_dict() == {"name": "fred"}
    assert Dog(age=10).to_dict() == {"age": 10}


def test_dict_mixin_nested():
    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Cat:
        name: str | None = None
        age: int | None = None

    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Dog:
        name: str | None = None
        age: int | None = None
        cat: Cat | None = None

        def to_dict(self) -> dict[str, Any]: ...  # ty: ignore

    assert Dog(name="fred", cat=Cat(name="cred")).to_dict() == {"name": "fred", "cat": {"name": "cred"}}


def test_empty_dataset_metadata():
    d1 = meta.DatasetMeta()
    assert d1.to_dict() == {"is_public": True, "non_redistributable": False, "jsonld": False}


def test_dataset_version():
    assert meta.DatasetMeta(version="1").version == "1"
    assert meta.DatasetMeta().version is None


def test_update_from_yaml(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    metapath = d / "meta.yml"

    with open(metapath, "w") as f:
        yaml.dump({"dataset": {"title": "From YAML", "description": "yaml desc"}}, f)

    d1 = meta.DatasetMeta(title="Original")
    d1.update_from_yaml(metapath)
    assert d1.title == "From YAML"
    assert d1.description == "yaml desc"


def test_owners_round_trip(tmp_path):
    # Empty owners must not appear in to_dict (pruned).
    assert "owners" not in meta.DatasetMeta().to_dict()

    # Populated owners must round-trip through to_dict / from_dict and YAML.
    owners = ["Mojmír Vinkler", "Pablo Rosado"]
    d1 = meta.DatasetMeta(owners=owners)
    assert d1.to_dict()["owners"] == owners
    assert meta.DatasetMeta.from_dict(d1.to_dict()).owners == owners

    metapath = tmp_path / "meta.yml"
    with open(metapath, "w") as f:
        yaml.dump({"dataset": {"owners": owners}}, f)

    d2 = meta.DatasetMeta()
    d2.update_from_yaml(metapath)
    # update_from_yaml goes through dynamic_yaml, which returns a list-proxy;
    # match by element rather than identity.
    assert list(d2.owners) == owners


def test_load_license_from_dict():
    d = {
        "url": "https://www.unicef.org/legal#terms-of-use",
    }
    license = meta.License.from_dict(d)
    assert license.url == d["url"]


def test_Origin_date_published():
    assert meta.Origin(producer="p", title="a", date_published="2020-01-01").date_published == "2020-01-01"  # ty: ignore
    assert meta.Origin(producer="p", title="a", date_published="2020").date_published == "2020"  # ty: ignore
    assert meta.Origin(producer="p", title="a", date_published="latest").date_published == "latest"  # ty: ignore
    with pytest.raises(ValueError):
        assert meta.Origin(producer="p", title="a", date_published="nope")  # ty: ignore


def test_hash():
    origin_a = meta.Origin("a", "b")
    origin_b = meta.Origin("a", "b")
    origin_c = meta.Origin("a", "c")
    assert origin_a == origin_b
    assert origin_a != origin_c
    assert {origin_a, origin_b, origin_c} == {origin_a, origin_c}

    # test hashing of nested dictionary
    var_a = meta.VariableMeta(display={"d": {"a": 1, "b": 2}})
    var_b = meta.VariableMeta(display={"d": {"a": 1, "b": 2}})
    var_c = meta.VariableMeta(display={"d": {"a": 1, "b": 2, "c": 3}})
    assert var_a == var_b
    assert var_a != var_c


def test_from_dict():
    @dataclass
    class X(meta.MetaBase):
        a: int

    @dataclass
    class Y(meta.MetaBase):
        x_list: list[X] | None = None

    # list of objects should be correctly loaded as that object
    y = Y.from_dict({"x_list": [{"a": 1}]})
    assert isinstance(y.x_list[0], X)


def test_render():
    jinja_title = """
    <% if dim_a == "x" %>Title X<% elif dim_a == "y" %>Title Y<% else %>Default Title<% endif %>
    """.strip()

    var_meta = meta.VariableMeta(title=jinja_title)  # ty: ignore
    rendered_meta = var_meta.render(dim_dict={"dim_a": "x"})
    assert isinstance(rendered_meta, meta.VariableMeta)
    assert rendered_meta.title == "Title X"


def test_description_key_to_string_flattens_multiline_items():
    # Line and paragraph breaks inside items are flattened to spaces, matching
    # how the old renderer displayed them (paragraphs unwrapped inside <li>).
    assert meta.description_key_to_string(["**Rationale:**\nMortality does not.", "Other."]) == (
        "- **Rationale:** Mortality does not.\n- Other."
    )
    assert meta.description_key_to_string(["Para one.\n\nPara two.", "Other."]) == ("- Para one. Para two.\n- Other.")
    # Markdown lists inside an item rendered as nested lists — keep them nested.
    assert meta.description_key_to_string(["Includes:\n- a\n- b", "Other."]) == ("- Includes:\n  - a\n  - b\n- Other.")
    # A single item passes through unchanged (it was rendered as full markdown).
    assert meta.description_key_to_string(["Para one.\n\nPara two."]) == "Para one.\n\nPara two."


def test_update_variable_metadata_converts_description_key_list():
    # A list of bullet points is converted to a markdown string after Jinja rendering.
    m = meta.VariableMeta(description_key=["Point 1", "Point 2"])
    assert meta.update_variable_metadata(m).description_key == "- Point 1\n- Point 2"

    m = meta.VariableMeta(description_key=["Only point"])
    assert meta.update_variable_metadata(m).description_key == "Only point"

    m = meta.VariableMeta(description_key="Already a string")
    assert meta.update_variable_metadata(m).description_key == "Already a string"


def test_render_description_key():
    jinja_description_key = [
        "<% if dim_a == 'x' %> Desc x <% endif %>  ",
        "<% if dim_a == 'y' %>  Desc y <% endif %>",
        "Desc z",
    ]

    var_meta = meta.VariableMeta(description_key=jinja_description_key)  # ty: ignore
    rendered_meta = var_meta.render(dim_dict={"dim_a": "x"})
    assert isinstance(rendered_meta, meta.VariableMeta)
    # A list set programmatically is normalized to a markdown string.
    assert rendered_meta.description_key == "- Desc x\n- Desc z"


def test_render_with_error():
    jinja_title = """
    <% if dim_a == "x" %>Title X<% elif dim_a == "y" %>Title Y<% else %>Default Title<% endif %>
    """.strip()

    var_meta = meta.VariableMeta(title=jinja_title)  # ty: ignore
    with pytest.raises(Exception):
        var_meta.render(dim_dict={"dim_b": "x"})


def test_update_variable_metadata():
    """Test the update_variable_metadata function which handles various post-processing tasks."""
    # Create FaqLink for testing pruning of empty fragment_ids
    empty_faq = meta.FaqLink(gdoc_id="123", fragment_id="   ")
    valid_faq = meta.FaqLink(gdoc_id="456", fragment_id="section-1")

    # Create a presentation with grapher_config containing various fields that need conversion
    presentation = meta.VariablePresentationMeta(
        grapher_config={"map": {"colorScale": {"customNumericValues": "1, 5, 10, 50, 100"}}},
        faqs=[empty_faq, valid_faq],
    )

    # Create variable metadata with fields that need processing
    variable = meta.VariableMeta(
        unit="dollars",
        short_unit="$",
        display={
            "numDecimalPlaces": "2",  # Should be converted to int
            "someOtherField": "value",
        },
        description_key=[
            "Important point",
            "",  # Empty entry should be pruned
            "   ",  # Whitespace-only entry should be pruned
            "Another important point",
        ],
        presentation=presentation,
    )

    # Apply the update function
    updated = meta.update_variable_metadata(variable)
    assert updated.display
    assert updated.presentation
    assert updated.presentation.grapher_config

    # Check that unit and short_unit were copied to display
    assert updated.display["unit"] == "dollars"
    assert updated.display["shortUnit"] == "$"

    # Check numDecimalPlaces was converted to int
    assert updated.display["numDecimalPlaces"] == 2
    assert isinstance(updated.display["numDecimalPlaces"], int)

    # Check empty description_key entries were pruned and the list normalized to a string
    assert updated.description_key == "- Important point\n- Another important point"

    # Check numeric conversions in grapher_config
    color_scale = updated.presentation.grapher_config["map"]["colorScale"]

    # Check string was converted to list
    assert color_scale["customNumericValues"] == [1, 5, 10, 50, 100]

    # Check empty FAQs were pruned
    assert len(updated.presentation.faqs) == 1
    assert updated.presentation.faqs[0].gdoc_id == "456"
