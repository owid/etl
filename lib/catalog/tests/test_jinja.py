from owid.catalog.core import jinja, meta


def test_expand_jinja():
    m = meta.VariableMeta(
        title="Title << foo >>",
        description_key=[
            '<% if foo == "bar" %>This is bar<% else %>This is not bar<% endif %>',
        ],
        presentation=meta.VariablePresentationMeta(
            title_variant="Variant << foo >>",
        ),
        display={
            "isProjection": "<% if foo == 'bar' %>true<% else %>false<% endif %>",
        },
    )
    out = jinja._expand_jinja(m, dim_dict={"foo": "bar"})
    assert out.to_dict() == {
        "title": "Title bar",
        "description_key": ["This is bar"],
        "presentation": {"title_variant": "Variant bar"},
        "display": {"isProjection": True},
    }


def test_as_value_filter_coerces_to_int_and_float():
    """The `as_value` filter coerces numeric Jinja output to int/float."""
    out = jinja._expand_jinja(
        {
            "min": "<% if age == '0' %><< 90 | as_value >><% endif %>",
            "max": "<% if age == '0' %><< 120.5 | as_value >><% endif %>",
            "label": "<% if age == '0' %>biological ratio<% endif %>",
        },
        dim_dict={"age": "0"},
    )
    assert out == {"min": 90, "max": 120.5, "label": "biological ratio"}


def test_empty_jinja_output_drops_dict_key():
    """A Jinja template that renders to empty string drops the key from a dict."""
    out = jinja._expand_jinja(
        {
            "min": "<% if age == '0' %><< 90 | as_value >><% endif %>",
            "max": "<% if age == '0' %><< 120 | as_value >><% endif %>",
            "always": "stays",
        },
        dim_dict={"age": "10"},
    )
    assert out == {"always": "stays"}


def test_empty_jinja_output_in_list_preserves_empty_string():
    """Plain Jinja that renders empty inside a list keeps "" (pre-PR behavior).

    Authors who want an item dropped should add an `<% else %>` branch with
    real content, or use `| as_value` (covered separately). Auto-dropping
    plain empty strings was tried in #5999 but caused collateral damage on
    fields like `subtitle` where empty is a valid signal to Grapher.
    """
    out = jinja._expand_jinja(
        [
            "<% if age == '0' %>kept<% endif %>",
            "always",
            "<% if age == '0' %>also-kept<% endif %>",
        ],
        dim_dict={"age": "10"},
    )
    assert out == ["", "always", ""]


def test_list_item_dict_emptied_by_as_value_is_dropped():
    """When every numeric key in a dict opts into drop via `as_value`, the `{}` is pruned from the list.

    This guards the `comparisonLines: [{}]` case: an entry whose numeric
    fields all rendered empty would otherwise survive as `{}` and break
    Grapher schema validation.
    """
    out = jinja._expand_jinja(
        [
            {
                "yEquals": "<% if age == '0' %><< 105 | as_value >><% endif %>",
                "yMin": "<% if age == '0' %><< 100 | as_value >><% endif %>",
            },
            {"yEquals": 100},
        ],
        dim_dict={"age": "10"},
    )
    assert out == [{"yEquals": 100}]


def test_dataclass_scalar_empty_jinja_renders_to_empty_string():
    """Empty Jinja on a scalar dataclass field stays as "" (not None).

    This is back-compat for metadata that uses `<% if cond %>x<% endif %>`
    on top-level fields like `unit`, `title_public`, `description_short`.
    Existing downstream code (e.g. `grapher_checks`) treats `unit: ""` as
    valid but `unit: None` as a hard failure.

    Dict keys also default to preserving "" so authors can force empty fields
    like `subtitle: ""` via Jinja (Grapher reads "" as "render no subtitle"
    rather than falling back to `description_short`). Opt into key-drop with
    the `| as_value` filter — see `test_empty_jinja_output_drops_dict_key`.
    """
    m = meta.VariableMeta(
        unit="<% if foo == 'bar' %>kg<% endif %>",
        description_key=["<% if foo == 'bar' %>kept<% endif %>", "always"],
        display={"numDecimalPlaces": "<% if foo == 'bar' %>2<% endif %>"},
    )
    out = jinja._expand_jinja(m, dim_dict={"foo": "other"})
    # Scalar dataclass field: "" preserved.
    assert out.unit == ""
    # List items: "" preserved too (use `<% else %>` for non-empty alternatives).
    assert out.description_key == ["", "always"]
    # Dict key without `as_value`: "" preserved (back-compat for `subtitle: ""`).
    assert out.display == {"numDecimalPlaces": ""}


def test_empty_jinja_in_grapher_config_preserves_empty_string():
    """Force-empty subtitle/note via Jinja must round-trip as "" through dict expansion.

    Regression test for the `subtitle: "{definitions.global.projections}"`
    pattern in un_wpp.meta.yml: when the inherited Jinja renders empty (e.g.
    `variant == 'estimates'`), Grapher must see `subtitle: ""` so it renders
    no subtitle — not a missing key, which would fall back to
    `description_short`.
    """
    out = jinja._expand_jinja(
        {
            "subtitle": "<% if variant != 'estimates' %>scenario blurb<% endif %>",
            "note": "<% if variant != 'estimates' %>note text<% endif %>",
            "originUrl": "https://example.org",
        },
        dim_dict={"variant": "estimates"},
    )
    assert out == {"subtitle": "", "note": "", "originUrl": "https://example.org"}
