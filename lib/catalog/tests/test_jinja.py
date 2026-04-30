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


def test_empty_jinja_output_drops_list_item():
    """A Jinja template rendering to empty inside a list element drops that element."""
    out = jinja._expand_jinja(
        [
            "<% if age == '0' %>kept<% endif %>",
            "always",
            "<% if age == '0' %>also-kept<% endif %>",
        ],
        dim_dict={"age": "10"},
    )
    assert out == ["always"]


def test_list_item_dict_emptied_by_jinja_is_dropped():
    """A list of dicts where every key is suppressed by Jinja drops the whole entry."""
    out = jinja._expand_jinja(
        [
            {
                "label": "<% if age == '0' %>at-birth<% endif %>",
                "yEquals": "<% if age == '0' %>105<% endif %>",
            },
            {"label": "always", "yEquals": "100"},
        ],
        dim_dict={"age": "10"},
    )
    assert out == [{"label": "always", "yEquals": "100"}]
