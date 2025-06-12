from owid.catalog import jinja, meta


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
