"""Tests for etl.collection.core.combine module.

This module tests the functionality of combining configuration dimensions,
which is used to merge automatically generated dimensions with YAML-configured
dimensions in ETL collection processing.
"""

from unittest.mock import patch

import pytest

from etl.collection.core.combine import combine_collections, combine_config_dimensions
from etl.collection.core.utils import create_collection_from_config


def _make_explorer_subcollection(
    view_type: str,
    catalog_path: str,
    *,
    extra_dimensions: list[dict] | None = None,
    extra_view_dimensions: dict[str, str] | None = None,
):
    """Build a minimal Explorer sub-collection for combine tests.

    Each sub-collection has one ``view_type`` dimension (with a single choice) and one
    view referencing a fake catalog path. Extra dimensions can be passed via
    ``extra_dimensions`` (e.g. a checkbox dim) — they're appended to the dimensions
    list, and ``extra_view_dimensions`` adds the corresponding choice to the view.
    """
    config = {
        "config": {
            "explorerTitle": "Test Explorer",
            "explorerSubtitle": "",
            "isPublished": False,
        },
        "title": {"title": f"Test {view_type}", "title_variant": ""},
        "default_selection": ["World"],
        "dimensions": [
            {
                "slug": "view_type",
                "name": "View type",
                "choices": [{"slug": view_type, "name": view_type}],
            },
            *(extra_dimensions or []),
        ],
        "views": [
            {
                "dimensions": {"view_type": view_type, **(extra_view_dimensions or {})},
                "indicators": {"y": [{"catalogPath": f"{catalog_path}#{view_type}"}]},
            },
        ],
    }
    # process_views attempts to expand catalog paths against table mappings — we use
    # already-complete fake paths, so the side trip to MySQL/the catalog is unnecessary.
    with patch("etl.collection.core.utils.process_views"):
        return create_collection_from_config(
            config,
            dependencies=set(),
            catalog_path=f"{catalog_path}#test_explorer",
            explorer=True,
            validate_schema=False,
        )


CHECKBOX_DIM = {
    "slug": "by_stage",
    "name": "By stage of supply chain",
    "choices": [
        {"slug": "combined", "name": ""},
        {"slug": "stages", "name": "By stage of supply chain"},
    ],
    "presentation": {
        "type": "checkbox",
        "choice_slug_true": "stages",
    },
}


def test_combine_config_dimensions_overwrite_and_order():
    """Test that YAML config dimensions overwrite auto dimensions and maintain proper ordering.

    This test verifies that:
    1. When both auto and YAML configs have dimensions with the same slug, YAML takes precedence
    2. YAML dimensions are placed first in the combined result
    3. Auto dimensions not present in YAML are appended after YAML dimensions
    4. Names and choices from YAML config properly override auto config values
    """
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
    """Test the choices_top parameter behavior when combining dimension choices.

    This test verifies that:
    1. When choices_top=True: YAML choices are placed first, followed by auto choices
    2. When choices_top=False: auto choices are placed first, followed by YAML choices
    3. Choices with the same slug are merged, with YAML overriding auto properties
    4. The order of choices can be controlled via the choices_top parameter
    """
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


# ---------------------------------------------------------------------------
# combine_collections — multi-table workflow
# ---------------------------------------------------------------------------


def test_combine_collections_preserves_yaml_views():
    """Hand-listed YAML views passed through ``config["views"]`` must end up in the
    combined collection alongside the views accumulated from sub-collections.

    ``etl.collection.core.create.create_collection`` strips YAML views from the configs
    handed to per-table sub-collections (so they don't get duplicated). Those views are
    then forwarded here via the ``config`` argument and must be merged into the final
    combined collection — otherwise users authoring a multi-table explorer have no way to
    declare a hand-listed view in YAML.
    """
    # Sub-collections both use by_stage=combined; the YAML view uses by_stage=stages so
    # it occupies a unique dimensional slot (this mirrors the actual food-footprints case
    # where the lifecycle-stage view is the only one with by_stage=stages).
    sub_a = _make_explorer_subcollection(
        "commodity",
        "poore_2018",
        extra_dimensions=[CHECKBOX_DIM],
        extra_view_dimensions={"by_stage": "combined"},
    )
    sub_b = _make_explorer_subcollection(
        "specific",
        "clark_2022",
        extra_dimensions=[CHECKBOX_DIM],
        extra_view_dimensions={"by_stage": "combined"},
    )

    yaml_view = {
        "dimensions": {"view_type": "commodity", "by_stage": "stages"},
        "indicators": {"y": [{"catalogPath": "lifecycle#stages"}]},
    }

    config = {
        "config": {
            "explorerTitle": "Combined",
            "explorerSubtitle": "",
            "isPublished": False,
        },
        "title": {"title": "Combined", "title_variant": ""},
        "default_selection": ["World"],
        "dimensions": [
            {
                "slug": "view_type",
                "name": "View type",
                "choices": [
                    {"slug": "commodity", "name": "commodity"},
                    {"slug": "specific", "name": "specific"},
                ],
            },
            CHECKBOX_DIM,
        ],
        "views": [yaml_view],
    }

    with patch("etl.collection.core.utils.process_views"):
        combined = combine_collections(
            collections=[sub_a, sub_b],
            catalog_path="combined#combined",
            config=config,
            is_explorer=True,
        )

    catalog_paths = [v.indicators.y[0].catalogPath for v in combined.views if v.indicators.y]
    assert "poore_2018#commodity" in catalog_paths, "sub-collection A's view was lost"
    assert "clark_2022#specific" in catalog_paths, "sub-collection B's view was lost"
    assert "lifecycle#stages" in catalog_paths, "YAML hand-listed view was not preserved"


def test_combine_collections_allows_identical_checkbox_dim():
    """When every sub-collection declares the same checkbox dimension (same slug, same
    choice slugs, same ``choice_slug_true``), the combine should succeed — the merge is
    structurally equivalent to a 2-choice radio.
    """
    sub_a = _make_explorer_subcollection(
        "commodity",
        "poore_2018",
        extra_dimensions=[CHECKBOX_DIM],
        extra_view_dimensions={"by_stage": "combined"},
    )
    sub_b = _make_explorer_subcollection(
        "specific",
        "clark_2022",
        extra_dimensions=[CHECKBOX_DIM],
        extra_view_dimensions={"by_stage": "combined"},
    )

    config = {
        "config": {
            "explorerTitle": "Combined",
            "explorerSubtitle": "",
            "isPublished": False,
        },
        "title": {"title": "Combined", "title_variant": ""},
        "default_selection": ["World"],
        "dimensions": [
            {
                "slug": "view_type",
                "name": "View type",
                "choices": [
                    {"slug": "commodity", "name": "commodity"},
                    {"slug": "specific", "name": "specific"},
                ],
            },
            CHECKBOX_DIM,
        ],
        "views": [],
    }

    with patch("etl.collection.core.utils.process_views"):
        combined = combine_collections(
            collections=[sub_a, sub_b],
            catalog_path="combined#combined",
            config=config,
            is_explorer=True,
        )

    by_stage_dim = next(d for d in combined.dimensions if d.slug == "by_stage")
    assert by_stage_dim.ui_type == "checkbox", "checkbox presentation was lost during combine"
    assert sorted(c.slug for c in by_stage_dim.choices) == ["combined", "stages"]


def test_combine_collections_rejects_differing_checkbox_dim():
    """When sub-collections declare a checkbox dimension with different choice slugs, the
    combine logic can't safely merge them (the checkbox would silently turn into a 3+-choice
    widget). It should raise a ``NotImplementedError`` with a message pointing at the
    actual problem."""
    sub_a = _make_explorer_subcollection(
        "commodity",
        "poore_2018",
        extra_dimensions=[CHECKBOX_DIM],
        extra_view_dimensions={"by_stage": "combined"},
    )

    # Sub-B has a checkbox dim with the same name/presentation but a different "off"
    # choice slug. The pre-existing structural-equality check ignores choice slugs, so
    # only the new check (introduced to allow checkbox merging when safe) can detect this.
    different_checkbox = {
        "slug": "by_stage",
        "name": "By stage of supply chain",
        "choices": [
            {"slug": "off", "name": ""},
            {"slug": "stages", "name": "By stage of supply chain"},
        ],
        "presentation": {"type": "checkbox", "choice_slug_true": "stages"},
    }
    sub_b = _make_explorer_subcollection(
        "specific",
        "clark_2022",
        extra_dimensions=[different_checkbox],
        extra_view_dimensions={"by_stage": "off"},
    )

    config = {
        "config": {"explorerTitle": "Combined", "explorerSubtitle": "", "isPublished": False},
        "title": {"title": "Combined", "title_variant": ""},
        "default_selection": ["World"],
        "dimensions": [],
        "views": [],
    }

    with patch("etl.collection.core.utils.process_views"):
        with pytest.raises(NotImplementedError, match="Checkbox dimension"):
            combine_collections(
                collections=[sub_a, sub_b],
                catalog_path="combined#combined",
                config=config,
                is_explorer=True,
            )
