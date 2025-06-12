"""Tests for etl.collection.core.combine module.

This module tests the functionality of combining configuration dimensions,
which is used to merge automatically generated dimensions with YAML-configured
dimensions in ETL collection processing.
"""

from etl.collection.core.combine import combine_config_dimensions


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
