"""Tests for common metadata merging functionality in ETL collections.

This module tests the complex logic for merging metadata configurations based on
dimensions and priority rules, which is essential for creating dynamic collection views.

Table of Contents:
- test_merge_common_metadata_1: Tests basic metadata merging with dimension-based priority
- test_merge_common_metadata_2: Tests complex nested metadata merging scenarios
- test_merge_common_metadata_3: Tests error handling when dimensions cannot be resolved
- test_definitions: Tests validation of common view definitions for duplicates
- test_merge_common_metadata_4: Tests priority system where common params override view config
- test_merge_common_metadata_5: Tests conflict detection when multiple dimensions compete
"""

import pytest

from etl.collection.exceptions import CommonViewParamConflict
from etl.collection.model.core import Definitions
from etl.collection.model.view import CommonView, merge_common_metadata_by_dimension


def test_merge_common_metadata_1():
    """Test basic metadata merging with hierarchical dimension-based priority.

    This test verifies the core functionality where:
    - Top-level params apply to all views
    - Level-1 params apply when specific dimensions match
    - Level-2 params (more specific) override level-1 params
    - Only relevant dimensions are considered for merging
    - Custom config gets merged with common metadata
    """
    common_params = [
        # Top-level params
        {
            "config": {
                "subtitle": "Something that is very common and should be kept",
            }
        },
        # Level-1 params
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
            },
        },
        {
            "dimensions": {
                "age": "10",
            },
            "config": {
                "title": "Something from level 1, age 10",
                "others": {
                    "description_aux1": "Something from level 1, age 10",
                },
            },
        },
        {
            "dimensions": {
                "sex": "male",
            },
            "config": {
                "title": "Something from level 1 that should be ignored",
                "others": {
                    "description_aux1": "Something from level 1 that should be ignored",
                },
            },
        },
        # Level-2 params
        {
            "dimensions": {
                "sex": "female",
                "age": "10",
            },
            "config": {
                "title": "Something from level 2, female aged 10",
            },
        },
    ]

    # Suppose we want the config for sex=female and age=10
    active_dimensions = {"sex": "female", "age": "10", "projection": "medium"}

    # This custom config adds a nested field for "others"
    custom_config = {
        "others": {
            "description_aux2": "custom",
        },
        "units": "per 100,000",
    }

    common_params = [CommonView.from_dict(r) for r in common_params]
    config = merge_common_metadata_by_dimension(common_params, active_dimensions, custom_config, "config")

    # Expected config
    config_expected = {
        "subtitle": "Something that is very common and should be kept",
        "title": "Something from level 2, female aged 10",
        "note": "Something from level 1, female",
        "others": {
            "description_aux1": "Something from level 1, age 10",
            "description_aux2": "custom",
        },
        "units": "per 100,000",
    }

    assert config == config_expected


def test_merge_common_metadata_2():
    """Test complex nested metadata merging with deep object structures.

    This test extends the basic functionality to handle:
    - Deeply nested configuration objects
    - Arrays within nested structures
    - Multiple levels of nesting (others1.others2.others3)
    - Proper merging of nested objects at different hierarchy levels
    - Custom config overriding specific nested properties
    """
    common_params = [
        # Top-level params
        {
            "config": {
                "subtitle": "Something that is very common and should be kept",
            }
        },
        # Level-1 params
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
                "others1": {
                    "description_aux1": [2, 3, 4],
                },
            },
        },
        {
            "dimensions": {
                "age": "10",
            },
            "config": {
                "title": "Something from level 1, age 10",
                "others1": {
                    "others2": {
                        "others3": {
                            "description_aux1": "Something from level 1, age 10",
                        },
                    },
                    "description_aux1": [1, 2, 3],
                },
            },
        },
        {
            "dimensions": {
                "sex": "male",
            },
            "config": {
                "title": "Something from level 1 that should be ignored",
            },
        },
        # Level-2 params
        {
            "dimensions": {
                "sex": "female",
                "age": "10",
            },
            "config": {
                "title": "Something from level 2, female aged 10",
                "others1": {
                    "others2": {
                        "description_aux2": "Something from level 2, female aged 10",
                    }
                },
            },
        },
    ]

    # Suppose we want the config for sex=female and age=10
    active_dimensions = {"sex": "female", "age": "10", "projection": "medium"}

    # This custom config adds a nested field for "others"
    custom_config = {
        "others": {
            "description_aux2": "custom",
        },
        "others1": {
            "description_aux1": [0],
            "others2": {
                "others3": {
                    "description_aux2": "custom",
                    "others4": "custom",
                },
            },
        },
        "units": "per 100,000",
    }

    common_params = [CommonView.from_dict(r) for r in common_params]
    config = merge_common_metadata_by_dimension(common_params, active_dimensions, custom_config, "config")

    # Expected config
    config_expected = {
        "subtitle": "Something that is very common and should be kept",
        "title": "Something from level 2, female aged 10",
        "note": "Something from level 1, female",
        "others1": {
            "description_aux1": [0],
            "others2": {
                "description_aux2": "Something from level 2, female aged 10",
                "others3": {
                    "description_aux1": "Something from level 1, age 10",
                    "description_aux2": "custom",
                    "others4": "custom",
                },
            },
        },
        "others": {"description_aux2": "custom"},
        "units": "per 100,000",
    }

    assert config == config_expected


def test_merge_common_metadata_3():
    """Test error handling when dimension resolution fails.

    This test verifies that the system properly raises a CommonViewParamConflict when:
    - Required dimensions cannot be resolved from common params
    - The active dimensions don't have sufficient matching common views
    - Missing level-2 params prevent proper metadata resolution
    """
    common_params = [
        # Top-level params
        {
            "config": {
                "subtitle": "Something that is very common and should be kept",
            }
        },
        # Level-1 params
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
            },
        },
        {
            "dimensions": {
                "age": "10",
            },
            "config": {
                "title": "Something from level 1, age 10",
                "others": {
                    "description_aux1": "Something from level 1, age 10",
                },
            },
        },
        {
            "dimensions": {
                "sex": "male",
            },
            "config": {
                "title": "Something from level 1 that should be ignored",
                "others": {
                    "description_aux1": "Something from level 1 that should be ignored",
                },
            },
        },
        # Level-2 params
    ]

    # Suppose we want the config for sex=female and age=10
    active_dimensions = {"sex": "female", "age": "10", "projection": "medium"}

    # This custom config adds a nested field for "others"
    custom_config = {
        "others": {
            "description_aux2": "custom",
        },
        "units": "per 100,000",
    }

    common_params = [CommonView.from_dict(r) for r in common_params]

    with pytest.raises(CommonViewParamConflict):
        _ = merge_common_metadata_by_dimension(common_params, active_dimensions, custom_config, "config")


def test_definitions():
    """Test validation of common view definitions for duplicate detection.

    This test ensures that the Definitions class properly validates:
    - Duplicate common_views with identical dimension combinations
    - Proper error reporting when conflicting definitions are found
    - Data integrity enforcement in collection configurations
    """
    common_params = [
        # Top-level params
        {
            "config": {
                "subtitle": "Something that is very common and should be kept",
            }
        },
        # Level-1 params
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
            },
        },
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
            },
        },
    ]

    with pytest.raises(ValueError):
        _ = Definitions.from_dict({"common_views": common_params})


def test_merge_common_metadata_4():
    """Test priority system where common params override view config.

    This test verifies the `common_has_priority=True` behavior where:
    - Common parameters take precedence over view configuration
    - More specific common params (level-2) still override less specific ones
    - Custom view config is used only when no common param conflicts exist
    - The priority system resolves configuration conflicts appropriately
    """
    common_params = [
        # Top-level params
        {
            "config": {
                "subtitle": "Something that is very common and should be kept",
                "units": "IT DIDN'T WORKED",
            }
        },
        # Level-1 params
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
                "others1": {
                    "description_aux1": [2, 3, 4],
                },
                "units": "IT WORKED",
            },
        },
        {
            "dimensions": {
                "age": "10",
            },
            "config": {
                "title": "Something from level 1, age 10",
                "others1": {
                    "others2": {
                        "others3": {
                            "description_aux1": "Something from level 1, age 10",
                        },
                    },
                },
            },
        },
        {
            "dimensions": {
                "sex": "male",
            },
            "config": {
                "title": "Something from level 1 that should be ignored",
            },
        },
        # Level-2 params
        {
            "dimensions": {
                "sex": "female",
                "age": "10",
            },
            "config": {
                "title": "Something from level 2, female aged 10",
                "others1": {
                    "others2": {
                        "description_aux2": "Something from level 2, female aged 10",
                    }
                },
            },
        },
    ]

    # Suppose we want the config for sex=female and age=10
    active_dimensions = {"sex": "female", "age": "10", "projection": "medium"}

    # This custom config adds a nested field for "others"
    custom_config = {
        "others": {
            "description_aux2": "custom",
        },
        "others1": {
            "description_aux1": [2, 3, 4],
            "others2": {
                "others3": {
                    "description_aux2": "custom",
                    "others4": "custom",
                },
            },
        },
        "units": "per 100,000",
    }

    common_params = [CommonView.from_dict(r) for r in common_params]
    config = merge_common_metadata_by_dimension(
        common_config=common_params,
        view_dimensions=active_dimensions,
        view_config=custom_config,
        field_name="config",
        common_has_priority=True,
    )

    # Expected config
    config_expected = {
        "subtitle": "Something that is very common and should be kept",
        "title": "Something from level 2, female aged 10",
        "note": "Something from level 1, female",
        "others1": {
            "description_aux1": [2, 3, 4],
            "others2": {
                "description_aux2": "Something from level 2, female aged 10",
                "others3": {
                    "description_aux1": "Something from level 1, age 10",
                    "description_aux2": "custom",
                    "others4": "custom",
                },
            },
        },
        "others": {"description_aux2": "custom"},
        "units": "IT WORKED",
    }

    assert config == config_expected


def test_merge_common_metadata_5():
    """Test conflict detection when multiple dimensions compete for same property.

    This test verifies that the system properly detects and reports conflicts when:
    - Multiple common params at the same priority level try to set the same property
    - sex=female and age=10 both attempt to set others1.description_aux1
    - View config cannot resolve the conflict due to lower priority
    - A ValueError is raised to indicate the unresolvable conflict
    """
    common_params = [
        # Top-level params
        {
            "config": {
                "subtitle": "Something that is very common and should be kept",
                "units": "IT DIDN'T WORKED",
            }
        },
        # Level-1 params
        {
            "dimensions": {
                "sex": "female",
            },
            "config": {
                "title": "Something from level 1, female",
                "note": "Something from level 1, female",
                "others1": {
                    "description_aux1": [2, 3, 4],
                },
                "units": "IT WORKED",
            },
        },
        {
            "dimensions": {
                "age": "10",
            },
            "config": {
                "title": "Something from level 1, age 10",
                "others1": {
                    "others2": {
                        "others3": {
                            "description_aux1": "Something from level 1, age 10",
                        },
                    },
                    "description_aux1": [1, 2, 3],
                },
            },
        },
        {
            "dimensions": {
                "sex": "male",
            },
            "config": {
                "title": "Something from level 1 that should be ignored",
            },
        },
        # Level-2 params
        {
            "dimensions": {
                "sex": "female",
                "age": "10",
            },
            "config": {
                "title": "Something from level 2, female aged 10",
                "others1": {
                    "others2": {
                        "description_aux2": "Something from level 2, female aged 10",
                    }
                },
            },
        },
    ]

    # Suppose we want the config for sex=female and age=10
    active_dimensions = {"sex": "female", "age": "10", "projection": "medium"}

    # This custom config adds a nested field for "others"
    custom_config = {
        "others": {
            "description_aux2": "custom",
        },
        "others1": {
            "description_aux1": [0],
            "others2": {
                "others3": {
                    "description_aux2": "custom",
                    "others4": "custom",
                },
            },
        },
        "units": "per 100,000",
    }

    common_params = [CommonView.from_dict(r) for r in common_params]
    with pytest.raises(CommonViewParamConflict):
        _ = merge_common_metadata_by_dimension(
            common_config=common_params,
            view_dimensions=active_dimensions,
            view_config=custom_config,
            field_name="config",
            common_has_priority=True,
        )
