import pytest

from etl.collection.model.core import Definitions
from etl.collection.model.view import CommonView, merge_common_metadata_by_dimension


def test_merge_common_metadata_1():
    """Work as expected."""
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
    """Work as expected, more complex example."""
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
    """Work as expected."""
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

    with pytest.raises(ValueError):
        _ = merge_common_metadata_by_dimension(common_params, active_dimensions, custom_config, "config")


def test_definitions():
    """Work as expected."""
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
    """Now we test when we give priority to the common params.

    That is, common_params should override the view_config."""
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
    """Now we test when we give priority to the common params.

    That is, common_params should override the view_config.

    In this case, the code should fail. That's because there is a conflict in others1.description_aux1. Both sex=female and age=10 are trying to set it and they have the same priority. Since view_config has no priority, the conflict is not resolved!"""
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
    with pytest.raises(ValueError):
        _ = merge_common_metadata_by_dimension(
            common_config=common_params,
            view_dimensions=active_dimensions,
            view_config=custom_config,
            field_name="config",
            common_has_priority=True,
        )
