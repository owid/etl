import pytest


@pytest.fixture
def mock_dag():
    # Dag of active steps.
    mock_dag_dict = {
        "steps": {
            "a": set(["b", "c"]),
            "b": set(["e", "d"]),
            "c": set(),
            "d": set(["e", "f"]),
            "e": set(),
            "f": set(),
        },
        "archive": {
            "g": set(["f"]),
            "h": set(["i", "j"]),
        },
    }

    return mock_dag_dict


@pytest.fixture
def mock_expected_dependencies():
    # Expected set of all dependencies for each active step.
    mock_expected_dependencies_dict = {
        "a": set(["b", "c", "e", "d", "f"]),
        "b": set(["e", "d", "f"]),
        "c": set(),
        "d": set(["e", "f"]),
        "e": set(),
        "f": set(),
    }

    return mock_expected_dependencies_dict


@pytest.fixture
def mock_expected_usages():
    # Expected set of all usages for each active step.
    mock_expected_usages_dict = {
        "a": set(),
        "b": set(["a"]),
        "c": set(["a"]),
        "d": set(["b", "a"]),
        "e": set(["d", "b", "a"]),
        "f": set(["d", "b", "a", "g"]),
    }

    return mock_expected_usages_dict


@pytest.fixture
def mock_expected_direct_usages():
    # Expected set of direct usages for each active step.
    mock_expected_direct_usages_dict = {
        "a": set(),
        "b": set(["a"]),
        "c": set(["a"]),
        "d": set(["b"]),
        "e": set(["d", "b"]),
        "f": set(["d", "g"]),
    }

    return mock_expected_direct_usages_dict
