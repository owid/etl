from unittest.mock import patch

import pytest
import streamlit as st

from apps.wizard.utils.components import url_persist


@pytest.fixture
def mock_component():
    """A simple mock Streamlit component that returns a marker string."""

    def _component(*args, **kwargs):
        return f"mock_component called with kwargs={kwargs}"

    return _component


class MockQueryParams(dict):
    """
    Mocks st.query_params so that:
      - It's dictionary-like for 'pop', 'update', direct indexing, etc.
      - It has a 'get_all(key)' method that returns a list of values
        (Streamlit's actual query_params has this method).
    """

    def get_all(self, key):
        val = self.get(key)
        if val is None:
            # No entry for this key => return empty list
            return []
        elif isinstance(val, list):
            # If it's already a list, return it
            return val
        else:
            # If it's a scalar, wrap it in a list
            return [val]


@pytest.fixture
def mock_query_params():
    """
    Returns a fresh MockQueryParams instance for each test.
    """
    return MockQueryParams()


@pytest.fixture
def mock_session_state():
    """
    Returns a fresh dict for simulating st.session_state.
    """
    return {}


def test_url_persist_with_no_session_no_query_default_set(mock_component, mock_query_params, mock_session_state):
    """
    If there's NO session_state value and NO query param,
    the session_state should get the default.
    Because it matches the default, st.query_params stays empty.
    """
    with patch.object(st, "session_state", mock_session_state):
        st.query_params = mock_query_params

        wrapped = url_persist(mock_component)
        result = wrapped(key="test_key", value="some_default")

        # The component was called
        assert "mock_component called with kwargs" in result
        # Session state should now have the default
        assert st.session_state["test_key"] == "some_default"
        # Because it's the default, not stored in query_params
        assert "test_key" not in st.query_params


def test_url_persist_uses_query_param_if_present(mock_component, mock_query_params, mock_session_state):
    """
    If there's NO existing session_state but a query param exists,
    session_state should adopt that value from the query string.
    """
    with patch.object(st, "session_state", mock_session_state):
        # Put something in query_params
        mock_query_params["test_key"] = "from_query"
        st.query_params = mock_query_params

        wrapped = url_persist(mock_component)
        result = wrapped(key="test_key", value="my_default")

        assert "mock_component called with kwargs" in result
        # The session_state should now have picked up the value
        assert st.session_state["test_key"] == "from_query"
        # We haven't changed query_params, so it remains
        assert st.query_params["test_key"] == "from_query"


def test_url_persist_session_state_non_default_updates_query_param(
    mock_component, mock_query_params, mock_session_state
):
    """
    If session_state already has a non-default value and there's no query param,
    we expect update_query_params(key) => st.query_params is updated with that value.
    """
    with patch.object(st, "session_state", mock_session_state):
        mock_session_state["test_key"] = "existing_value"
        st.query_params = mock_query_params

        wrapped = url_persist(mock_component)
        result = wrapped(key="test_key", value="my_default")

        assert "mock_component called with kwargs" in result
        # Session state remains whatever was set
        assert st.session_state["test_key"] == "existing_value"
        # Because it's different from the default, query_params should be set
        assert st.query_params["test_key"] == "existing_value"


def test_url_persist_session_state_equals_default_removes_query_param(
    mock_component, mock_query_params, mock_session_state
):
    """
    If session_state has the same value as the default,
    remove_query_params(key) => st.query_params should not have that key.
    """
    with patch.object(st, "session_state", mock_session_state):
        mock_session_state["test_key"] = "same_as_default"
        st.query_params = mock_query_params

        wrapped = url_persist(mock_component)
        result = wrapped(key="test_key", value="same_as_default")

        assert "mock_component called with kwargs" in result
        # Session state hasn't changed
        assert st.session_state["test_key"] == "same_as_default"
        # Because it matches the default, the query param should be removed
        assert "test_key" not in st.query_params


def test_url_persist_invalid_option_raises_value_error(mock_component, mock_query_params, mock_session_state):
    """
    If the URL contains a value not in 'options',
    a ValueError should be raised by _check_options_params.
    """
    with patch.object(st, "session_state", mock_session_state):
        mock_query_params["color"] = "red"
        st.query_params = mock_query_params

        def _comp(*args, **kwargs):
            return "mock_component with options"

        wrapped = url_persist(_comp)

        with pytest.raises(ValueError) as exc_info:
            wrapped(key="color", options=["blue", "green"])

        assert "not in options" in str(exc_info.value)
