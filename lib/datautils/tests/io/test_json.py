"""Test functions in owid.datautils.io.local module.

"""

from unittest.mock import mock_open, patch

from pytest import warns

from owid.datautils.io.json import load_json, save_json


class TestLoadJson:
    @patch("builtins.open", new_callable=mock_open, read_data='{"1": "10", "2": "20"}')
    def test_load_json_without_duplicated_keys(self, _):
        assert load_json(_) == {"1": "10", "2": "20"}

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"1": {"1_1": "1", "1_2": "2"}, "2": "20"}',
    )
    def test_load_json_without_duplicated_keys_with_more_levels(self, _):
        assert load_json(_) == {"1": {"1_1": "1", "1_2": "2"}, "2": "20"}

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='[{"1": {"1_1": "1", "1_2": "2"}, "2": "20"}, {"1": "10"}]',
    )
    def test_load_json_without_duplicated_keys_with_more_levels_in_a_list(self, _):
        # Here the key "1" is repeated, however, it is in a different dictionary, so it is not a duplicated key.
        assert load_json(_) == [{"1": {"1_1": "1", "1_2": "2"}, "2": "20"}, {"1": "10"}]

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"1": "10", "2": "20", "1": "100"}',
    )
    def test_load_json_with_duplicated_keys_and_no_warning(self, _):
        assert load_json(_, warn_on_duplicated_keys=False) == {"1": "100", "2": "20"}

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"1": "10", "2": "20", "1": "100"}',
    )
    def test_warn_on_load_json_with_duplicated_keys_and_warning(self, _):
        with warns(UserWarning, match="Duplicated"):
            assert load_json(_, warn_on_duplicated_keys=True) == {"1": "100", "2": "20"}

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_load_empty_json(self, _):
        assert load_json(_) == {}


def test_save_json(tmpdir):
    data = {"1": "10", "2": "20"}
    save_json(data, tmpdir / "test.json")
