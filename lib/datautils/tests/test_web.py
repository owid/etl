"""Test functions in owid.datautils.web module.

"""


from unittest import mock

from pytest import warns

from owid.datautils.io.json import load_json
from owid.datautils.web import download_file_from_url, get_base_url

from .mocks import MockResponse

# Mock URLs and responses.
MOCK_URL_1 = "http://owid_example.com/test.json"
MOCK_RESPONSE_1 = {"key": "value"}
MOCK_URL_2 = "http://new_owid_example.com/new_test.json"
MOCK_RESPONSE_2 = {"new_key": "new_value"}
MOCK_WRONG_URL = "http://wrong_owid_url.com/wrong_test.json"


class TestGetBaseUrl:
    def test_on_correct_urls_returning_scheme(self):
        # With http.
        assert get_base_url("http://example.com") == "http://example.com"
        assert get_base_url("http://example.com/some/path") == "http://example.com"
        assert get_base_url("http://example.com.au/some/path") == "http://example.com.au"
        # With https.
        assert get_base_url("https://example.com") == "https://example.com"
        assert get_base_url("https://example.com/some/path") == "https://example.com"
        assert get_base_url("https://example.com.au/some/path") == "https://example.com.au"

    def test_on_correct_urls_without_returning_scheme(self):
        # With http.
        assert get_base_url("http://example.com", include_scheme=False) == "example.com"
        assert get_base_url("http://example.com/some/path", include_scheme=False) == "example.com"
        assert get_base_url("http://example.com.au/some/path", include_scheme=False) == "example.com.au"
        # With https.
        assert get_base_url("https://example.com", include_scheme=False) == "example.com"
        assert get_base_url("https://example.com/some/path", include_scheme=False) == "example.com"
        assert get_base_url("https://example.com.au/some/path", include_scheme=False) == "example.com.au"

    def test_on_urls_without_scheme_returning_scheme(self):
        with warns(UserWarning):
            assert get_base_url("example.com") == "http://example.com"
            assert get_base_url("example.com/some/path") == "http://example.com"
            assert get_base_url("example.com.au/some/path") == "http://example.com.au"
            assert get_base_url("bad_url") == "http://bad_url"

    def test_on_urls_without_scheme_without_returning_scheme(self):
        with warns(UserWarning):
            assert get_base_url("example.com", include_scheme=False) == "example.com"
            assert get_base_url("example.com/some/path", include_scheme=False) == "example.com"
            assert get_base_url("example.com.au/some/path", include_scheme=False) == "example.com.au"
            assert get_base_url("bad_url", include_scheme=False) == "bad_url"


# Mock function to replace requests.get.
def mocked_requests_get(*args, **kwargs):
    if args[0] == MOCK_URL_1:
        return MockResponse(MOCK_RESPONSE_1, 200)
    elif args[0] == MOCK_URL_2:
        return MockResponse(MOCK_RESPONSE_2, 200)
    else:
        return MockResponse(None, 404)


@mock.patch("requests.Session.get", side_effect=mocked_requests_get)
class TestDownloadFileFromUrl:
    def test_download_file_from_url_with_valid_urls(self, mock_get, tmp_path):
        # Send a mock request to MOCK_URL_1, store the result in a temporary file, and check that it is MOCK_RESPONSE_1.
        tmp_file = tmp_path / "test_1.json"
        download_file_from_url(url=MOCK_URL_1, local_path=tmp_file)
        assert load_json(tmp_file) == MOCK_RESPONSE_1

        # Send a mock request to MOCK_URL_2, store the result in a temporary file, and check that it is MOCK_RESPONSE_2.
        download_file_from_url(url=MOCK_URL_2, local_path=tmp_file)
        assert load_json(tmp_file) == MOCK_RESPONSE_2

    def test_download_file_from_url_with_valid_urls_and_ciphers_low(self, mock_get, tmp_path):
        # Send a mock request to MOCK_URL_1, store the result in a temporary file, and check that it is MOCK_RESPONSE_1.
        tmp_file = tmp_path / "test_1.json"
        download_file_from_url(url=MOCK_URL_1, local_path=tmp_file, ciphers_low=True)
        assert load_json(tmp_file) == MOCK_RESPONSE_1

        # Send a mock request to MOCK_URL_2, store the result in a temporary file, and check that it is MOCK_RESPONSE_2.
        download_file_from_url(url=MOCK_URL_2, local_path=tmp_file, ciphers_low=True)
        assert load_json(tmp_file) == MOCK_RESPONSE_2

    # def test_download_file_from_url_with_invalid_url(self, mock_get, tmp_path):
    #     # TODO: I couldn't manage to catch the exception that download_file_from_url raises when the url is wrong.
