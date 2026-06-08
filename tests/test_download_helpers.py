"""Tests for etl.download_helpers, focused on the bot-challenge (Anubis) fallback."""

from unittest.mock import MagicMock, patch

from etl import download_helpers


def _challenge_response() -> MagicMock:
    """A response that looks like an Anubis bot-challenge page."""
    resp = MagicMock()
    resp.headers = {"content-type": "text/html; charset=utf-8", "set-cookie": "techaro.lol-anubis-auth=; Path=/"}
    return resp


def _file_response(data: bytes, content_type: str = "application/zip") -> MagicMock:
    """A response that streams `data` like a real file download."""
    resp = MagicMock()
    resp.headers = {"content-type": content_type, "content-length": str(len(data))}
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    resp.iter_content.return_value = iter([data])
    resp.raise_for_status.return_value = None
    return resp


def test_is_bot_challenge_detects_anubis():
    assert download_helpers._is_bot_challenge(_challenge_response()) is True


def test_is_bot_challenge_ignores_real_file_and_plain_html():
    assert download_helpers._is_bot_challenge(_file_response(b"PK\x03\x04")) is False
    html_no_anubis = MagicMock()
    html_no_anubis.headers = {"content-type": "text/html", "set-cookie": "session=abc"}
    assert download_helpers._is_bot_challenge(html_no_anubis) is False


def test_download_retries_with_plain_ua_on_bot_challenge(tmp_path):
    data = b"PK\x03\x04real-zip-bytes"
    out = tmp_path / "f.zip"
    with patch.object(
        download_helpers.requests, "get", side_effect=[_challenge_response(), _file_response(data)]
    ) as mock_get:
        download_helpers.download("http://example.test/f.zip", str(out), quiet=True)

    # It tried the browser UA first, then retried with the default (no custom UA).
    assert mock_get.call_count == 2
    assert "Mozilla" in mock_get.call_args_list[0].kwargs["headers"]["User-Agent"]
    assert "headers" not in mock_get.call_args_list[1].kwargs
    assert out.read_bytes() == data


def test_download_raises_when_both_user_agents_are_walled(tmp_path):
    out = tmp_path / "f.zip"
    with patch.object(download_helpers.requests, "get", side_effect=[_challenge_response(), _challenge_response()]):
        try:
            download_helpers.download("http://example.test/f.zip", str(out), quiet=True)
        except download_helpers.DownloadCorrupted as e:
            assert "bot-challenge" in str(e)
        else:
            raise AssertionError("expected DownloadCorrupted when both UAs are challenged")
    assert not out.exists()
