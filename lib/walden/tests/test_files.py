#
#  test_files.py
#  walden
#

import hashlib
import tempfile
from pathlib import Path

import pytest
import requests_mock
from owid.walden import files

test_dataset = """some,data,wow
42,24,00
42,24,00
42,24,00
42,24,00
42,24,00
42,24,00
"""
encoded = test_dataset.encode("utf-8")
md5 = hashlib.md5()
md5.update(encoded)
expected_md5 = md5.hexdigest()


def test_download_no_md5():
    with requests_mock.Mocker() as mocker, tempfile.NamedTemporaryFile() as destination:
        data_url = "https://very/important/data.csv"
        mocker.get(data_url, content=encoded)
        files.download(data_url, destination.name)

        with open(destination.name) as _destination:
            content = "".join(_destination.readlines())
            assert content == test_dataset


def test_download_with_md5():
    with requests_mock.Mocker() as mocker, tempfile.NamedTemporaryFile() as destination:
        data_url = "https://very/important/data.csv"
        mocker.get(data_url, content=encoded)
        files.download(data_url, destination.name, expected_md5=expected_md5)

        with open(destination.name) as _destination:
            content = "".join(_destination.readlines())
            assert content == test_dataset


def test_download_with_wrong_md5_raises():
    with requests_mock.Mocker() as mocker, tempfile.NamedTemporaryFile(delete=False) as destination:
        data_url = "https://very/important/data.csv"
        mocker.get(data_url, content=encoded)
        with pytest.raises(files.ChecksumDoesNotMatch):
            files.download(data_url, destination.name, expected_md5="oh no.")


def test_download_with_windows_newlines_size():
    encoded = b"a\r\nb"

    with requests_mock.Mocker() as mocker, tempfile.NamedTemporaryFile(delete=False) as destination:
        data_url = "https://very/important/data.csv"
        mocker.get(data_url, content=encoded)
        files.download(data_url, destination.name)
        assert Path(destination.name).stat().st_size == len(encoded)


def test_empty_checksum():
    with tempfile.NamedTemporaryFile() as tmp:
        md5 = files.checksum(tmp.name)

    assert md5 == hashlib.md5().hexdigest()


def test_known_checksum():
    s = "Hello world o/\n"
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write("Hello world o/\n".encode("utf8"))
        tmp.flush()
        md5 = files.checksum(tmp.name)

    assert md5 == hashlib.md5(s.encode("utf8")).hexdigest()
