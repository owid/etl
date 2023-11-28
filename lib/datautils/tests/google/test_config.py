import os
import tempfile
from pathlib import Path
from unittest import mock

import pydrive2
from pytest import raises

from owid.datautils.google.config import (
    _check_google_config,
    google_config_init,
    is_google_config_init,
)

some_error_mock = mock.Mock()
some_error_mock.side_effect = FileNotFoundError


@mock.patch("owid.datautils.google.config._check_google_config", some_error_mock)
def test_google_config_is_init_false():
    assert not is_google_config_init()


@mock.patch("owid.datautils.google.config._check_google_config", return_value=None)
def test_google_config_is_init_true(mock_check):
    assert is_google_config_init()


def test_check_google_config_1():
    """All files are created"""
    with tempfile.TemporaryDirectory() as config_dir:
        with mock.patch("owid.datautils.google.config.CONFIG_DIR", config_dir), mock.patch(
            "owid.datautils.google.config.CLIENT_SECRETS_PATH",
            Path(config_dir) / "google_client_secrets.json",
        ) as secrets_dir, mock.patch(
            "owid.datautils.google.config.SETTINGS_PATH",
            Path(config_dir) / "google_settings.yaml",
        ) as settings_dir, mock.patch(
            "owid.datautils.google.config.CREDENTIALS_PATH",
            Path(config_dir) / "google_credentials.json",
        ) as creds_dir:
            with open(secrets_dir, "w") as f:
                f.write("This is test")
            with open(settings_dir, "w") as f:
                f.write("This is test")
            with open(creds_dir, "w") as f:
                f.write("This is test")
            _check_google_config()


def test_check_google_config_2():
    """Folder and files not created"""
    config_dir = "/some/random/path/3278032478325870/347280?"
    with raises(FileNotFoundError):
        with mock.patch("owid.datautils.google.config.CONFIG_DIR", config_dir):
            _check_google_config()


def test_check_google_config_3():
    """Folder created, files not created"""
    with raises(FileNotFoundError), tempfile.TemporaryDirectory() as config_dir:
        with mock.patch("owid.datautils.google.config.CONFIG_DIR", config_dir), mock.patch(
            "owid.datautils.google.config.CLIENT_SECRETS_PATH",
            Path(config_dir) / "google_client_secrets.json",
        ) as _, mock.patch(
            "owid.datautils.google.config.SETTINGS_PATH",
            Path(config_dir) / "google_settings.yaml",
        ) as _, mock.patch(
            "owid.datautils.google.config.CREDENTIALS_PATH",
            Path(config_dir) / "google_credentials.json",
        ) as _:
            _check_google_config()


def test_google_config_init_error():
    with raises(ValueError), tempfile.TemporaryDirectory() as config_dir:
        client_secrets_file = Path(config_dir) / "google_client_secrets.json"
        google_config_init(client_secrets_file)


@mock.patch.object(pydrive2.auth.GoogleAuth, "__init__", return_value=None)
@mock.patch.object(pydrive2.auth.GoogleAuth, "CommandLineAuth", return_value=None)
def test_google_config_init_1(mocker_google_1, mocker_google_2):
    # with tempfile.TemporaryDirectory() as config_dir:
    config_dir = next(tempfile._get_candidate_names())  # type: ignore
    defult_tmp_dir = tempfile._get_default_tempdir()  # type: ignore
    config_dir = os.path.join(defult_tmp_dir, config_dir)
    with mock.patch("owid.datautils.google.config.CONFIG_DIR", config_dir), mock.patch(
        "owid.datautils.google.config.CLIENT_SECRETS_PATH",
        Path(config_dir) / "google_client_secrets.json",
    ) as _, mock.patch(
        "owid.datautils.google.config.SETTINGS_PATH",
        Path(config_dir) / "google_settings.yaml",
    ) as _, mock.patch(
        "owid.datautils.google.config.CREDENTIALS_PATH",
        Path(config_dir) / "google_credentials.json",
    ) as _:
        with tempfile.NamedTemporaryFile() as secrets_dir_og:
            google_config_init(str(secrets_dir_og.name))
