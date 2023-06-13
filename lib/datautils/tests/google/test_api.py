from unittest import mock

from pytest import raises

from owid.datautils.google.api import GoogleApi


class TestGoogleApi:
    @mock.patch("gdown.download_folder", return_value=None)
    def test_download_file(self, mock_gdown):
        GoogleApi.download_folder(url="some-random-url", output="local-folder")

    @mock.patch("gdown.download", return_value=None)
    def test_download_file_1(self, mock_gdown):
        GoogleApi.download_file(url="some-random-url", output="local-folder")

    @mock.patch("gdown.download", return_value=None)
    def test_download_file_2(self, mock_gdown):
        GoogleApi.download_file(file_id="some-random-id", output="local-folder")

    @mock.patch("gdown.download", return_value=None)
    def test_download_file_3(self, mock_gdown):
        with raises(ValueError):
            GoogleApi.download_file(output="local-folder")

    # @mock.patch.object(pydrive2.auth.GoogleAuth, "__init__", return_value=None)
    # @mock.patch.object(pydrive2.auth.GoogleDrive, "__init__", return_value=None)
    # def test_drive(self, mock_1, mock_2):
    #     pass
