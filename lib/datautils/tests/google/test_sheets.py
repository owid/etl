from unittest import mock

import pandas as pd

from owid.datautils.google.sheets import GSheetsApi


class MockSheets:
    def __init__(self, *args, **kwargs):
        self.name = "hello"

    @classmethod
    def from_files(cls, *args, **kwargs):
        return cls()

    def get(self, *args, **kwargs):
        return MockSpreadSheet()  # type: ignore


class MockSpreadSheet:
    def __init__(self, *args, **kwargs):
        self.name = "spreadsheet"

    def get(self, *args, **kwargs):
        return MockWorkSheet()  # type: ignore

    def to_csv(self, *args, **kwargs):
        return None


class MockWorkSheet:
    def __init__(self, *args, **kwargs):
        self.name = "worksheet"

    def to_csv(self, *args, **kwargs):
        return None

    def to_frame(self):
        return pd.DataFrame()


@mock.patch.object(GSheetsApi, "_init_config_folder", return_value=None)
class TestGSheetsApi:
    clients_secrets = "a"
    credentials_path = "b"
    ss_id = "ss_id"
    ws_id = 1

    def test_init(self, mock_init):
        api = GSheetsApi(self.clients_secrets, self.credentials_path)
        assert api.clients_secrets == self.clients_secrets
        assert api.credentials_path == self.credentials_path

    @mock.patch("gsheets.Sheets.from_files", side_effect=MockSheets.from_files)
    def test_sheets(self, mock_init, mock_sheets_from_files):
        api = GSheetsApi(self.clients_secrets, self.credentials_path)
        _ = api.sheets
        assert api.sheets.name == "hello"

    @mock.patch("gsheets.Sheets.from_files", side_effect=MockSheets.from_files)
    @mock.patch("gsheets.Sheets.get", side_effect=MockSheets.get)
    def test_get(self, mock_init, mock_sheets_1, mock_sheets_2):
        api = GSheetsApi(self.clients_secrets, self.credentials_path)
        # Only spreadsheet
        ss = api.get(spreadsheet_id=self.ss_id)
        assert isinstance(ss, MockSpreadSheet)
        # Spreadsheet and worksheet
        ws = api.get(spreadsheet_id=self.ss_id, worksheet_id=self.ws_id)
        assert isinstance(ws, MockWorkSheet)

    @mock.patch("owid.datautils.google.sheets.GSheetsApi.get", return_value=MockWorkSheet())  # type: ignore
    def test_download_worksheet(self, mock_init, mock_sheets):
        api = GSheetsApi(self.clients_secrets, self.credentials_path)
        # with output_path
        api.download_worksheet(self.ss_id, self.ws_id, output_path="local-path")
        # without output_path
        api.download_worksheet(self.ss_id, self.ws_id)

    @mock.patch("owid.datautils.google.sheets.GSheetsApi.get", return_value=MockWorkSheet())  # type: ignore
    def test_download_spreadsheet(self, mock_init, mock_sheets_1):
        api = GSheetsApi(self.clients_secrets, self.credentials_path)
        # with output_dir
        api.download_spreadsheet(self.ss_id, output_dir="local-path")

    @mock.patch("owid.datautils.google.sheets.GSheetsApi.get", return_value=MockWorkSheet())  # type: ignore
    def test_worksheet_to_df(self, mock_init, mock_sheets):
        api = GSheetsApi(self.clients_secrets, self.credentials_path)
        df = api.worksheet_to_df(self.ss_id, self.ws_id)
        assert isinstance(df, pd.DataFrame)
