"""Google Sheet utils."""
import os
from typing import Any, Optional, Union

import pandas as pd
from gsheets import Sheets
from gsheets.models import SpreadSheet, WorkSheet

from owid.datautils.google.config import CLIENT_SECRETS_PATH, CREDENTIALS_PATH


class GSheetsApi:
    """Interface to interact with Google sheets."""

    def __init__(
        self,
        clients_secrets: str = CLIENT_SECRETS_PATH,
        credentials_path: str = CREDENTIALS_PATH,
    ) -> None:
        self.clients_secrets = clients_secrets
        self.credentials_path = credentials_path
        self._init_config_folder()
        self.__sheets = None

    @property
    def sheets(self) -> Sheets:
        """Access or initialize sheets attribute."""
        if self.__sheets is None:
            self.__sheets = Sheets.from_files(self.clients_secrets, self.credentials_path, no_webserver=True)
        return self.__sheets

    def _init_config_folder(self) -> None:
        credentials_folder = os.path.expanduser(os.path.dirname(self.credentials_path))
        if not os.path.isdir(credentials_folder):
            os.makedirs(credentials_folder, exist_ok=True)

    def get(self, spreadsheet_id: str, worksheet_id: Optional[int] = None) -> Union[SpreadSheet, WorkSheet]:
        """Get a spreadsheet or worksheet from a Google sheet.

        If only `spreadsheet_id` is provided, this will return the entire spreadsheet. Otherwise,
        the specific worksheet will be returned.

        Parameters
        ----------
        spreadsheet_id : str
            ID of the spreadsheet.
        worksheet_id : int
            ID of the worksheet.

        Returns
        -------
        Spreadsheet or WorkSheet
            SpreadSheet or Worksheet.
        """
        ssheet = self.sheets.get(spreadsheet_id)
        if worksheet_id:
            return ssheet.get(worksheet_id)
        return ssheet

    def download_worksheet(
        self,
        spreadsheet_id: str,
        worksheet_id: int,
        output_path: Optional[str] = None,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> None:
        """Download a worksheet from a Google sheet.

        Saves the downloaded worksheet as a CSV.

        Parameters
        ----------
        spreadsheet_id : str
            ID of the spreadsheet. This is included in the URL of the spreadsheet.
        worksheet_id : int
            ID of the worksheet. This is included in the URL of the spreadsheet. Look for
            "edit#gid=<worksheet_id>" section.
        output_path : str, optional
            Local path where to save the downloaded worksheet. By default it will save it in
            the execution directory under the name that was given to the worksheet on Google Drive.
        encoding : str, optional
            Encoding of the file, by default "utf-8"
        """
        sheet = self.get(spreadsheet_id, worksheet_id)
        if output_path:
            sheet.to_csv(output_path, encoding=encoding, **kwargs)
        else:
            make_filename = "%(title)s.csv"
            sheet.to_csv(make_filename=make_filename, encoding=encoding, **kwargs)

    def download_spreadsheet(
        self, spreadsheet_id: str, output_dir: str, encoding: str = "utf-8", **kwargs: Any
    ) -> None:
        """Download a spreadsheet from a Google sheet.

        It downloads all the worksheets from the given spreadsheets and saves them as CSVs.
        The filenames given to the worksheets are as follows:

        `<output_dir>/<spreadsheet_title> - <worksheet_title>.csv`

        Parameters
        ----------
        spreadsheet_id : str
            ID of the spreadsheet. This is included in the URL of the spreadsheet.
        output_dir : str, optional
            Local directory where to store all the downloaded worksheets.
        encoding : str, optional
            Encoding of the file, by default "utf-8"
        """
        sheet = self.get(spreadsheet_id)
        make_filename = os.path.join(output_dir, "%(title)s - %(sheet)s.csv")
        sheet.to_csv(make_filename=make_filename, encoding=encoding, **kwargs)

    def worksheet_to_df(self, spreadsheet_id: str, worksheet_id: int) -> pd.DataFrame:
        """Load a Worksheet as a dataframe.

        Parameters
        ----------
        spreadsheet_id : str
            ID of the spreadsheet. This is included in the URL of the spreadsheet.
        worksheet_id : int
            ID of the worksheet. This is included in the URL of the spreadsheet. Look for
            "edit#gid=<worksheet_id>" section.

        Returns
        -------
        pd.DataFrame:
            Dataframe with the data from the worksheet.
        """
        ws = self.get(spreadsheet_id, worksheet_id)
        df: pd.DataFrame = ws.to_frame()
        return df
