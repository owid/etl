"""Google API class."""
from typing import Any, Optional

import gdown
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import GoogleDriveFileList

from owid.datautils.google.config import (
    CLIENT_SECRETS_PATH,
    CREDENTIALS_PATH,
    SETTINGS_PATH,
    google_config_init,
    is_google_config_init,
)
from owid.datautils.google.sheets import GSheetsApi


class GoogleApi:
    """API for Google Drive."""

    def __init__(self, clients_secrets_file: Optional[str] = None) -> None:
        """Initialise Google API.

        To obtain `client_secrets_file`, follow the instructions from:
        https://medium.com/analytics-vidhya/how-to-connect-google-drive-to-python-using-pydrive-9681b2a14f20

        IMPORTANT:
            - Additionally, make sure to add yourself in Test users, as noted in:
              https://stackoverflow.com/questions/65980758/pydrive-quickstart-and-error-403-access-denied
            - Select Desktop App instead of Web Application as the application type.

        Parameters
        ----------
        clients_secrets_file : str, optional
            Path to client_secrets file.

        Examples
        --------
        First time calling the function should look similar to:

        >>> from owid.datautils.google.api import GoogleApi
        >>> api = GoogleApi("path/to/credentials.json")

        New calls can then be made as follows:

        >>> api = GoogleApi()
        """
        if not is_google_config_init():
            if not clients_secrets_file:
                # No clients_secrets, can't initialize Google configuration!
                raise ValueError("No value for `clients_secrets_file` was provided!")
            else:
                google_config_init(clients_secrets_file)

        self.sheets = GSheetsApi(CLIENT_SECRETS_PATH, CREDENTIALS_PATH)

    @classmethod
    def download_folder(cls, url: str, output: str, quiet: bool = True, **kwargs: Any) -> None:
        """Download a folder from Google Drive.

        The folderm must be public, otherwise this function won't work.

        Parameters
        ----------
        url : str
            URL to the folder on Google Drive (must be public).
        output : str
            Local path to save the downloaded folder.
        quiet: bool, optional
            Suppress terminal output. Default is False.
        """
        gdown.download_folder(url, output=output, quiet=quiet, use_cookies=False, **kwargs)

    @classmethod
    def download_file(
        cls,
        output: str,
        url: Optional[str] = None,
        file_id: Optional[str] = None,
        quiet: bool = True,
        **kwargs: Any,
    ) -> None:
        """Download a file from Google Drive.

        The file must be public, otherwise this function won't work.

        Parameters
        ----------
        output : str
            Local path to save the downloaded file.
        url : str, optional
            URL to the file on Google Drive (it must be public), by default None
        file_id : str, optional
            ID of the file on Google Drive (the file must be public), by default None.
        quiet: bool, optional
            Suppress terminal output. Default is False.

        Raises
        ------
        ValueError
            If neither `url` nor `id` are provided.
        """
        if url:
            gdown.download(url=url, output=output, fuzzy=True, quiet=quiet, **kwargs)
        elif file_id:
            gdown.download(id=file_id, output=output, quiet=quiet, **kwargs)
        else:
            raise ValueError("You must provide a `url` or `file_id`")

    @property
    def drive(self) -> GoogleDrive:
        """Google Drive object."""
        gauth = GoogleAuth(settings_file=SETTINGS_PATH)
        # gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)
        return drive

    def list_files(self, parent_id: str) -> GoogleDriveFileList:
        """List files in a Google Drive folder.

        Parameters
        ----------
        parent_id : str
            Google Drive folder ID.

        Returns
        -------
        List
            Files
        """
        request = f"'{parent_id}' in parents and trashed=false"
        # Get list of files
        files = self.drive.ListFile({"q": request}).GetList()
        return files
