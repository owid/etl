"""Google configuration functions."""
import os
from pathlib import Path
from shutil import copyfile
from typing import Union

import yaml
from pydrive2.auth import GoogleAuth

# PATHS
CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "owid")
CLIENT_SECRETS_PATH = os.path.join(CONFIG_DIR, "google_client_secrets.json")
SETTINGS_PATH = os.path.join(CONFIG_DIR, "google_settings.yaml")
CREDENTIALS_PATH = os.path.join(CONFIG_DIR, "google_credentials.json")


def google_config_init(client_secrets_file: Union[str, Path], encoding: str = "utf8") -> GoogleAuth:
    """Initialise Google configuration files.

    Uses `clients_secrets` to generate all the required Google configuration files: `SETTINGS_PATH`, `CREDENTIALS_PATH`.
    Also, it creates a copy of `clients_secrets` in `CLIENT_SECRETS_PATH`, so the original file can be safely deleted
    after running this method.

    This should only run once before reading a file from Google Drive the first time. Subsequent executions should run
    seamlessly.

    To obtain `client_secrets_file`, follow the instructions from:
    https://medium.com/analytics-vidhya/how-to-connect-google-drive-to-python-using-pydrive-9681b2a14f20

    IMPORTANT:
        - Additionally, make sure to add yourself in Test users, as noted in:
          https://stackoverflow.com/questions/65980758/pydrive-quickstart-and-error-403-access-denied
        - Select Desktop App instead of Web Application as the application type.

    Method partly sourced from:
    https://github.com/lucasrodes/whatstk/blob/bcb9cf7c256df1c9e270aab810b74ab0f7329436/whatstk/utils/gdrive.py#L38

    Parameters
    ----------
    client_secrets_file : str
        Path to client_secrets file.
    encoding : str, optional
        Encoding of the text in `client_secrets` file, by default "utf8"

    Returns
    -------
    GoogleAuth
        Google authenticator object.
    """
    # Check client_secrets
    if not os.path.isfile(client_secrets_file):
        raise ValueError(f"Credentials not found at {client_secrets_file}. Please provide a valid" " path!")
    # Check or create config directory
    if not os.path.isdir(CONFIG_DIR):
        os.makedirs(CONFIG_DIR, exist_ok=True)

    # Copy credentials to config folder
    copyfile(client_secrets_file, CLIENT_SECRETS_PATH)

    # Create settings.yaml file
    dix = {
        "client_config_backend": "file",
        "client_config_file": CLIENT_SECRETS_PATH,
        "save_credentials": True,
        "save_credentials_backend": "file",
        "save_credentials_file": CREDENTIALS_PATH,
        "get_refresh_token": True,
        "oauth_scope": [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.install",
        ],
    }
    with open(SETTINGS_PATH, "w", encoding=encoding) as f:
        yaml.dump(dix, f)

    # credentials.json
    gauth = GoogleAuth(settings_file=SETTINGS_PATH)
    gauth.CommandLineAuth()
    return gauth


def _check_google_config() -> None:
    """Run checks agains Google configuration files.

    Raises
    ------
    FileNotFoundError
        If owid.datautils.google.config.CONFIG_DIR directory does not exist.
    FileNotFoundError
        If any of the Google configuration files is missing. These include:
            - owid.datautils.google.config.CLIENT_SECRETS_PATH
            - owid.datautils.google.config.SETTINGS_PATH
            - owid.datautils.google.config.CREDENTIALS_PATH
    """
    if not os.path.isdir(CONFIG_DIR):
        raise FileNotFoundError(
            f"{CONFIG_DIR} folder is not created. Please check you have run" " `google_config_init`!"
        )
    for f in [CLIENT_SECRETS_PATH, CREDENTIALS_PATH, SETTINGS_PATH]:
        if not os.path.isfile(f):
            raise FileNotFoundError(f"{f} file was not found. Please check you have run" " `google_config_init`!")


def is_google_config_init() -> bool:
    """Check if Google configuration files were created.

    Returns
    -------
    bool
        True if Google configuration files were created, False otherwise.
    """
    try:
        _check_google_config()
    except FileNotFoundError:
        return False
    return True
