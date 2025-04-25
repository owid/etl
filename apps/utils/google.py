import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from etl.config import GOOGLE_APPLICATION_CREDENTIALS

########################################################################################################################
# TODO: Handling gdocs will not work for other users, because they won't have the client_secret.json file.
#  We need to find a common way to handle credentials. It could be similar to the way GBQ handles them, but with credentials adapted to the new scopes.
# Path to OAuth client credentials.
CLIENT_SECRET_FILE = Path.home() / ".config" / "owid_etl_doc" / "client_secret.json"

# Path where to cache the token after the first login.
TOKEN_PATH = Path.home() / ".config" / "owid_etl_doc" / "token.pickle"
########################################################################################################################


def read_gbq(*args, **kwargs) -> pd.DataFrame:
    if GOOGLE_APPLICATION_CREDENTIALS:
        # Use service account
        credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
        return pandas_gbq.read_gbq(*args, **kwargs, credentials=credentials)  # type: ignore
    else:
        # Use browser authentication.
        return pandas_gbq.read_gbq(*args, **kwargs)  # type: ignore


class GoogleDocHandler:
    # Define relevant scopes.
    SCOPES = [
        # Create and edit docs.
        "https://www.googleapis.com/auth/documents",
        # Only access files created by the app.
        "https://www.googleapis.com/auth/drive.file",
    ]

    def __init__(self):
        # Authenticate and get credentials.
        credentials = self.authenticate()
        # Initialize docs and drive services.
        self.docs_service = build("docs", "v1", credentials=credentials)
        self.drive_service = build("drive", "v3", credentials=credentials)

    def authenticate(self):
        # Load the cached OAuth token if it exists.
        if TOKEN_PATH.exists():
            with TOKEN_PATH.open("rb") as token_file:
                credentials = pickle.load(token_file)
        else:
            # Create an OAuth flow using the client secrets file and the required scopes.
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, self.SCOPES)

            # Start a local server to handle the OAuth callback after user logs in.
            credentials = flow.run_local_server(port=0)

            # Ensure the directory for storing the token exists.
            TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)

            # Save the OAuth credentials to disk so the user is not prompted again.
            with TOKEN_PATH.open("wb") as token_file:
                pickle.dump(credentials, token_file)

        # Return the authenticated credentials object.
        return credentials

    def create(self, body: Optional[Dict[str, str]] = None) -> str:
        # Fallback to empty dict if no body is provided.
        if body is None:
            # This will create a document titled "Untitled document".
            # To use a different title, use: body={"title": "Some title"}
            body = {}

        # Create a gdoc in the root folder of the drive.
        # NOTE: It is not a problem if multiple files have the same title (they are identified by ID).
        doc = self.docs_service.documents().create(body=body).execute()

        # Extract the unique document ID.
        doc_id = doc["documentId"]

        return doc_id

    def move(self, doc_id: str, folder_id: str) -> None:
        # Move the file to the appropriate folder.
        file = self.drive_service.files().get(fileId=doc_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))
        self.drive_service.files().update(
            fileId=doc_id, addParents=folder_id, removeParents=prev_parents, fields="id, parents"
        ).execute()

    def copy(self, doc_id: str, body: Optional[Dict[str, str]] = None) -> str:
        # If no body is provided, fetch the original title and generate a default name.
        if body is None:
            original = self.drive_service.files().get(fileId=doc_id, fields="name").execute()
            old_title = original.get("name", "Untitled document")
            body = {"name": f"Copy of {old_title}"}

        copied_file = self.drive_service.files().copy(fileId=doc_id, body=body).execute()
        return copied_file["id"]

    def edit(self, doc_id: str, requests: List[Dict[str, Any]]) -> None:
        self.docs_service.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()

    def list_files_in_folder(self, folder_id: str) -> list[dict]:
        query = f"'{folder_id}' in parents and trashed = false"
        files = []
        page_token = None

        while True:
            response = (
                self.drive_service.files()
                .list(q=query, spaces="drive", fields="nextPageToken, files(id, name, mimeType)", pageToken=page_token)
                .execute()
            )

            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return files

    @staticmethod
    def url(doc_id: str) -> str:
        return f"https://docs.google.com/document/d/{doc_id}/edit"

    def replace_text(self, doc_id: str, mapping: Dict[str, str]) -> None:
        edits = []
        for placeholder, replacement in mapping.items():
            edits.append(
                {
                    "replaceAllText": {
                        "containsText": {"text": placeholder, "matchCase": True},
                        "replaceText": replacement,
                    }
                }
            )
        self.edit(doc_id=doc_id, requests=edits)

    def find_marker_index(self, doc_id, marker) -> int:
        doc = self.docs_service.documents().get(documentId=doc_id).execute()
        for element in doc.get("body", {}).get("content", []):
            if "paragraph" in element:
                for run in element["paragraph"].get("elements", []):
                    text_run = run.get("textRun", {})
                    if marker in text_run.get("content", ""):
                        return run["startIndex"]
        raise ValueError(f"Marker '{marker}' not found in document.")
