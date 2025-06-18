import pickle
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from structlog import get_logger

from etl.config import GOOGLE_APPLICATION_CREDENTIALS

# Initialize logger.
log = get_logger()

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


class GoogleDrive:
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

    def authenticate(self) -> Any:
        """
        Authenticate the user and return the credentials object.

        If the token file exists, load the credentials from it.
        If not, create a new OAuth flow and save the credentials to the token file.

        Returns
        -------
        Any
            The authenticated credentials object.

        """
        # Check if the token file exists and load the credentials from it.
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

    def create_doc(self, body: Optional[Dict[str, str]] = None) -> str:
        """
        Create a new Google Doc and return its ID.

        If no body is provided, a document with the title "Untitled document" will be created.

        Parameters
        ----------
        body : dict, optional
            Body of the new document. If None, a document with the title "Untitled document" will be created.
            Default is None.

        Returns
        -------
        str
            ID of the created document.

        """
        # Create a new document with the provided body.
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

    def move(self, file_id: str, folder_id: str) -> None:
        """
        Move a file to a specified folder.

        Parameters
        ----------
        file_id : str
            ID of the file to be moved.
        folder_id : str
            ID of the folder to move the file to.

        """
        # Move the file to the appropriate folder.
        file = self.drive_service.files().get(fileId=file_id, fields="parents").execute()
        prev_parents = ",".join(file.get("parents", []))
        self.drive_service.files().update(
            fileId=file_id, addParents=folder_id, removeParents=prev_parents, fields="id, parents"
        ).execute()

    def copy(self, file_id: str, body: Optional[Dict[str, str]] = None) -> str:
        """
        Copy a file and return the new file ID.

        If no body is provided, the original title will be used with "Copy of" prepended.

        Parameters
        ----------
        file_id : str
            ID of the file to be copied.
        body : dict, optional
            Body of the new file. If None, the original title will be used with "Copy of" prepended.
            Default is None.

        Returns
        -------
        str
            ID of the copied file.

        """
        # If no body is provided, fetch the original title and generate a default name.
        if body is None:
            original = self.drive_service.files().get(fileId=file_id, fields="name").execute()
            old_title = original.get("name", "Untitled document")
            body = {"name": f"Copy of {old_title}"}

        copied_file = self.drive_service.files().copy(fileId=file_id, body=body).execute()
        return copied_file["id"]

    def list_files_in_folder(self, folder_id: str) -> list[dict]:
        """
        List all files in a specified folder.

        Parameters
        ----------
        folder_id : str
            The ID of the folder to list files from.

        Returns
        -------
        list[dict]
            A list of dictionaries containing file IDs, names, and MIME types.

        """
        # Query to list files in the specified folder.
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


class GoogleDoc:
    def __init__(self, doc_id: str):
        self.doc_id = doc_id
        self.url = f"https://docs.google.com/document/d/{self.doc_id}/edit"
        self.drive = GoogleDrive()

    def move(self, folder_id: str) -> None:
        """
        Move document to a specified folder.

        Parameters
        ----------
        folder_id : str
            The ID of the folder to move the document to.

        """
        self.drive.move(folder_id=folder_id, file_id=self.doc_id)

    def copy(self, body: Optional[Dict[str, str]] = None) -> str:
        """
        Copy the document and return the new document ID.

        If no body is provided, the original title will be used with "Copy of" prepended.

        Parameters
        ----------
        body : dict, optional
            The body of the new document. If None, the original title will be used with "Copy of" prepended.
            Default is None.

        Returns
        -------
        str
            The ID of the copied document.

        """
        return self.drive.copy(file_id=self.doc_id, body=body)

    def edit(self, requests: List[Dict[str, Any]]) -> None:
        """
        Edit the document using the provided requests.

        Parameters
        ----------
        requests : list[dict]
            A list of dictionaries representing the requests to be made to the Google Docs API. The requests can include operations like inserting text, replacing text, and more.

        """
        self.drive.docs_service.documents().batchUpdate(documentId=self.doc_id, body={"requests": requests}).execute()

    def replace_text(self, mapping: Dict[str, str]) -> None:
        """
        Replace all occurrences of specified placeholders in the document with the corresponding replacements.

        Parameters
        ----------
        mapping : dict
            A dictionary where the keys are the placeholders to be replaced and the values are the replacements.

        """
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
        self.edit(requests=edits)

    def find_marker_index(self, marker) -> int:
        """
        Find the index of a marker in the document.

        The marker is a string that should be present in the document. If the marker is not found, a ValueError is raised.

        Parameters
        ----------
        marker : str
            Marker string to search for in the document.

        Returns
        -------
        int
            Index of the marker in the document.

        """
        # Fetch the document content.
        doc = self.drive.docs_service.documents().get(documentId=self.doc_id).execute()
        for element in doc.get("body", {}).get("content", []):
            if "paragraph" in element:
                for run in element["paragraph"].get("elements", []):
                    text_run = run.get("textRun", {})
                    if marker in text_run.get("content", ""):
                        return run["startIndex"]
        raise ValueError(f"Marker '{marker}' not found in document.")

    def insert_image(self, image_url, placeholder, width=350) -> None:
        """
        Insert an image into the document at the position of a placeholder text.

        The image will be centered and the placeholder text will be removed.

        Parameters
        ----------
        image_url : str
            URL of the image to be inserted.
        placeholder : str
            Placeholder text in the document where the image will be inserted.
        width : int, optional
            Width of the image in points.

        """
        # Get the index of the position where the image should be inserted.
        insert_index = self.find_marker_index(marker=placeholder)

        edits = [
            {
                "insertInlineImage": {
                    "location": {"index": insert_index},
                    "uri": image_url,
                    "objectSize": {
                        # "height": {
                        #     "magnitude": 200,
                        #     "unit": "PT"
                        # },
                        "width": {"magnitude": width, "unit": "PT"}
                    },
                }
            },
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": insert_index, "endIndex": insert_index + 1},
                    "paragraphStyle": {"alignment": "CENTER"},
                    "fields": "alignment",
                }
            },
        ]

        # Apply both image insert and alignment.
        self.edit(requests=edits)

        # Remove the original placeholder text.
        self.replace_text(mapping={placeholder: ""})

    def save_as_pdf(
        self, pdf_name: Optional[str] = None, folder_id: Optional[str] = None, overwrite: bool = False
    ) -> str:
        """
        Export this Google Doc as PDF and save it to Google Drive.

        Parameters
        ----------
        pdf_name : str, optional
            Name for the PDF file (without extension). If None, the Google Doc's name will be used.
        folder_id : str, optional
            Google Drive folder ID where the PDF should be saved. If None, the PDF will be saved in the same folder as the original Google Doc.
        overwrite : bool, optional
            If True, overwrite existing PDF with the same name. If False, raise an error if a file with the same name exists.

        Returns
        -------
        pdf_id : str
            The ID of the created PDF file in Google Drive.

        """
        try:
            # Get document info (name and parents)
            doc_info = self.drive.drive_service.files().get(fileId=self.doc_id, fields="name,parents").execute()

            # If no pdf_name provided, use the document's name
            if pdf_name is None:
                pdf_name = doc_info.get("name", "Untitled document")

            # If no folder_id provided, use the document's current folder(s)
            if folder_id is None:
                parents = doc_info.get("parents", [])
            else:
                parents = [folder_id]

            pdf_filename = f"{pdf_name}.pdf"

            # Check for existing PDF with the same name in the target folder(s)
            existing_pdf_id = None
            for parent in parents:
                # Search for files with the same name in this folder
                query = (
                    f"name='{pdf_filename}' and '{parent}' in parents and trashed=false and mimeType='application/pdf'"
                )
                response = self.drive.drive_service.files().list(q=query, fields="files(id, name)").execute()

                files = response.get("files", [])
                if files:
                    existing_pdf_id = files[0]["id"]
                    break

            # Handle existing file based on overwrite setting
            if existing_pdf_id:
                if not overwrite:
                    raise FileExistsError(
                        f"PDF file '{pdf_filename}' already exists in the target folder. Set overwrite=True to replace it."
                    )
                else:
                    log.info(f"Found existing PDF '{pdf_filename}', will overwrite it")

            # Export the document as PDF
            request = self.drive.drive_service.files().export_media(
                fileId=self.doc_id,
                mimeType="application/pdf",
            )

            # Get the PDF content
            pdf_content = request.execute()

            # Upload the PDF to Google Drive
            media = MediaIoBaseUpload(
                BytesIO(pdf_content),
                mimetype="application/pdf",
                resumable=True,
            )

            if existing_pdf_id and overwrite:
                # Update existing file
                updated_file = (
                    self.drive.drive_service.files()
                    .update(fileId=existing_pdf_id, media_body=media, fields="id")
                    .execute()
                )
                pdf_id = updated_file.get("id")
                log.info(f"PDF updated in Google Drive with ID: {pdf_id}")
            else:
                # Create new file
                pdf_metadata = {
                    "name": pdf_filename,
                    "parents": parents,
                    "mimeType": "application/pdf",
                }

                pdf_file = (
                    self.drive.drive_service.files()
                    .create(
                        body=pdf_metadata,
                        media_body=media,
                        fields="id",
                    )
                    .execute()
                )
                pdf_id = pdf_file.get("id")
                log.info(f"PDF created in Google Drive with ID: {pdf_id}")

            return pdf_id

        except Exception as e:
            log.error(f"Failed to create PDF in Drive: {e}")
            raise
