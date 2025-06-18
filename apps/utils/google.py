"""
Google utilities.

"""

import pickle
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
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
        # Read and write Google Sheets.
        "https://www.googleapis.com/auth/spreadsheets",
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
                    log.warning(
                        f"PDF file '{pdf_filename}' already exists in the target folder. Set overwrite=True to replace it."
                    )
                    return existing_pdf_id
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


class GoogleSheet:
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self.url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/edit"
        self.drive = GoogleDrive()
        # Initialize sheets service
        credentials = self.drive.authenticate()
        self.sheets_service = build("sheets", "v4", credentials=credentials)

    def move(self, folder_id: str) -> None:
        """
        Move spreadsheet to a specified folder.

        Parameters
        ----------
        folder_id : str
            The ID of the folder to move the spreadsheet to.

        """
        self.drive.move(folder_id=folder_id, file_id=self.sheet_id)

    def copy(self, body: Optional[Dict[str, str]] = None) -> str:
        """
        Copy the spreadsheet and return the new spreadsheet ID.

        If no body is provided, the original title will be used with "Copy of" prepended.

        Parameters
        ----------
        body : dict, optional
            The body of the new spreadsheet. If None, the original title will be used with "Copy of" prepended.
            Default is None.

        Returns
        -------
        str
            The ID of the copied spreadsheet.

        """
        return self.drive.copy(file_id=self.sheet_id, body=body)

    def get_values(self, range_name: str) -> List[List[str]]:
        """
        Get values from a specified range in the spreadsheet.

        Parameters
        ----------
        range_name : str
            The A1 notation range to retrieve (e.g., 'Sheet1!A1:C10').

        Returns
        -------
        List[List[str]]
            A 2D list of values from the specified range.

        """
        result = (
            self.sheets_service.spreadsheets().values().get(spreadsheetId=self.sheet_id, range=range_name).execute()
        )
        return result.get("values", [])

    def update_values(self, range_name: str, values: List[List[Any]], value_input_option: str = "USER_ENTERED") -> None:
        """
        Update values in a specified range of the spreadsheet.

        Parameters
        ----------
        range_name : str
            The A1 notation range to update (e.g., 'Sheet1!A1:C10').
        values : List[List[Any]]
            A 2D list of values to write to the range.
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        body = {"values": values}
        self.sheets_service.spreadsheets().values().update(
            spreadsheetId=self.sheet_id, range=range_name, valueInputOption=value_input_option, body=body
        ).execute()

    def append_values(self, range_name: str, values: List[List[Any]], value_input_option: str = "USER_ENTERED") -> None:
        """
        Append values to the end of a specified range in the spreadsheet.

        Parameters
        ----------
        range_name : str
            The A1 notation range to append to (e.g., 'Sheet1!A:C').
        values : List[List[Any]]
            A 2D list of values to append.
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        body = {"values": values}
        self.sheets_service.spreadsheets().values().append(
            spreadsheetId=self.sheet_id, range=range_name, valueInputOption=value_input_option, body=body
        ).execute()

    def clear_values(self, range_name: str) -> None:
        """
        Clear values in a specified range of the spreadsheet.

        Parameters
        ----------
        range_name : str
            The A1 notation range to clear (e.g., 'Sheet1!A1:C10').

        """
        self.sheets_service.spreadsheets().values().clear(spreadsheetId=self.sheet_id, range=range_name).execute()

    def batch_update_values(self, value_ranges: List[Dict[str, Any]], value_input_option: str = "USER_ENTERED") -> None:
        """
        Update multiple ranges in the spreadsheet in a single request.

        Parameters
        ----------
        value_ranges : List[Dict[str, Any]]
            A list of dictionaries, each containing 'range' and 'values' keys.
            Example: [{'range': 'Sheet1!A1:B2', 'values': [['A1', 'B1'], ['A2', 'B2']]}]
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        body = {"valueInputOption": value_input_option, "data": value_ranges}
        self.sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()

    def add_sheet(self, title: str, rows: int = 1000, cols: int = 26) -> int:
        """
        Add a new sheet to the spreadsheet.

        Parameters
        ----------
        title : str
            The title of the new sheet.
        rows : int, optional
            The number of rows in the new sheet. Default is 1000.
        cols : int, optional
            The number of columns in the new sheet. Default is 26.

        Returns
        -------
        int
            The ID of the newly created sheet.

        """
        body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {"title": title, "gridProperties": {"rowCount": rows, "columnCount": cols}}
                    }
                }
            ]
        }
        response = self.sheets_service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()
        return response["replies"][0]["addSheet"]["properties"]["sheetId"]

    def delete_sheet(self, sheet_id: int) -> None:
        """
        Delete a sheet from the spreadsheet.

        Parameters
        ----------
        sheet_id : int
            The ID of the sheet to delete (not the spreadsheet ID).

        """
        body = {"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
        self.sheets_service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()

    def get_sheet_properties(self) -> List[Dict[str, Any]]:
        """
        Get properties of all sheets in the spreadsheet.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries containing sheet properties.

        """
        spreadsheet = self.sheets_service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
        return [sheet["properties"] for sheet in spreadsheet.get("sheets", [])]

    def get_sheet_data(self, sheet_name: str = "Sheet1") -> List[List[str]]:
        """
        Get all data from a worksheet by name.

        Parameters
        ----------
        sheet_name : str, optional
            The name of the worksheet to read from. Default is 'Sheet1'.

        Returns
        -------
        List[List[str]]
            A 2D list of all values from the specified worksheet.

        """
        # Get all data from the sheet (will automatically find the used range)
        range_name = f"{sheet_name}!A:ZZ"
        return self.get_values(range_name)

    def write_sheet_data(
        self, data: List[List[Any]], sheet_name: str = "Sheet1", value_input_option: str = "USER_ENTERED"
    ) -> None:
        """
        Write data to a worksheet, starting from A1.

        Parameters
        ----------
        data : List[List[Any]]
            A 2D list of values to write to the worksheet.
        sheet_name : str, optional
            The name of the worksheet to write to. Default is 'Sheet1'.
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        # Determine the range based on data dimensions
        num_rows = len(data)
        num_cols = max(len(row) for row in data) if data else 0

        if num_rows == 0 or num_cols == 0:
            return

        # Convert column number to letter (A, B, C, ..., Z, AA, AB, ...)
        end_col = self._number_to_column_letter(num_cols)
        range_name = f"{sheet_name}!A1:{end_col}{num_rows}"

        self.update_values(range_name, data, value_input_option)

    def append_sheet_data(
        self, data: List[List[Any]], sheet_name: str = "Sheet1", value_input_option: str = "USER_ENTERED"
    ) -> None:
        """
        Append data to the end of a worksheet.

        Parameters
        ----------
        data : List[List[Any]]
            A 2D list of values to append to the worksheet.
        sheet_name : str, optional
            The name of the worksheet to append to. Default is 'Sheet1'.
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        # Use the entire column range to let Sheets API find the next empty row
        range_name = f"{sheet_name}!A:ZZ"
        self.append_values(range_name, data, value_input_option)

    def clear_sheet_data(self, sheet_name: str = "Sheet1") -> None:
        """
        Clear all data from a worksheet.

        Parameters
        ----------
        sheet_name : str, optional
            The name of the worksheet to clear. Default is 'Sheet1'.

        """
        # Clear a large range to ensure we get everything
        range_name = f"{sheet_name}!A:ZZ"
        self.clear_values(range_name)

    def read_dataframe(self, sheet_name: str = "Sheet1", header_row: int = 0) -> pd.DataFrame:
        """
        Read all data from a worksheet into a pandas DataFrame.

        Parameters
        ----------
        sheet_name : str, optional
            The name of the worksheet to read from. Default is 'Sheet1'.
        header_row : int, optional
            The row index to use as column headers. Default is 0 (first row).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing all data from the specified worksheet.

        """
        values = self.get_sheet_data(sheet_name)

        if not values:
            return pd.DataFrame()

        if header_row is not None and len(values) > header_row:
            headers = values[header_row]
            data = values[header_row + 1 :]
            df = pd.DataFrame(data, columns=headers)
        else:
            df = pd.DataFrame(values)

        return df

    def write_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        include_index: bool = False,
        include_header: bool = True,
        value_input_option: str = "USER_ENTERED",
    ) -> None:
        """
        Write a pandas DataFrame to a worksheet, replacing all existing data.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to write.
        sheet_name : str, optional
            The name of the worksheet to write to. Default is 'Sheet1'.
        include_index : bool, optional
            Whether to include the DataFrame index. Default is False.
        include_header : bool, optional
            Whether to include the DataFrame column headers. Default is True.
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        # First clear the sheet
        self.clear_sheet_data(sheet_name)

        # Convert DataFrame to list of lists
        values = []

        if include_header:
            headers = df.columns.tolist()
            if include_index:
                headers = [df.index.name or ""] + headers
            values.append(headers)

        for idx, row in df.iterrows():
            row_values = row.tolist()
            if include_index:
                row_values = [str(idx)] + row_values
            values.append(row_values)

        # Convert values to appropriate types for Google Sheets
        def convert_value(cell):
            if cell is None or pd.isna(cell):
                return ""
            elif isinstance(cell, (int, float)) and not isinstance(cell, bool):
                # Ensure we return native Python int/float, not numpy types
                if isinstance(cell, np.number):
                    return cell.item()  # Convert numpy types to native Python types
                return cell  # Keep numbers as numbers
            elif isinstance(cell, bool):
                return cell  # Keep booleans as booleans
            else:
                return str(cell)  # Convert everything else to strings

        values = [[convert_value(cell) for cell in row] for row in values]

        self.write_sheet_data(values, sheet_name, value_input_option)

    def append_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        include_index: bool = False,
        include_header: bool = False,
        value_input_option: str = "USER_ENTERED",
    ) -> None:
        """
        Append a pandas DataFrame to the end of a worksheet.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to append.
        sheet_name : str, optional
            The name of the worksheet to append to. Default is 'Sheet1'.
        include_index : bool, optional
            Whether to include the DataFrame index. Default is False.
        include_header : bool, optional
            Whether to include the DataFrame column headers. Default is False.
        value_input_option : str, optional
            How the input data should be interpreted. Options are 'RAW' or 'USER_ENTERED'.
            Default is 'RAW'.

        """
        # Convert DataFrame to list of lists
        values = []

        if include_header:
            headers = df.columns.tolist()
            if include_index:
                headers = [df.index.name or ""] + headers
            values.append(headers)

        for idx, row in df.iterrows():
            row_values = row.tolist()
            if include_index:
                row_values = [str(idx)] + row_values
            values.append(row_values)

        # Convert values to appropriate types for Google Sheets
        def convert_value(cell):
            if cell is None or pd.isna(cell):
                return ""
            elif isinstance(cell, (int, float)) and not isinstance(cell, bool):
                # Ensure we return native Python int/float, not numpy types
                if isinstance(cell, np.number):
                    return cell.item()  # Convert numpy types to native Python types
                return cell  # Keep numbers as numbers
            elif isinstance(cell, bool):
                return cell  # Keep booleans as booleans
            else:
                return str(cell)  # Convert everything else to strings

        values = [[convert_value(cell) for cell in row] for row in values]

        self.append_sheet_data(values, sheet_name, value_input_option)

    def _number_to_column_letter(self, num: int) -> str:
        """
        Convert a column number to its corresponding letter(s).

        Parameters
        ----------
        num : int
            Column number (1-based, where 1 = A, 2 = B, etc.)

        Returns
        -------
        str
            The column letter(s) (A, B, ..., Z, AA, AB, ...)
        """
        result = ""
        while num > 0:
            num -= 1
            result = chr(65 + (num % 26)) + result
            num //= 26
        return result

    def get_sheet_names(self) -> List[str]:
        """
        Get the names of all worksheets in the spreadsheet.

        Returns
        -------
        List[str]
            A list of worksheet names.

        """
        properties = self.get_sheet_properties()
        return [sheet["title"] for sheet in properties]

    @classmethod
    def create_sheet(cls, title: str, folder_id: Optional[str] = None) -> "GoogleSheet":
        """
        Create a new Google Sheet and return a GoogleSheet instance.

        Parameters
        ----------
        title : str
            The title of the new spreadsheet.
        folder_id : str, optional
            The ID of the folder to create the spreadsheet in. If None, it will be created in the root folder.

        Returns
        -------
        GoogleSheet
            A GoogleSheet instance for the newly created spreadsheet.

        """
        drive = GoogleDrive()

        # Use the Drive API to create the spreadsheet file
        file_metadata: Dict[str, Any] = {"name": title, "mimeType": "application/vnd.google-apps.spreadsheet"}

        if folder_id:
            file_metadata["parents"] = [folder_id]

        spreadsheet = drive.drive_service.files().create(body=file_metadata).execute()
        sheet_id = spreadsheet["id"]

        return cls(sheet_id)
