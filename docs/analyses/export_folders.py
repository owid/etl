"""Helper functions for exporting media deaths analysis data to R2."""

import os
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import structlog
from owid.catalog.s3_utils import upload

log = structlog.get_logger()


def create_zip_from_folder(
    folder_path: str,
    zip_path: str,
    exclude_files: Optional[list[str]] = None,
    exclude_folders: Optional[list[str]] = None,
) -> None:
    """
    Create a zip file from a folder, with optional exclusions.

    Parameters
    ----------
    folder_path : str
        Path to the folder to zip.
    zip_path : str
        Path where the zip file should be created.
    exclude_files : list[str], optional
        List of file names to exclude from the zip.
    exclude_folders : list[str], optional
        List of folder names to exclude from the zip.
    """
    folder = Path(folder_path)
    exclude_files = exclude_files or []
    exclude_folders = exclude_folders or []

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder):
            # Filter out excluded folders
            dirs[:] = [d for d in dirs if d not in exclude_folders]

            for file in files:
                if file in exclude_files:
                    continue

                file_path = Path(root) / file
                # Create relative path for zip archive
                archive_name = file_path.relative_to(folder)
                zipf.write(file_path, archive_name)
                log.debug("Added to zip", file=str(archive_name))

    log.info("Zip file created", zip_path=zip_path, source_folder=folder_path)


def upload_folder_as_zip(
    local_folder_path: str,
    r2_zip_path: str,
    public: bool = False,
    exclude_files: Optional[list[str]] = None,
    exclude_folders: Optional[list[str]] = None,
    zip_filename: Optional[str] = None,
    dry_run: bool = False,
) -> str:
    """
    Zip a folder and upload it as a single file to R2.

    Parameters
    ----------
    local_folder_path : str
        Local path to the folder to zip and upload.
    r2_zip_path : str
        R2 destination path for the zip file (e.g., "s3://bucket/path/archive.zip").
        If zip_filename is provided, this should be the folder path.
    public : bool, optional
        Set to True to make the zip file publicly accessible. Defaults to False.
    exclude_files : list[str], optional
        List of file names to exclude from the zip.
    exclude_folders : list[str], optional
        List of folder names to exclude from the zip.
    zip_filename : str, optional
        Name for the zip file. If not provided, uses the folder name.
    dry_run : bool, optional
        If True, creates zip but doesn't upload. Defaults to False.

    Returns
    -------
    str
        Path where the zip file was uploaded (or would be uploaded if dry_run=True).

    Examples
    --------
    # Upload folder as zip with auto-generated name
    upload_folder_as_zip("/path/to/folder", "s3://bucket/archives/")

    # Upload folder as zip with specific name
    upload_folder_as_zip("/path/to/folder", "s3://bucket/archives/", zip_filename="my_data.zip")
    """
    local_path = Path(local_folder_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Local folder does not exist: {local_folder_path}")

    if not local_path.is_dir():
        raise ValueError(f"Path is not a directory: {local_folder_path}")

    # Determine zip filename
    if zip_filename is None:
        zip_filename = f"{local_path.name}.zip"

    # Construct full R2 path
    if r2_zip_path.endswith(".zip"):
        # If r2_zip_path already includes the filename
        full_r2_path = r2_zip_path
    else:
        # If r2_zip_path is a folder, append the zip filename
        if not r2_zip_path.endswith("/"):
            r2_zip_path += "/"
        full_r2_path = r2_zip_path + zip_filename

    # Create temporary zip file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
        temp_zip_path = temp_zip.name

    try:
        # Create the zip
        log.info("Creating zip file", source_folder=local_folder_path, zip_name=zip_filename)
        create_zip_from_folder(
            folder_path=local_folder_path,
            zip_path=temp_zip_path,
            exclude_files=exclude_files,
            exclude_folders=exclude_folders,
        )

        if dry_run:
            log.info("Dry run: zip created but not uploaded", zip_path=temp_zip_path, would_upload_to=full_r2_path)
            return full_r2_path

        # Upload the zip file
        log.info("Uploading zip to R2", r2_path=full_r2_path)
        upload(s3_url=full_r2_path, filename=temp_zip_path, public=public, quiet=False)

        log.info("Successfully uploaded zip file to R2", r2_path=full_r2_path)
        return full_r2_path

    finally:
        # Clean up temporary zip file
        if os.path.exists(temp_zip_path):
            os.unlink(temp_zip_path)
            log.debug("Cleaned up temporary zip file", temp_path=temp_zip_path)


def upload_analysis_as_zip(
    analysis_folder: str,
    folder_name: str,
    r2_bucket: str = "owid-catalog",
    dry_run: bool = False,
) -> str:
    """
    Upload media deaths analysis folder to a fixed R2 address.

    Parameters
    ----------
    analysis_folder : str
        Local path to the analysis folder containing files to upload.
    folder_name : str, optional
        Folder name in R2.
    r2_bucket : str, optional
        R2 bucket name. Defaults to "owid-catalog"
    dry_run : bool, optional
        If True, simulate the upload without actually transferring files. Defaults to False.

    Returns
    -------
    str
        The R2 URL where files were uploaded.

    Examples
    --------
    # Upload analysis folder to default location
    url = upload_analysis_as_zip("/path/to/analysis", "media-deaths-analysis")
    print(f"Files uploaded to: {url}")

    """
    # Upload as a single zip file
    r2_zip_path = f"https://{r2_bucket}/analyses/{folder_name}.zip"
    upload_folder_as_zip(
        local_folder_path=analysis_folder,
        r2_zip_path=r2_zip_path,
        public=True,  # Make publicly accessible
        exclude_files=[".DS_Store", ".ipynb_checkpoints"],
        exclude_folders=["__pycache__"],
        dry_run=dry_run,
    )

    # Return public URL for zip file
    public_url = f"https://{r2_bucket.replace('owid-', '')}.owid.io/analyses/{folder_name}.zip"
    return public_url


# upload_analysis_as_zip(analysis_folder="/Users/tunaacisu/Code/etl/docs/analyses/media_deaths", folder_name="media-deaths-analysis")
