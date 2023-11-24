import tarfile
import zipfile
from pathlib import Path
from typing import Union

from pytest import raises

from owid.datautils.io.archive import decompress_file


class TestDecompressZipFile:
    def test_decompress_file_with_content_zip(self, tmp_path):
        _test_decompress_file_with_content(tmp_path, ".zip")

    def test_decompress_file_with_content_within_folder_zip(self, tmp_path):
        _test_decompress_file_with_content_within_folder(tmp_path, ".zip")

    def test_overwrite_file_zip(self, tmp_path):
        _test_overwrite_file(tmp_path, ".zip")

    def test_raise_error_if_file_exists_zip(self, tmp_path):
        _test_raise_error_if_file_exists(tmp_path, ".zip")


class TestDecompressTarGzFile:
    def test_decompress_file_with_content_targz(self, tmp_path):
        _test_decompress_file_with_content(tmp_path, ".tar.gz")

    def test_decompress_file_with_content_within_folder_targz(self, tmp_path):
        _test_decompress_file_with_content_within_folder(tmp_path, ".tar.gz")

    def test_overwrite_file_targz(self, tmp_path):
        _test_overwrite_file(tmp_path, ".tar.gz")

    def test_raise_error_if_file_exists_targz(self, tmp_path):
        _test_raise_error_if_file_exists(tmp_path, ".tar.gz")


class TestDecompressTarBz2File:
    def test_decompress_file_with_content_tarbz2(self, tmp_path):
        _test_decompress_file_with_content(tmp_path, ".tar.bz2")

    def test_decompress_file_with_content_within_folder_tarbz2(self, tmp_path):
        _test_decompress_file_with_content_within_folder(tmp_path, ".tar.bz2")

    def test_overwrite_file_tarbz2(self, tmp_path):
        _test_overwrite_file(tmp_path, ".tar.bz2")

    def test_raise_error_if_file_exists_tarbz2(self, tmp_path):
        _test_raise_error_if_file_exists(tmp_path, ".tar.bz2")


class TestDecompressWrongFile:
    def test_decompress_file_with_content_wrong(self, tmp_path):
        with raises(ValueError):
            _test_raise_error_if_file_exists(tmp_path, ".error")


def _test_decompress_file_with_content(containing_dir: str, f: str) -> None:
    # Create a compressed file with some example content.
    example_content = "Example content."
    file_name = "example_file.txt"
    _create_compressed_file_with_content(
        file_name=file_name,
        containing_dir=containing_dir,
        content=example_content,
        format=f,
    )
    # Decompress the original file in a new folder.
    new_dir = Path(containing_dir) / "new_dir"
    decompress_file(
        input_file=(Path(containing_dir) / file_name).with_suffix(f),
        output_folder=new_dir,
    )
    # Check that, inside the new folder, we find the original (decompressed) folder and the file inside.
    recovered_file = new_dir / file_name
    assert new_dir.is_dir()
    assert recovered_file.is_file()
    # Read the file to check that its content is the expected one.
    with open(recovered_file, "r") as _recovered_file:
        assert _recovered_file.read() == example_content


def _test_decompress_file_with_content_within_folder(containing_dir: str, f: str) -> None:
    # Create a compressed file with some example content within a folder.
    example_content = "Example content."
    file_name = "example_file.txt"
    sub_dir_name = "example_dir"
    _create_compressed_file_with_content_within_folder(
        file_name=file_name,
        containing_dir=containing_dir,
        sub_dir_name=sub_dir_name,
        content=example_content,
        format=f,
    )
    # Decompress the original folder in a new folder.
    new_dir = Path(containing_dir) / "new_dir"
    decompress_file(
        input_file=(Path(containing_dir) / sub_dir_name).with_suffix(f),
        output_folder=new_dir,
    )
    # Check that, inside the new folder, we find the original (decompressed) folder and the file inside.
    recovered_file = new_dir / sub_dir_name / file_name
    assert new_dir.is_dir()
    assert recovered_file.is_file()
    # Read the file to check that its content is the expected one.
    with open(recovered_file, "r") as _recovered_file:
        assert _recovered_file.read() == example_content


def _test_overwrite_file(containing_dir: str, f: str) -> None:
    # Create a compressed file with some example content.
    example_content = "Example content."
    file_name = "example_file.txt"
    _create_compressed_file_with_content(
        file_name=file_name,
        containing_dir=containing_dir,
        content=example_content,
        format=f,
    )
    # Decompress the original file in a new folder.
    new_dir = Path(containing_dir) / "new_dir"
    decompress_file(
        input_file=(Path(containing_dir) / file_name).with_suffix(f),
        output_folder=new_dir,
    )
    # Decompress file again and overwrite previous.
    decompress_file(
        input_file=(Path(containing_dir) / file_name).with_suffix(f),
        output_folder=new_dir,
        overwrite=True,
    )
    # Check that, inside the new folder, we find the original (decompressed) folder and the file inside.
    recovered_file = new_dir / file_name
    assert new_dir.is_dir()
    assert recovered_file.is_file()
    # Read the file to check that its content is the expected one.
    with open(recovered_file, "r") as _recovered_file:
        assert _recovered_file.read() == example_content


def _test_raise_error_if_file_exists(containing_dir: str, f: str) -> None:
    # Create a compressed file with some example content.
    example_content = "Example content."
    file_name = "example_file.txt"
    _create_compressed_file_with_content(
        file_name=file_name,
        containing_dir=containing_dir,
        content=example_content,
        format=f,
    )
    # Decompress the original file in a new folder.
    new_dir = Path(containing_dir) / "new_dir"
    decompress_file(
        input_file=(Path(containing_dir) / file_name).with_suffix(f),
        output_folder=new_dir,
    )
    # Try to decompress file again, which should fail because file exists (and overwrite is by default False).
    with raises(FileExistsError):
        decompress_file(
            input_file=(Path(containing_dir) / file_name).with_suffix(f),
            output_folder=new_dir,
        )


def _create_compressed_file_with_content(
    file_name: str,
    containing_dir: Union[str, Path],
    content: str,
    format: str,
) -> None:
    # Create a file with some example content.
    file_inside = Path(containing_dir) / file_name
    with open(file_inside, "w") as _file_inside:
        _file_inside.write(content)
    # Compress that folder.
    if format == ".zip":
        with zipfile.ZipFile(file_inside.with_suffix(".zip"), "w") as zip_file:
            zip_file.write(file_inside, file_inside.name)
    elif format == ".tar.gz":
        with tarfile.open(file_inside.with_suffix(".tar.gz"), "w:gz") as tar_file:
            tar_file.add(file_inside, file_inside.name)
    elif format == ".tar.bz2":
        with tarfile.open(file_inside.with_suffix(".tar.bz2"), "w:bz2") as tar_file:
            tar_file.add(file_inside, file_inside.name)
    else:
        raise ValueError(f"Format {format} not tested!")


def _create_compressed_file_with_content_within_folder(
    file_name: str,
    containing_dir: Union[str, Path],
    sub_dir_name: str,
    content: str,
    format: str,
) -> None:
    # Create a folder that will later be compressed.
    to_compress_dir = Path(containing_dir) / sub_dir_name
    to_compress_dir.mkdir()
    # Create a file inside that folder with some example content.
    file_inside = to_compress_dir / file_name
    with open(file_inside, "w") as _file_inside:
        _file_inside.write(content)
    # Compress that folder.
    if format == ".zip":
        with zipfile.ZipFile(to_compress_dir.with_suffix(".zip"), "w") as zip_file:
            zip_file.write(file_inside, file_inside.relative_to(to_compress_dir.parent))
    elif format == ".tar.gz":
        with tarfile.open(to_compress_dir.with_suffix(".tar.gz"), "w:gz") as tar_file:
            tar_file.add(file_inside, file_inside.relative_to(to_compress_dir.parent))
    elif format == ".tar.bz2":
        with tarfile.open(to_compress_dir.with_suffix(".tar.bz2"), "w:bz2") as tar_file:
            tar_file.add(file_inside, file_inside.relative_to(to_compress_dir.parent))
    else:
        raise ValueError(f"Format {format} not tested!")
