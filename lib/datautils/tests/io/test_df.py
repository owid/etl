"""Test functions in owid.datautils.io.local module.

"""
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pytest import raises

from owid.datautils.io.df import from_file, to_file


class TestLoadDf:
    df_original = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["a", "b", "c"])

    def test_from_file_basic(self, tmpdir):
        output_methods = {
            "csv": self.df_original.to_csv,
            # "dta": self.df_original.to_stata,
            "feather": self.df_original.to_feather,
            "parquet": self.df_original.to_parquet,
            "pickle": self.df_original.to_pickle,
            "pkl": self.df_original.to_pickle,
            "xlsx": self.df_original.to_excel,
            "xml": self.df_original.to_xml,
        }
        for extension, funct in output_methods.items():
            file = tmpdir / f"test.{extension}"
            if extension in ["dta", "pickle", "pkl", "feather"]:
                funct(file)
            else:
                funct(file, index=False)
            df = from_file(str(file))
            assert df.equals(self.df_original)

    def test_from_file_json(self, tmpdir):
        file = tmpdir / "test.json"
        self.df_original.to_json(file, orient="records")
        df = from_file(str(file))
        assert df.equals(self.df_original)
        # Compressed
        file = tmpdir / "test.zip"
        self.df_original.to_json(file, orient="records")
        df = from_file(str(file), file_type="json")
        assert df.equals(self.df_original)

    def test_from_file_html(self, tmpdir):
        file = tmpdir / "test.html"
        self.df_original.to_html(file, index=False)
        df = from_file(str(file))[0]
        assert df.equals(self.df_original)

    # Writing to hdf requires additional dependencies that are causing issues.
    # def test_from_file_hdf(self, tmpdir):  # hdf5?
    #     file = tmpdir / "test.hdf"
    #     self.df_original.to_hdf(file, key="df")
    #     df = from_file(str(file))
    #     assert df.equals(self.df_original)

    def test_from_file_dta(self, tmpdir):
        file = tmpdir / "test.dta"
        self.df_original.to_stata(file, write_index=False)
        df = from_file(str(file))
        assert df.astype(int).equals(self.df_original.astype(int))
        # Compressed
        file = tmpdir / "test.zip"
        self.df_original.to_stata(file, write_index=False)
        df = from_file(str(file), file_type="dta")
        assert df.astype(int).equals(self.df_original.astype(int))

    def test_from_file_csv_zip(self, tmpdir):
        file = tmpdir / "test.zip"
        self.df_original.to_csv(file, index=False)
        df = from_file(str(file), file_type="csv")
        assert df.equals(self.df_original)

    def test_from_file_filenotfound(self, tmpdir):
        """File does not exist."""
        with raises(FileNotFoundError):
            file = tmpdir / "test.csv"
            _ = from_file(str(file))

    def test_from_file_zip_err(self, tmpdir):
        """Compressed file, but no file type is given."""
        with raises(ValueError):
            file = tmpdir / "test.zip"
            _ = from_file(str(file))

    def test_from_file_format_err(self, tmpdir):
        """Unknown file format."""
        with raises(ValueError):
            file = tmpdir / "test.error"
            _create_csv_file(file)
            _ = from_file(str(file))


def _create_csv_file(file: Any) -> Any:
    content = "a,b,c\n1,2,3\n4,5,6"
    file.write_text(content, encoding="utf-8")


def _store_dataframe_in_temp_file_and_read_it(
    df: pd.DataFrame, file_path: Path, **kwargs: Any
) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save dataframe in a temporary file.
        temp_file = Path(temp_dir) / file_path
        to_file(df, file_path=temp_file, **kwargs)
        # Read dataframe.
        recovered_df = pd.read_csv(temp_file)

    return recovered_df


class TestToFile:
    def test_save_csv_file_with_dummy_index(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv")
        )
        assert recovered_df.equals(df)

    def test_save_csv_file_with_dummy_index_and_overwrite(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "temp.csv"
            to_file(df, file_path=temp_file)
            # Overwrite dataframe.
            to_file(df, file_path=temp_file)
            # Read dataframe.
            recovered_df = pd.read_csv(temp_file)
        assert recovered_df.equals(df)

    def test_save_csv_file_with_dummy_index_and_fail_to_overwrite(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "temp.csv"
            to_file(df, file_path=temp_file)
            with raises(FileExistsError):
                # Attempt to overwrite dataframe.
                to_file(df, file_path=temp_file, overwrite=False)

    def test_save_csv_file_within_subfolders(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        # Save dataframe in a temporary file, ensuring all required subfolders are created.
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("dir_1/dir_2/temp.csv")
        )
        assert recovered_df.equals(df)

    def test_save_csv_file_with_single_column_and_single_index(self):
        df = pd.DataFrame({"a": [1, 2, 3]}).set_index("a")
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv")
        )
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_single_index(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(
            "a"
        )
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv")
        )
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_single_index_as_a_list(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(
            ["a"]
        )
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv")
        )
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_multiple_indexes(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(
            ["a", "b"]
        )
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv")
        )
        assert recovered_df.equals(df.reset_index())

    def test_save_csv_file_with_multiple_columns_and_multiple_indexes_ignoring_indexes(
        self,
    ):
        # Now the dataframe will be stored ignoring the index (so it will only have column "c").
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(
            ["a", "b"]
        )
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv"), index=False
        )
        assert recovered_df.equals(df.reset_index(drop=True))

    def test_save_csv_file_with_multiple_columns_and_multiple_indexes_and_additional_kwarg(
        self,
    ):
        # We will impose that nans must be replaced by a certain number (by using keyword argument 'na_rep').
        df = pd.DataFrame(
            {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, np.nan, 9]}
        ).set_index(["a", "b"])
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv"), na_rep=80
        )
        assert recovered_df.equals(df.reset_index().replace({np.nan: 80}))

    def test_save_parquet_file_keeping_multiindex(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(
            ["a", "b"]
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.parquet"
            to_file(df, file_path=temp_file)
            # Read dataframe.
            recovered_df = pd.read_parquet(temp_file)
        assert recovered_df.equals(df)

    def test_save_parquet_file_resetting_multiindex(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}).set_index(
            ["a", "b"]
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.parquet"
            to_file(df, file_path=temp_file, index=False)
            # Read dataframe.
            recovered_df = pd.read_parquet(temp_file)
        assert recovered_df.equals(df.reset_index(drop=True))

    def test_save_feather_file(self):
        # Multiindex dataframes cannot be stored as feather files.
        # Also, df.to_feather() does not accept an 'index' argument.
        # This test will check that 'index' is not been passed on to 'to_feather'.
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.feather"
            to_file(df, file_path=temp_file)
            # Read dataframe.
            recovered_df = pd.read_feather(temp_file)
        assert recovered_df.equals(df)

    def test_raise_error_on_unknown_file_extension(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save dataframe in a temporary file.
            temp_file = Path(temp_dir) / "test.made_up_extension"
            with raises(ValueError):
                to_file(df, file_path=temp_file)

    def test_dataframe_save_csv_file(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        recovered_df = _store_dataframe_in_temp_file_and_read_it(
            df=df, file_path=Path("temp.csv")
        )
        assert recovered_df.equals(df)
