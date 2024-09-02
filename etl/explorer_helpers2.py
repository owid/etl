from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from structlog import get_logger

from etl import config
from etl.db import get_variables_data
from etl.files import upload_file_to_server
from etl.paths import EXPLORERS_DIR

# Initialize logger.
log = get_logger()


class Explorer:
    """Explorer object that lets us parse an explorer file, create a new one, modify its content, and write a tsv file.

    NOTE: For now, this class is only adapted to indicator-based explorers!
    """

    def __init__(self, content: Optional[str] = None, sep: str = ","):
        """Build Explorer object from `content`.

        `content` is the raw text from the explorer config file.
        `sep`: is the delimiter in the config file. ',' for CSV, '\t' for TSV.
        """
        # Comments at the beginning of the explorer file.
        self.comments = []

        if content is None:
            log.info("Initializing a new explorer file from scratch.")
            self.create_empty()
        else:
            # Text content of an explorer file. (this is given by the user)
            self.content = content

            # Split content in lines
            content = content.splitlines()

            # Get raw data
            config_raw = []
            for line_nr, line in enumerate(content):
                if line[:8] in {"columns", "graphers"}:
                    break
                elif line.startswith("#"):
                    self.comments.append(line)
                else:
                    config_raw.append(line)

            # Config
            self.config = self._parse_config(config_raw, sep)

            # Read graphers (and columns) as dataframe
            csv_data = StringIO("\n".join(content[line_nr:]))
            df = pd.read_csv(csv_data, sep=sep, skiprows=0)
            df = self._process_df(df, sep)

            # Separate graphers and columns tables
            df_graphers = self._get_df_nested(df, "graphers")
            df_columns = self._get_df_nested(df, "columns")

            # Process dataframes
            self.df_graphers = self._upgrade_df_graphers(df_graphers)
            self.df_columns = self._upgrade_df_columns(df_columns)

    @classmethod
    def from_file(cls, path: str) -> "Explorer":
        """Load explorer config from a given path (tsv or csv)."""

        if not (path.endswith("csv") or path.endswith("tsv")):
            raise ValueError("Path should be CSV of TSV")
        if Path(path).exists():
            log.info(f"Loading explorer file {path}.")
            with open(path, "r") as f:
                content = f.read()
            if path.endswith("csv"):
                sep = ","
            else:
                sep = "\t"
            return cls(content, sep=sep)
        else:
            raise ValueError(f"Unknown path '{path}'!")

    @classmethod
    def from_owid_content(cls, name: str) -> "Explorer":
        """Load explorer config from a file in owid-content directory.

        NOTE: owid-content should be at the same level as etl.
        """
        name = name
        path = (Path(EXPLORERS_DIR) / name).with_suffix(".explorer.tsv")

        # Build explorer from file
        explorer = cls.from_file(path)

        # Save path to use when exporting?

        return explorer

    @staticmethod
    def _parse_config(config_raw, sep):
        config_raw = [c for c in config_raw if c.strip() != ""]
        if sep == ",":
            csv_data = StringIO("\n".join(config_raw))
            df_config = pd.read_csv(csv_data, sep=sep, skiprows=0)

            # Drop columns with all NaNs
            df_config = df_config.dropna(axis=1, how="all")
            # Drop rows with value NaN
            df_config = df_config.dropna(subset=df_config.columns[1])

            assert df_config.shape[1] == 2, "Header of explorer should only have two columns! Please review"
            config = df_config.set_index(df_config.columns[0]).squeeze().to_dict()
        elif sep == "\t":
            config = {
                parts[0]: parts[1] if len(parts) > 1 else None for parts in (line.split("\t", 1) for line in config_raw)
            }
        else:
            raise ValueError(f"Unknown separator {sep}")

        if "selection" in config:
            config["selection"] = config["selection"].split("\t")
        return config

    @staticmethod
    def _process_df(df: pd.DataFrame, sep: str = "\t"):
        """Some minor initial formatting of the explorer config content."""
        if sep == "\t":
            df = df.reset_index()
        elif sep == ",":
            df = df.reset_index(drop=True)

        # Categorize columns vs graphers
        df[df.columns[0]] = df[df.columns[0]].ffill()
        name_fillna = {"columns", "graphers"} - set(df[df.columns[0]])
        assert len(name_fillna) >= 1
        df[df.columns[0]] = df[df.columns[0]].fillna(name_fillna.pop())

        return df

    @staticmethod
    def _get_df_nested(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
        """Get graphers/columns fields as a clean dataframe."""
        # Keep relevant rows
        df = df.loc[df[df.columns[0]].isin([keyword])]
        # Remove first column, and first row
        df = df.drop(columns=[df.columns[0]]).dropna(axis=0, how="all").reset_index(drop=True)
        # Set column headers
        df, df.columns = df[1:], df.iloc[0]
        # Remove unnecessary columns
        df = df.dropna(axis=1, how="all")
        return df

    def create_empty(self):
        """Create empty object if no content is provided.

        This can be useful when explorer config is created on the fly.
        """
        # Initialize all required internal attributes.
        # Text content of an explorer file.
        self.content = ""
        # Configuration of the explorer (defined at the beginning of the file).
        self.config = {
            "explorerTitle": self.name,
            "isPublished": "false",
        }
        # Graphers table of the explorer.
        self.df_graphers = pd.DataFrame([], columns=["yVariableIds"])
        # Columns table of the explorer.
        self.df_columns = pd.DataFrame([], columns=["variableId"])

    @staticmethod
    def _process_df_common(df) -> pd.DataFrame:
        # Boolean types
        for column in df.columns:
            if set(df[column]) <= {"false", "true", None}:
                df[column] = df[column].map({"false": False, "true": True, None: None}).astype(bool)
        return df

    def _upgrade_df_graphers(self, df: pd.DataFrame) -> pd.DataFrame:
        # Boolean types
        df = self._process_df_common(df)

        # Convert "yVariableIds" into a list of integers, or strings (if they are catalog paths).
        if "yVariableIds" in df.columns:
            df["yVariableIds"] = [
                [int(variable_id) if variable_id.isnumeric() else variable_id for variable_id in variable_ids.split()]
                for variable_ids in df["yVariableIds"]
            ]

        return df

    def _upgrade_df_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        def _parse_color_numeric(value):
            if isinstance(value, str):
                value = value.split(";")
                values_new = []
                for v in value:
                    v = v.split(",")
                    if len(v) == 1:
                        bin = int(v[0]) if v[0].isdigit() else float(v[0])
                        values_new.append(bin)
                    elif len(v) == 3:
                        bin = int(v[0]) if v[0].isdigit() else float(v[0])
                        values_new.append((bin, v[1], v[2]))
                assert all([isinstance(v, (int, float)) or (len(v) == 3) for v in values_new])
                return values_new
            return value

        # Boolean types
        df = self._process_df_common(df)

        # Convert string variable ids to integers.
        if "variableId" in df.columns:
            df["variableId"] = df["variableId"].astype("UInt64")

        # Convert strings of brackets separated by ";" to list of brackets.
        if "colorScaleNumericBins" in df.columns:
            df["colorScaleNumericBins"] = df["colorScaleNumericBins"].apply(_parse_color_numeric)

        if "colorScaleNumericMinValue" in df.columns:
            # Convert strings of numbers to floats.
            df["colorScaleNumericMinValue"] = df["colorScaleNumericMinValue"].replace("", None).astype(float)

        return df

    @property
    def generate_df(self):
        # 1/ CONFIG (and comments)
        # Pre-process special fields
        conf = self.config
        conf["selection"] = "\t".join(self.config["selection"])
        # Convert to dataframe df_config
        df_config = pd.DataFrame.from_dict([conf]).T.reset_index()
        df_config.columns = [0, 1]
        df_config = self._add_top_empty_row(df_config)
        if self.comments != []:
            df_comments = pd.DataFrame(self.comments, columns=[0])
            df_config = pd.concat([df_comments, df_config], ignore_index=True)

        # 2/ GRAPHERS
        df_graphers = self._adapt_df_nested(self.df_graphers, "graphers")
        df_graphers = self._downgrade_df_graphers(df_graphers)
        # 3/ COLUMNS
        df_columns = self._adapt_df_nested(self.df_columns, "columns")
        df_columns = self._downgrade_df_columns(df_columns)

        # Combine
        df = pd.concat(
            [
                df_config,
                df_graphers,
                df_columns,
            ],
            ignore_index=True,
        )
        return df

    def generate_content(self) -> str:
        content = self.as_df.to_csv(sep="\t", index=False, header=False)
        return content

    def _downgrade_df_graphers(self, df: pd.DataFrame):
        # Convert boolean columns to strings of true, false.
        for column in df.select_dtypes(include="bool").columns:
            df[column] = df[column].astype(str).str.lower()

        if "yVariableIds" in df.columns:
            if not all([isinstance(ids, list) for ids in df["yVariableIds"]]):
                raise ValueError(
                    "Each row in 'yVariableIds' (in the graphers dataframe) must contain a list of variable ids (or ETL paths)."
                )
            # Convert lists of variable ids to strings.
            df["yVariableIds"] = df["yVariableIds"].apply(lambda x: " ".join(str(variable_id) for variable_id in x))

        # For convenience, ensure the first columns are index columns (yVariableIds, variableId and/or catalogPath).
        index_columns = ["yVariableIds"]
        df = df[
            [col for col in index_columns if col in df.columns]
            + [col for col in df.columns if col not in index_columns]
        ]
        return df

    def _downgrade_df_columns(self, df: pd.DataFrame):
        # Convert boolean columns to strings of true, false.
        for column in df.select_dtypes(include="bool").columns:
            df[column] = df[column].astype(str).str.lower()

        if "colorScaleNumericBins" in df.columns:
            # Convert list of brackets into strings separated by ";".
            df["colorScaleNumericBins"] = [
                ";".join(map(str, row)) if row else "" for row in df["colorScaleNumericBins"]
            ]
        if "variableId" in df.columns:
            if df["variableId"].isnull().all():
                # Remove column if it contains only nan.
                df = df.drop(columns=["variableId"])
            else:
                # Otherwise, ensure it's made of integers (and possibly nans).
                df["variableId"] = df["variableId"].astype("Int64")

        # For convenience, ensure the first columns are index columns (yVariableIds, variableId and/or catalogPath).
        index_columns = ["catalogPath", "variableId"]
        df = df[
            [col for col in index_columns if col in df.columns]
            + [col for col in df.columns if col not in index_columns]
        ]
        return df

    def _adapt_df_nested(self, df: pd.DataFrame, keyword: str):
        headers = pd.DataFrame([df.columns.values], columns=df.columns)
        df = pd.concat([headers, df], ignore_index=True)
        df.columns = range(1, df.shape[1] + 1)

        # Add empty row
        df = self._add_top_empty_row(df)

        # Add top-level property name
        df[0] = keyword
        df.loc[1:, 0] = np.nan

        # Order columns
        df = df.sort_index(axis=1)

        return df

    @staticmethod
    def _add_top_empty_row(df: pd.DataFrame):
        empty_row = pd.DataFrame([[np.nan] * len(df.columns)], columns=df.columns)
        df = pd.concat([empty_row, df], ignore_index=True)
        return df

    @staticmethod
    def _df_to_lines(df: pd.DataFrame) -> List[str]:
        df = df.copy()

        if df.empty:
            return []

        if "colorScaleNumericBins" in df.columns:
            # Convert list of brackets into strings separated by ";".
            df["colorScaleNumericBins"] = [
                ";".join(map(str, row)) if row else "" for row in df["colorScaleNumericBins"]
            ]

        if "variableId" in df.columns:
            if df["variableId"].isnull().all():
                # Remove column if it contains only nan.
                df = df.drop(columns=["variableId"])
            else:
                # Otherwise, ensure it's made of integers (and possibly nans).
                df["variableId"] = df["variableId"].astype("Int64")

        # Convert boolean columns to strings of true, false.
        for column in df.select_dtypes(include="bool").columns:
            df[column] = df[column].astype(str).str.lower()

        # For convenience, ensure the first columns are index columns (yVariableIds, variableId and/or catalogPath).
        index_columns = ["yVariableIds", "catalogPath", "variableId"]
        df = df[
            [col for col in index_columns if col in df.columns]
            + [col for col in df.columns if col not in index_columns]
        ]

        df_tsv = df.to_csv(sep="\t", index=False)
        lines = ["\t" + line.rstrip() if len(line) > 0 else "" for line in df_tsv.split("\n")]  # type: ignore

        return lines

    @staticmethod
    def _ignore_commented_and_empty_lines(content: str) -> str:
        _content = "\n".join([line for line in content.split("\n") if (len(line) > 0) and (not line.startswith("#"))])
        return _content

    def has_changed(self) -> bool:
        # Return True if content of explorer has changed, and False otherwise.
        # NOTE: The original content and the generated one may differ either because of commented lines or empty lines.
        # Ignore those lines, and check if the original and the new content coincide.
        original = self._ignore_commented_and_empty_lines(content=self.content)
        current = self._ignore_commented_and_empty_lines(content=self.generate_content())
        content_has_changed = original != current

        return content_has_changed

    def save(self, path: Optional[Union[str, Path]] = None) -> None:
        if path is None:
            path = self.path

        path = Path(path)

        # Write parsed content to file.
        path.write_text(self.generate_content())

        # Upload it to staging server.
        if config.STAGING:
            upload_file_to_server(path, f"owid@{config.DB_HOST}:~/owid-content/explorers/")

    def get_variable_config(self, variable_id: int) -> Dict[str, Any]:
        variable_config = {}
        # Load configuration for a variable from the explorer columns section, if any.
        if "variableId" in self.df_columns:
            variable_row = self.df_columns.loc[self.df_columns["variableId"] == variable_id]
            if len(variable_row) == 1:
                variable_config = variable_row.set_index("variableId").loc[variable_id].to_dict()
            elif len(variable_row) > 1:
                # Not sure if this could happen, but raise an error if there are multiple entries for the same variable.
                log.error(f"Explorer 'columns' table contains multiple rows for variable {variable_id}")

        return variable_config

    def get_variable_config_from_catalog_path(self, catalog_path: str) -> Dict[str, Any]:
        variable_config = {}
        if "catalogPath" in self.df_columns:
            variable_row = self.df_columns.loc[self.df_columns["catalogPath"] == catalog_path]

            if len(variable_row) == 1:
                variable_config = variable_row.set_index("catalogPath").loc[catalog_path].to_dict()
            elif len(variable_row) > 1:
                # Not sure if this could happen, but raise an error if there are multiple entries for the same variable.
                log.error(f"Explorer 'columns' table contains multiple rows for variable {catalog_path}")
        return variable_config

    def check(self) -> None:
        # TODO: Create checks that raise warnings and errors if the explorer has formatting issue.
        log.warning("Function not yet implemented!")
        pass

    def convert_ids_to_etl_paths(self) -> None:
        # Gather all variable ids from the graphers and columns tables.
        variable_ids_from_graphers = sorted(
            [
                variable_id
                for variable_id in set(sum(self.df_graphers["yVariableIds"].tolist(), []))
                if str(variable_id).isnumeric()
            ]
        )
        variable_ids = variable_ids_from_graphers
        if "variableId" in self.df_columns:
            variable_ids_from_columns = sorted(
                [
                    variable_id
                    for variable_id in set(self.df_columns["variableId"].tolist())
                    if str(variable_id).isnumeric()
                ]
            )
            variable_ids = sorted(set(variable_ids + variable_ids_from_columns))

        if len(variable_ids) == 0:
            log.warning("No variable ids found.")
            id_to_etl_path = dict()
        else:
            # Fetch the catalog paths for all required variables from database.
            df_from_db = get_variables_data(filter={"id": variable_ids})[["id", "catalogPath"]]
            # Warn if any variable id has no catalog path.
            variable_ids_missing = df_from_db[df_from_db["catalogPath"].isna()]["id"].to_list()
            if any(variable_ids_missing):
                log.warning(f"Missing catalog paths for {len(variable_ids_missing)} variables: {variable_ids_missing}")
                df_from_db = df_from_db.dropna(subset=["catalogPath"])
            # Create a dictionary that maps variable ids (for all required variables) to etl paths.
            id_to_etl_path = df_from_db.set_index("id").to_dict()["catalogPath"]

        # Map variable ids to etl paths in the graphers table, whenever possible.
        self.df_graphers["yVariableIds"] = self.df_graphers["yVariableIds"].apply(
            lambda x: [
                id_to_etl_path.get(variable_id) if variable_id in id_to_etl_path else variable_id for variable_id in x
            ]
        )

        # Map variable ids to etl paths in the columns table, whenever possible.
        # Here, I assume that, if there is a catalog path, then add it to the catalogPath column, and make the value in variableId None.
        # And, if there is no catalog path, then keep the variableId as it is, and make catalogPath None.
        self.df_columns["catalogPath"] = self.df_columns["variableId"].apply(
            lambda x: id_to_etl_path.get(x) if x in id_to_etl_path else None
        )
        self.df_columns["variableId"] = self.df_columns["variableId"].apply(
            lambda x: None if x in id_to_etl_path else x
        )
        # If there is no catalog path, remove the column.
        if self.df_columns["catalogPath"].isna().all():
            self.df_columns = self.df_columns.drop(columns=["catalogPath"])
        # If there is no variableId, remove the column.
        if self.df_columns["variableId"].isna().all():
            self.df_columns = self.df_columns.drop(columns=["variableId"])
