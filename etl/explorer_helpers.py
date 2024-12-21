from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from deprecated import deprecated
from structlog import get_logger

from etl import config
from etl.files import upload_file_to_server
from etl.grapher_io import get_variables_data
from etl.paths import EXPLORERS_DIR

# Initialize logger.
log = get_logger()


@deprecated("Use the etl.explorer.Explorer class instead.")
class Explorer:
    """Explorer object that lets us parse an explorer file, create a new one, modify its content, and write a tsv file.

    The only argument required is the name of the explorer file (without the path or the ".explorer.tsv" extension).
    To access or modify the content of the explorer, simply work with the df_graphers and df_columns dataframes.
    Then, to write the changes to the explorer file, call the write() method.

    NOTE: For now, this class is only adapted to indicator-based explorers!
    """

    def __init__(self, name: str):
        self.name = name
        self.path = (Path(EXPLORERS_DIR) / name).with_suffix(".explorer.tsv")

        # Initialize all required internal attributes.
        # Text content of an explorer file.
        self.content = ""
        # Comments at the beginning of the explorer file.
        self.comments = []
        # Configuration of the explorer (defined at the beginning of the file).
        self.config = {
            "explorerTitle": self.name,
            "isPublished": False,
        }
        # Graphers table of the explorer.
        self.df_graphers = pd.DataFrame([], columns=["yVariableIds"])
        # Columns table of the explorer.
        self.df_columns = pd.DataFrame([], columns=["variableId"])

        if self.path.exists():
            log.info(f"Loading explorer file {self.path}.")
            # Read explorer from existing file.
            self._load_content()
            self._parse_content()
        else:
            log.info(f"Initializing a new explorer file {self.path} from scratch.")

    def _load_content(self):
        # Load content of explorer file as a string.
        with open(self.path, "r") as f:
            self.content = f.read()

        if "yVariableIds" not in self.content:
            raise NotImplementedError(
                "Unexpected error. This can be for various reasons. Likely reasons are: (i) For the moment, Explorer is only adapted to indicator-based explorers. (ii) Explorer config tsv is not up-to-date in owid-content, pelase pull latest changes."
            )

    def _parse_content(self):
        # Initialize flags that will help parse the content.
        graphers_content_starts = False
        columns_content_starts = False
        # Initialize lists that will store lines of different sections.
        graphers_content = []
        columns_content = []
        config_content = []
        comment_lines = []

        # Parse the lines of the explorer content.
        for line in self.content.strip().split("\n"):
            # line = line.strip()
            if len(line) == 0:
                # Skip empty lines.
                continue
            elif line.startswith("#"):
                # Store comment lines.
                comment_lines.append(line)
                continue
            elif line == "graphers":
                # The graphers table is starting.
                graphers_content_starts = True
                columns_content_starts = False
                continue
            elif line == "columns":
                graphers_content_starts = False
                columns_content_starts = True
                continue
            else:
                if graphers_content_starts:
                    graphers_content.append(line.lstrip())
                elif columns_content_starts:
                    columns_content.append(line.lstrip())
                else:
                    config_content.append(line.lstrip())

        # Store comment lines.
        self.comments = comment_lines

        # Parse the explorer config.
        self.config = {
            parts[0]: parts[1] if len(parts) > 1 else None for parts in (line.split("\t", 1) for line in config_content)
        }

        # Parse the explorer graphers table.
        self.df_graphers = self._lines_to_df(lines=graphers_content)

        # Parse the explorer columns table.
        self.df_columns = self._lines_to_df(lines=columns_content)

    @staticmethod
    def _lines_to_df(lines: List[str]) -> pd.DataFrame:
        if len(lines) == 0:
            return pd.DataFrame()

        # Parse the explorer graphers table.
        df = pd.DataFrame.from_records([line.split("\t") for line in lines[1:]], columns=lines[0].split("\t"))
        # Improve dataframe format.
        for column in df.columns:
            if set(df[column]) <= {"false", "true", None}:
                df[column] = df[column].map({"false": False, "true": True, None: None}).astype(bool)

        if "variableId" in df.columns:
            # Convert string variable ids to integers.
            df["variableId"] = df["variableId"].astype("Int64")

        if "yVariableIds" in df.columns:
            # Convert "yVariableIds" into a list of integers, or strings (if they are catalog paths).
            df["yVariableIds"] = [
                [
                    int(variable_id) if variable_id.isnumeric() else variable_id
                    for variable_id in variable_ids.split(" ")
                ]
                for variable_ids in df["yVariableIds"]
            ]

        if "colorScaleNumericBins" in df.columns:
            # Convert strings of brackets separated by ";" to list of brackets.
            df["colorScaleNumericBins"] = [
                [int(bracket) if bracket.isdigit() else float(bracket) for bracket in row.rstrip(";").split(";")]
                if row
                else None
                for row in df["colorScaleNumericBins"]
            ]

        if "colorScaleNumericMinValue" in df.columns:
            # Convert strings of numbers to floats.
            df["colorScaleNumericMinValue"] = df["colorScaleNumericMinValue"].replace("", None).astype(float)

        return df

    @staticmethod
    def _df_to_lines(df: pd.DataFrame) -> List[str]:
        df = df.copy()

        if df.empty:
            return []

        if "yVariableIds" in df.columns:
            if not all([isinstance(ids, list) for ids in df["yVariableIds"]]):
                raise ValueError(
                    "Each row in 'yVariableIds' (in the graphers dataframe) must contain a list of variable ids (or ETL paths)."
                )
            # Convert lists of variable ids to strings.
            df["yVariableIds"] = df["yVariableIds"].apply(lambda x: " ".join(str(variable_id) for variable_id in x))

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

    def generate_content(self):
        # Reconstruct the comments section.
        # NOTE: We assume comments are at the beginning of the file.
        # Any comments in a line in the middle of the explorer will be brought to the beginning.
        comments_part = self.comments

        # Reconstruct the config section.
        config_part = []
        for key, value in self.config.items():
            if value is not None:
                if isinstance(value, list):
                    # Special case that happens at least for the "selection" key, which is a list of strings.
                    config_part_row = f"{key}\t" + "\t".join(value)
                elif isinstance(value, bool):
                    # Convert True and False to "true" and "false".
                    config_part_row = f"{key}\t{str(value).lower()}"
                else:
                    # Normal case, where value is just one item.
                    config_part_row = f"{key}\t{value}"
            else:
                config_part_row = f"{key}"
            config_part.append(config_part_row)

        # Reconstruct the graphers section.
        graphers_part = ["graphers"] + self._df_to_lines(df=self.df_graphers)

        # Reconstruct the columns section if it exists
        if not self.df_columns.empty:
            columns_part = ["columns"] + self._df_to_lines(df=self.df_columns)
        else:
            columns_part = []

        # Combine all sections
        full_content = (
            "\n".join(comments_part + [""] + config_part + graphers_part + (columns_part if columns_part else []))
            + "\n"
        )

        return full_content

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
