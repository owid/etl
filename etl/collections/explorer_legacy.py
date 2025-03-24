"""
This module contains logic to map new explorer config to old one. It was designed with indicator-based explorers in mind.

TODO:
    - Why float in colorScaleNumericBins
    - Test without columns
    - Compare content and content_raw
    - Test it in Pablo's scripts
"""

from copy import copy
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher import model as gm
from etl.grapher.io import get_variables_data
from etl.paths import EXPLORERS_DIR, EXPORT_DIR

# Initialize logger.
log = get_logger()

# Fields in config section that are lists and need to be tabbed.
# NOTE: Can we do this for all fields that are lists instead of hardcoding it?
LIST_CONFIG_FIELDS = ["selection", "pickerColumnSlugs"]


class ExplorerLegacy:
    """Explorer object that lets us parse an explorer file, create a new one, modify its content, and write a tsv file.

    NOTE: This class only works for indicator-based explorers!

    You can modify four fields of the explorer config:

        - self.comments: List with comments. This should be preceeded by tge '#' sign, and are placed at the top of the explorer config.
        - self.config: Dictionary with the explorer config parameters. This comes at first, before defining the graphers and columns.
        - self.df_graphers: Dataframe with the graphers configuration. Each row in the dataframe defines an explorer view.
        - self.df_columns: Dataframe with the columns configuration. Use this to customize some of the FASTT of the indicators. This is optional.

    Example:
    --------

    ```py
    explorer = Explorer.from_file("explorer.tsv")

    # Edit config
    explorer.config['key'] = ...
    # Edit comments:
    explorer.comments.append("# Something")
    # Edit graphers
    explorer.df_graphers = ...
    # Edit columns
    explorer.df_columns = ...
    ```
    """

    def __init__(
        self,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        df_graphers: Optional[pd.DataFrame] = None,
        df_columns: Optional[pd.DataFrame] = None,
        comments: Optional[List[str]] = None,
        explorer_path: Optional[str] = None,
    ):
        """Build Explorer object from `content`.

        `content` is the raw text from the explorer config file.
        `sep`: is the delimiter in the config file. ',' for CSV, '\t' for TSV.
        """
        if name is None:
            log.warning("`name` not set. Using 'unknown_name'.")
            name = "unknown_name"

        # Configuration of the explorer (defined at the beginning of the file).
        if config is None:
            config = {
                "explorerTitle": name,
                "isPublished": "false",
            }
        # Graphers table of the explorer.
        if df_graphers is None:
            df_graphers = pd.DataFrame([], columns=["yVariableIds"])
        # Columns table of the explorer.
        if df_columns is None:
            df_columns = pd.DataFrame([], columns=["variableId"])
        # Comments at the beginning of the explorer file.
        if comments is None:
            comments = []

        self.config = config
        self.df_graphers = df_graphers
        self.df_columns = df_columns
        self.comments = comments

        # Others
        self.name = name
        self.explorer_path = explorer_path

        # Path
        self.path = None
        self.content_raw = None

    @classmethod
    def from_raw_string(
        cls,
        content: Optional[str] = None,
        sep: str = ",",
        name: Optional[str] = None,
        explorer_path: Optional[str] = None,
    ):
        """Build Explorer object from `content`.

        `content` is the raw text from the explorer config file.
        `sep`: is the delimiter in the config file. ',' for CSV, '\t' for TSV.
        """
        # Comments at the beginning of the explorer file.
        comments = []

        if content is None:
            log.info("Initializing a new explorer file from scratch.")

            explorer = cls(name=name, explorer_path=explorer_path)
        else:
            # Text content of an explorer file. (this is given by the user)
            assert isinstance(content, str), "content should be a string!"
            content_raw = content

            # Split content in lines
            content_list = content.splitlines()

            # Get raw data
            config_raw = []
            line_nr = 0
            for line_nr, line in enumerate(content_list):
                if line[:8] in {"columns", "graphers"}:
                    break
                elif line.startswith("#"):
                    comments.append(line)
                else:
                    config_raw.append(line)

            # Config
            config = cls._parse_config(config_raw, sep)

            # Read graphers (and columns) as dataframe
            csv_data = StringIO("\n".join(content_list[line_nr:]))
            df = pd.read_csv(csv_data, sep=sep, skiprows=0)
            df = cls._process_df(df, sep)

            # Graphers
            df_graphers = cls._parse_df_graphers(df)

            # Columns
            if "columns" in set(df[df.columns[0]].unique()):
                df_columns = cls._parse_df_columns(df)
            else:
                df_columns = None

            explorer = cls(
                name=name,
                config=config,
                df_graphers=df_graphers,
                df_columns=df_columns,
                comments=comments,
                explorer_path=explorer_path,
            )

            explorer.content_raw = content_raw

        return explorer

    @classmethod
    def from_db(cls, name: str, owid_env: Optional[OWIDEnv] = None) -> "ExplorerLegacy":
        """Load explorer config from DB."""
        # Ensure we have an environment set
        if owid_env is None:
            owid_env = OWID_ENV

        # Build explorer from DB
        with Session(owid_env.engine) as session:
            db_exp = gm.Explorer.load_explorer(session, slug=name)
            if db_exp is None:
                raise ValueError(f"Explorer '{name}' not found in the database.")

        assert "\t" in db_exp.tsv, "Explorer config should use \t separator."
        explorer = cls.from_raw_string(db_exp.tsv, sep="\t", name=name)

        return explorer

    def export(self, path: Union[str, Path]):
        """Export file."""
        path = Path(path)
        # Write parsed content to file.
        path.write_text(self.content)

    def save(self, owid_env: Optional[OWIDEnv] = None) -> None:
        # Ensure we have an environment set
        if owid_env is None:
            owid_env = OWID_ENV

        # Update TSV in owid-content, this is only temporary to see a diff.
        # TODO: Get rid of this and show diff in explorer-diff
        if EXPLORERS_DIR.exists():
            explorer_path = (Path(EXPLORERS_DIR) / self.name).with_suffix(".explorer.tsv")
            explorer_path.write_text(self.content)

        # Upsert config via Admin API
        admin_api = AdminAPI(owid_env)
        admin_api.put_explorer_config(self.name, self.content)

    def to_owid_content(self, path: Optional[Union[str, Path]] = None):
        """Save your config in owid-content and push to server if applicable.

        This is useful when working with config files from the owid-content repository.
        """
        if path is None:
            path = self.path

        # Export content to path
        assert isinstance(path, (str, Path)), "Path should be a string or a Path object."
        self.export(path)

        # Upload it to staging server.
        if self._on_staging():
            upload_file_to_server(Path(path), f"owid@{config.DB_HOST}:~/owid-content/explorers/")

            # Commit on the staging server
            run_command_on_server(
                f"owid@{config.DB_HOST}",
                "cd owid-content && git add . && git diff-index --quiet HEAD || git commit -m ':robot: Update explorer from ETL'",
            )

    @property
    def local_tsv_path(self) -> Path:
        # export://explorers/who/latest/influenza#influenza -> explorers/who/latest/influenza/influenza.tsv
        assert self.explorer_path
        return EXPORT_DIR / (self.explorer_path.split("://")[1].replace("#", "/") + ".tsv")

    def save(self, path: Optional[Union[str, Path]] = None) -> None:
        """See docs for `to_owid_content`."""
        self.to_owid_content(path)

        # Export content to local directory in addition to uploading it to MySQL for debugging.
        log.info(f"Exporting explorer to {self.local_tsv_path}")
        self.export(self.local_tsv_path)

    @staticmethod
    def _parse_config(config_raw, sep) -> Dict[str, Any]:
        """Parse the config at the top of the explorer file."""
        config_raw = [c for c in config_raw if c.strip() != ""]
        if sep == ",":
            csv_data = StringIO("\n".join(config_raw))
            df_config = pd.read_csv(csv_data, sep=sep, skiprows=0)

            if "selection" in set(df_config[df_config.columns[0]]):
                selection = (
                    df_config.loc[df_config[df_config.columns[0]] == "selection", df_config.columns[1:]]
                    .dropna(axis=1)
                    .squeeze()
                    .tolist()
                )
                selection = "\t".join(selection)
                df_config.loc[df_config[df_config.columns[0]] == "selection", df_config.columns[1]] = selection
                df_config.loc[df_config[df_config.columns[0]] == "selection", df_config.columns[2:]] = np.nan

            # Drop columns with all NaNs
            df_config = df_config.dropna(axis=1, how="all")
            # Drop rows with value NaN
            df_config = df_config.dropna(subset=df_config.columns[1])

            assert df_config.shape[1] == 2, "Header of explorer should only have two columns! Please review"
            conf = df_config.set_index(df_config.columns[0]).squeeze().to_dict()
        elif sep == "\t":
            conf = {
                parts[0]: parts[1] if len(parts) > 1 else None for parts in (line.split("\t", 1) for line in config_raw)
            }
        else:
            raise ValueError(f"Unknown separator {sep}")

        if "selection" in conf:
            assert isinstance(conf["selection"], str), "selection should be a string!"
            conf["selection"] = conf["selection"].split("\t")  # type: ignore

        # 'true' -> True, 'false' -> False
        bool_mapping = {
            "true": True,
            "false": False,
        }
        for k in conf.keys():
            conf[k] = bool_mapping.get(str(conf[k]), conf[k])
        return conf

    @staticmethod
    def _process_df(df: pd.DataFrame, sep: str = "\t"):
        """Some minor initial formatting of the explorer graphers (and columns) content."""
        if sep == "\t":
            df = df.reset_index()
        elif sep == ",":
            df = df.reset_index(drop=True)

        # Categorize columns vs graphers
        df[df.columns[0]] = df[df.columns[0]].ffill()

        if df[df.columns[0]].isna().all():
            if (df.columns[0] == "graphers") | (df.columns[-1] == "graphers"):
                df[df.columns[0]] = df[df.columns[0]].fillna("graphers")
        else:
            name_fillna = {"columns", "graphers"} - set(df[df.columns[0]])
            assert len(name_fillna) >= 1
            df[df.columns[0]] = df[df.columns[0]].fillna(name_fillna.pop())

        return df

    @classmethod
    def _parse_df_graphers(cls, df: pd.DataFrame) -> pd.DataFrame:
        # Get raw graphers
        df = cls._get_df_nested(df, "graphers")

        # Boolean types
        df = cls._process_df_common(df)

        def _parse_variable_id(variable_ids):
            if isinstance(variable_ids, str):
                variable_ids = variable_ids.split()
                result = [int(variable_id) if variable_id.isnumeric() else variable_id for variable_id in variable_ids]
            else:
                result = variable_ids
            return result

        # Convert "yVariableIds" into a list of integers, or strings (if they are catalog paths).
        if "yVariableIds" in df.columns:
            df["yVariableIds"] = [_parse_variable_id(variable_ids) for variable_ids in df["yVariableIds"]]

        return df

    @classmethod
    def _parse_df_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = cls._get_df_nested(df, "columns")

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
        df = cls._process_df_common(df)

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

    @staticmethod
    def _get_df_nested(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
        """Get graphers/columns fields as clean dataframes."""
        # Keep relevant rows
        df = df.loc[df[df.columns[0]].isin([keyword])]
        # Remove first column, and first row
        df = df.drop(columns=[df.columns[0]]).dropna(axis=0, how="all").reset_index(drop=True)
        # Set column headers
        df, df.columns = df[1:], df.iloc[0]
        # Remove unnecessary columns
        df = df.dropna(axis=1, how="all")
        return df

    @staticmethod
    def _process_df_common(df) -> pd.DataFrame:
        # Boolean types
        for column in df.columns:
            if set(df[column]) <= {"false", "true", None}:
                df[column] = df[column].map({"false": False, "true": True, None: None}).astype(bool)
        return df

    @property
    def df(self) -> pd.DataFrame:
        dfs = []

        # 0/ COMMENTS
        if self.comments != []:
            df_comments = pd.DataFrame(self.comments, columns=[0])
            dfs.append(df_comments)

        # 1/ CONFIG (and comments)
        # Pre-process special fields
        conf = copy(self.config)
        # Convert to dataframe df_config
        df_config = pd.DataFrame.from_dict([conf]).T.reset_index()  # type: ignore
        df_config.columns = [0, 1]
        df_config = self._add_empty_row(df_config)
        # True, False -> 'true', 'false'
        df_config[1] = df_config[1].apply(lambda x: str(x).lower() if str(x) in {"False", "True"} else x)
        dfs.append(df_config)

        # 2/ GRAPHERS
        df_graphers = self._adapt_df_nested(self._df_graphers_output, "graphers")
        dfs.append(df_graphers)

        # 3/ COLUMNS (only if there's any!)
        if len(self.df_columns) > 0:
            df_columns = self._adapt_df_nested(self._df_columns_output, "columns")
            df_columns = self._add_empty_row(df_columns, "top")
            dfs.append(df_columns)

        # Combine
        df = pd.concat(
            dfs,
            ignore_index=True,
        )

        # Fix config.selection and config.pickerColumnSlugs (should be tabbed!)
        for col in LIST_CONFIG_FIELDS:
            sub_df = df.loc[df[0] == col, 1]
            if sub_df.empty:
                continue
            selections = sub_df.item()
            for i, selection in enumerate(selections):
                if i + 1 > df.shape[1] - 1:
                    # Add column
                    df[i + 1] = ""
                df.loc[df[0] == col, i + 1] = selection

        assert isinstance(df, pd.DataFrame), "df should be a dataframe!"
        return df  # type: ignore

    @property
    def content(self) -> str:
        """Based on modified config, graphers and column, build the raw content text."""
        df_clean = self.df.copy()

        # Replace actual newline characters with literal "\n"
        for col in df_clean.select_dtypes(include=["object"]).columns:
            df_clean[col] = df_clean[col].str.replace("\n", "\\n", regex=False)

        content = df_clean.to_csv(sep="\t", index=False, header=False)

        assert isinstance(content, str), "content should be a string!"
        content = [c.rstrip() for c in content.splitlines()]
        content = "\n".join(content)
        return content

    @property
    def _df_graphers_output(self):
        df_ = self.df_graphers.copy()
        # Convert boolean columns to strings of true, false.
        for column in df_.select_dtypes(include="bool").columns:
            df_[column] = df_[column].astype(str).str.lower()

        # There must be at least one indicator given!!!
        columns_indicators = [
            "yVariableIds",
            "ySlugs",
            "xVariableId",
            "xSlug",
            "sizeVariableId",
            "sizeSlug",
            "colorVariableId",
            "colorSlug",
        ]
        columns_indicators = [col for col in columns_indicators if col in df_.columns]
        x = df_[columns_indicators].copy().astype(str)
        assert (x.groupby(columns_indicators).size() >= 1).all()

        # Convert lists of variable ids to strings.

        if "yVariableIds" in df_.columns:
            # Convert lists of variable ids to strings.
            df_["yVariableIds"] = df_["yVariableIds"].apply(
                lambda x: " ".join(str(variable_id) for variable_id in x) if isinstance(x, list) else x
            )
        if "ySlugs" in df_.columns:
            # Convert lists of variable ids to strings.
            df_["ySlugs"] = df_["ySlugs"].apply(
                lambda x: " ".join(str(variable_id) for variable_id in x) if isinstance(x, list) else x
            )

        # For convenience, ensure the first columns are index columns (yVariableIds, variableId and/or catalogPath).
        df_ = df_[
            [col for col in columns_indicators if col in df_.columns]
            + [col for col in df_.columns if col not in columns_indicators]
        ]
        return df_

    @property
    def _df_columns_output(self):
        df_ = self.df_columns.copy()

        def _parse_color_numeric(value):
            if isinstance(value, list):
                elements = []
                for v in value:
                    # value = value.split(";")
                    # values_new = []
                    # for v in value:
                    if isinstance(v, tuple):
                        assert len(v) == 3, "Tuple must be of length 3!"
                        elements.append(",".join([str(vv) for vv in v]))
                    else:
                        elements.append(str(v))
                return ";".join(elements)
            return value

        # Convert boolean columns to strings of true, false.
        for column in df_.select_dtypes(include="bool").columns:
            df_[column] = df_[column].astype(str).str.lower()

        # Convert strings of brackets separated by ";" to list of brackets.
        if "colorScaleNumericBins" in df_.columns:
            df_["colorScaleNumericBins"] = df_["colorScaleNumericBins"].apply(_parse_color_numeric)

        if "variableId" in df_.columns:
            if df_["variableId"].isnull().all():
                # Remove column if it contains only nan.
                df_ = df_.drop(columns=["variableId"])
            else:
                # Otherwise, ensure it's made of integers (and possibly nans).
                df_["variableId"] = df_["variableId"].astype("Int64")

        # For convenience, ensure the first columns are index columns (yVariableIds, variableId and/or catalogPath).
        index_columns = ["catalogPath", "variableId"]
        df_ = df_[
            [col for col in index_columns if col in df_.columns]
            + [col for col in df_.columns if col not in index_columns]
        ]
        return df_

    def _adapt_df_nested(self, df: pd.DataFrame, keyword: str):
        headers = pd.DataFrame([df.columns.values], columns=df.columns)
        df = pd.concat([headers, df], ignore_index=True)
        df.columns = range(1, df.shape[1] + 1)

        # Add empty row at the top
        df = self._add_empty_row(df, "top")

        # Add top-level property name
        df[0] = keyword
        df.loc[1:, 0] = np.nan

        # Order columns
        df = df.sort_index(axis=1)

        return df

    @staticmethod
    def _add_empty_row(df: pd.DataFrame, where: str = "top"):
        empty_row = pd.DataFrame([[np.nan] * len(df.columns)], columns=df.columns)
        if where == "top":
            dfs = [empty_row, df]
        elif where == "bottom":
            dfs = [df, empty_row]
        else:
            raise ValueError("`where` should be 'top' or 'bottom'.")
        df = pd.concat(dfs, ignore_index=True)
        return df

    @staticmethod
    def _ignore_commented_and_empty_lines(content: str) -> str:
        _content = "\n".join([line for line in content.split("\n") if (len(line) > 0) and (not line.startswith("#"))])
        return _content

    def has_changed(self) -> bool:
        # Return True if content of explorer has changed, and False otherwise.
        # NOTE: The original content and the generated one may differ either because of commented lines or empty lines.
        # Ignore those lines, and check if the original and the new content coincide.
        if self.content_raw is None:
            log.warning("There is no original content to compare with!")
            return True

        original = self._ignore_commented_and_empty_lines(content=self.content_raw)
        current = self._ignore_commented_and_empty_lines(content=self.content)
        content_has_changed = original != current

        return content_has_changed

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

        if not self.df_columns.empty and ("variableId" not in self.df_columns.columns):
            raise ValueError(
                "This config file does not contain a `variableId` column in the columns section. It might be the case that it was already migrated to ETL-paths. Please review."
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


def create_explorer_legacy(
    dest_dir: Union[str, Path],
    config: Dict[str, Any],
    df_graphers: pd.DataFrame,
    df_columns: Optional[pd.DataFrame] = None,
    explorer_name: Optional[str] = None,
) -> ExplorerLegacy:
    """This function is used to create an Explorer object using the legacy configuration.

    To use the new tools, first migrate the explorer to use the new MDIM-based configuration.
    """
    log.warning(
        "This function is operative, but relies on legacy configuration. To use latest tools, consider migrating your explorer to use MDIM-based configuration."
    )
    # Extract information about this step from dest_dir.
    channel, namespace, version, short_name = str(dest_dir).split("/")[-4:]
    explorer_path = f"{channel}/{namespace}/{version}/{short_name}"

    # If the name of the explorer is specified in config, take that, otherwise use the step's short_name.
    # NOTE: This is the expected name of the explorer tsv file.
    if explorer_name is None:
        if "name" in config:
            explorer_name = config["name"]
        else:
            explorer_name = short_name
    assert isinstance(explorer_name, str)

    return _create_explorer_legacy(
        explorer_path=explorer_path,
        config=config,
        df_graphers=df_graphers,
        explorer_name=explorer_name,
        df_columns=df_columns,
    )


def _create_explorer_legacy(
    explorer_path: str,
    config: Dict[str, Any],
    df_graphers: pd.DataFrame,
    explorer_name: str,
    df_columns: Optional[pd.DataFrame] = None,
    reset: bool = False,
) -> ExplorerLegacy:
    """This function is used to create an Explorer object using the legacy configuration.

    To use the new tools, first migrate the explorer to use the new MDIM-based configuration.

    :param reset: If True, create explorer from scratch. If False, update explorer in database.
    """
    if reset:
        # Create explorer from scratch.
        explorer = ExplorerLegacy.from_raw_string(
            content=None, sep="\t", name=explorer_name, explorer_path=explorer_path
        )
    else:
        # Load explorer from database.
        explorer = ExplorerLegacy.from_db(explorer_name)

    # TODO: this is temporary, in the future Explorers won't have path attribute.
    explorer.path = (Path(EXPLORERS_DIR) / explorer_name).with_suffix(".explorer.tsv")  # type: ignore

    # Add a comment to avoid manual edits.
    explorer.comments = [f"# DO NOT EDIT THIS FILE MANUALLY. IT WAS GENERATED BY ETL step '{explorer_path}'."]
    # Update its config.
    explorer.config.update(config)
    # Update its graphers and columns tables.
    explorer.df_graphers = df_graphers
    if df_columns is not None:
        explorer.df_columns = df_columns

    return explorer
