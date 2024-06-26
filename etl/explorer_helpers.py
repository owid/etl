from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from structlog import get_logger

from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()

# Default path to the explorers folder.
EXPLORERS_DIR = BASE_DIR.parent / "owid-content/explorers"


class Explorer:
    """Explorer object, that lets us parse an explorer file, modify its content, and write to a tsv file.

    NOTE: For now, this class is only adapted to indicator-based explorers!
    """

    def __init__(self, name: str):
        self.name = name
        self.path = (EXPLORERS_DIR / name).with_suffix(".explorer.tsv")
        self._load_content()
        self._parse_content()

    def _load_content(self):
        # Load content of explorer file as a string.
        with open(self.path, "r") as f:
            self.content = f.read()

        if "yVariableIds" not in self.content:
            raise NotImplementedError("For the moment, Explorer is only adapted to indicator-based explorers.")

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
            df["variableId"] = df["variableId"].astype(int)

        if "yVariableIds" in df.columns:
            # Convert "yVariableIds" into a list of integers.
            df["yVariableIds"] = [
                [int(variable_id) for variable_id in variable_ids.split(" ") if variable_id.isnumeric()]
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
            df["colorScaleNumericMinValue"] = df["colorScaleNumericMinValue"].astype(float)

        return df

    @staticmethod
    def _df_to_lines(df: pd.DataFrame) -> List[str]:
        df = df.copy()

        if df.empty:
            return []

        if "yVariableIds" in df.columns:
            # Convert lists of variable ids to strings.
            df["yVariableIds"] = df["yVariableIds"].apply(lambda x: " ".join(str(variable_id) for variable_id in x))

        if "colorScaleNumericBins" in df.columns:
            # Convert list of brackets into strings separated by ";".
            df["colorScaleNumericBins"] = [
                ";".join(map(str, row)) if row else "" for row in df["colorScaleNumericBins"]
            ]

        if "variableId" in df.columns:
            df["variableId"] = df["variableId"].astype(int)

        # Convert boolean columns to strings of true, false.
        for column in df.select_dtypes(include="bool").columns:
            df[column] = df[column].astype(str).str.lower()

        df_tsv = df.to_csv(sep="\t", index=False)
        lines = ["\t" + line.rstrip() if len(line) > 0 else "" for line in df_tsv.split("\n")]  # type: ignore

        return lines

    def generate_content(self):
        # Reconstruct the comments section.
        # NOTE: We assume comments are at the beginning of the file.
        # Any comments in a line in the middle of the explorer will be brought to the beginning.
        comments_part = self.comments

        # Reconstruct the config section
        config_part = [f"{key}\t{value}" if value else key for key, value in self.config.items()]

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

    def write(self, path: Optional[Union[str, Path]] = None) -> None:
        if path is None:
            path = self.path

        # Write parsed content to file.
        Path(path).write_text(self.generate_content())

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
