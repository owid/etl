from pathlib import Path
from typing import Optional, Union

import pandas as pd

from etl.paths import BASE_DIR

# Default path to the explorers folder.
EXPLORERS_DIR = BASE_DIR.parent / "owid-content/explorers"


class Explorer:
    """Explorer object, that lets us parse an explorer file, modify its content, and write to a tsv file.

    NOTE: For now, this class is only adapted to indicator-based explorers!
    """

    def __init__(self, name: str):
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
                    graphers_content.append(line)
                elif columns_content_starts:
                    columns_content.append(line)
                else:
                    config_content.append(line)

        # Store comment lines.
        self.comments = comment_lines

        # Parse the explorer config.
        self.config = {
            parts[0]: parts[1] if len(parts) > 1 else None for parts in (line.split("\t", 1) for line in config_content)
        }

        # Parse the explorer graphers table.
        self.df_graphers = pd.DataFrame.from_records(
            [line.split("\t") for line in graphers_content[1:]], columns=graphers_content[0].split("\t")
        )

        if len(columns_content) > 0:
            # Parse the explorer columns table.
            self.df_columns = pd.DataFrame.from_records(
                [line.split("\t") for line in columns_content[1:]], columns=columns_content[0].split("\t")
            )
        else:
            self.df_columns = pd.DataFrame()

    def generate_content(self):
        # Reconstruct the comments section.
        # NOTE: We assume comments are at the beginning of the file.
        # Any comments in a line in the middle of the explorer will be brought to the beginning.
        comments_part = self.comments

        # Reconstruct the config section
        config_part = [f"{key}\t{value}" if value else key for key, value in self.config.items()]

        # Reconstruct the graphers section.
        graphers_tsv: str = self.df_graphers.to_csv(sep="\t", index=False)  # type: ignore
        graphers_part = ["graphers"] + [line.rstrip() for line in graphers_tsv.split("\n")]

        # Reconstruct the columns section if it exists
        if not self.df_columns.empty:
            columns_tsv: str = self.df_columns.to_csv(sep="\t", index=False)  # type: ignore
            columns_part = ["columns"] + [line.rstrip() for line in columns_tsv.split("\n")]
        else:
            columns_part = []

        # Combine all sections
        full_content = (
            "\n".join(comments_part + config_part + graphers_part + (columns_part if columns_part else [])) + "\n"
        )

        return full_content

    def write(self, path: Optional[Union[str, Path]] = None) -> None:
        if path is None:
            path = self.path

        # Write parsed content to file.
        Path(path).write_text(self.generate_content())
