"""Migrate old explorer config to new one."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from structlog import get_logger

log = get_logger()

JSON_VERSION = 1

FLAGS = {
    "backgroundSeriesLimit",
    "downloadDataLink",
    "entityType",
    "explorerSubtitle",
    "explorerTitle",
    "facet",
    "googleSheet",
    "hasMapTab",
    "hideAlertBanner",
    "hideControls",
    "hideTitleAnnotation",
    "indexViewsSeparately",
    "isPublished",
    "sourceDesc",
    "subNavCurrentId",
    "subNavId",
    "subtitle",
    "tab",
    "thumbnail",
    "title",
    "type",
    "wpBlockId",
    "yAxisMin",
    "ySlugs",
}


@dataclass
class Statement:
    verb: str
    args: List[str]
    block: Optional[List[dict]]

    def is_flag(self):
        return self.verb in FLAGS

    def name(self) -> str:
        assert self.block or self.verb == "table"
        if not self.args or self.args[-1].startswith("http"):
            return "_default"
        return self.args[-1]

    def __post_init__(self):
        assert self.verb


def parse_explorer(slug: str, explorer_file: Path) -> dict:
    statements = parse_line_by_line(explorer_file.as_posix(), slug)

    result: Dict[Any, Any] = {"_version": JSON_VERSION}
    for s in statements:
        if s.is_flag():
            result[s.verb] = s.args[0] if s.args else None

        elif s.verb not in ("table", "graphers", "columns"):
            result[s.verb] = s.args

        else:
            blocks = result.setdefault("blocks", [])
            blocks.append({"type": s.verb, "args": s.args, "block": s.block})

    if "isPublished" not in result:
        result["isPublished"] = "false"

    return result


def parse_line_by_line(explorer_file, slug) -> List[Statement]:
    with open(explorer_file, "r", encoding="utf-8") as f:  # specify encoding to make sure it works on all OSs
        lines = f.readlines()

    # skip empty lines and comments
    lines = [l for l in lines if l.strip() or l.startswith("##")]

    data = []
    i = 0
    while i < len(lines):
        parts = lines[i].rstrip("\n\t").split("\t")
        verb = parts[0]
        args = parts[1:]
        i += 1

        if not is_block(verb, args):
            data.append(Statement(verb, args, None))
            continue

        # consume a multiline block
        records, offset = read_block(lines, slug, i)

        # fast forward the number of records plus the header row
        i += offset

        data.append(Statement(verb, args, records))

    return data


def is_block(verb: str, args: List[str]) -> int:
    if verb in ("graphers", "columns"):
        return True

    return verb == "table" and not args


def read_block(lines: List[str], slug: str, start: int) -> Tuple[List[dict], int]:
    """
    Read an embedded tsv block in the file.
    """
    # accumulate the block of lines, without the leading tab
    block = []
    i = start
    while i < len(lines) and lines[i].startswith("\t"):
        line = lines[i][1:].rstrip("\n")
        block.append(line)
        i += 1
    assert len(block) == i - start

    records = tsv_to_records(block, slug, start)

    # only leave significant values in the records
    prune_nulls(records)

    offset = i - start

    if i > 0:
        assert len(records) == offset - 1

    return records, i - start


def prune_nulls(records):
    for r in records:
        for k in list(r):
            if r[k] in (None, ""):
                del r[k]


def tsv_to_records(block: List[str], slug: str, start: int) -> List[dict]:
    # do this ourselves instead of using csv.DictReader because the format coming in
    # contains unterminated quotes and other fun that messes up the standard reader

    if block[0].endswith("\t"):
        log.warn("loose tab on block header", name=slug, line_no=start)

    header = block[0].rstrip("\t").split("\t")
    if not all(col for col in header):
        log.error("empty column name", name=slug, line_no=start)
        # fill in a dummy column name just to allow parsing to continue
        for i, col in enumerate(header):
            if not col:
                header[i] = f"_column{i}"

    records = []
    for line in block[1:]:
        record = dict(zip(header, line.split("\t")))
        records.append(record)

    return records


################ WIP

# Read and parse all config
explorers = {}
explorer_dir = Path("/home/lucas/repos/owid-content/explorers/")
explorers_path = explorer_dir.glob("*.explorer.tsv")
explorers_path = sorted(list(explorers_path))
for path in explorers_path:
    name = Path(path.stem).stem
    print(name)
    explorer_json = parse_explorer(name, path)
    explorers[name] = explorer_json

# Filter and keep public ones
explorers = {k: v for k, v in explorers.items() if v["isPublished"] == "true"}


class ExplorerMigration:
    def __init__(self, explorer: Dict[str, Any], slug: str):
        self.slug = slug
        self.explorer = explorer
        self.config = self.extract_config()
        self.grapher_block, self.table_columns_block = self.get_blocks()
        self.types = self.get_explorer_types()
        self._sanity_checks()

    def extract_config(self):
        config = {}
        for key, value in self.explorer.items():
            if (key not in {"_version", "blocks"}) and not key.startswith("#"):
                config[key] = value
        return config

    def get_blocks(self):
        """Get grapher and table/columns blocks."""
        # Get blocks
        blocks = self.explorer.get("blocks", [])
        if not blocks:
            raise ValueError(f"{self.slug}: No blocks found")

        # Get grapher and tables_columns block
        grapher_block = None
        table_columns_block = []
        for block in blocks:
            if block["type"] == "graphers":
                if grapher_block is None:
                    grapher_block = block
                else:
                    raise ValueError("Multiple graphers block found")
            elif block["type"] in {"table", "columns"}:
                table_columns_block.append(block)
            else:
                raise ValueError(f"{self.slug}: Unknown block type: {block['type']}")

        return grapher_block, table_columns_block

    def get_explorer_types(self):
        """Get types of the explorer.

        It is typically either CSV, Indicator or Grapher. But occasionally it can be a hybrid of these. Example: Grapher and CSV.
        """
        # Inspect record to detect explorer type
        assert self.grapher_block is not None, "No grapher block found"
        cols = pd.DataFrame(self.grapher_block["block"]).columns

        explorer_types = []
        if "grapherId" in cols:
            explorer_types.append("grapher")
        if ("yVariableIds" in cols) or ("xVariableId" in cols):
            explorer_types.append("indicator")
        elif len(self.table_columns_block) > 0:
            explorer_types.append("csv")

        return set(explorer_types)

    def _sanity_checks(self):
        """Sanity check that the explorer configuration makes sense."""
        # Grapher block
        if self.grapher_block is None:
            raise ValueError(f"{self.slug}: No grapher block found")
        # Table/columns block
        ## 1) If no table/column is given, it must be indicator- or grapher-based
        if len(self.table_columns_block) == 0:
            if self.types != {"indicator"} and self.types != {"grapher"}:
                raise ValueError(f"{self.slug}: You must provide a table or columns block for CSV-based explorers")
        else:
            ## 2) table/columns is not accepted for grapher-based
            if self.types == {"grapher"}:
                raise ValueError(f"{self.slug}: table/column block not expected for grapher-based explorers")
            ## 3) length of table/columns must be an even number, at least 2, except for indicator-based
            elif self.types != {"indicator"}:
                # 3.1) table/columns must be of length 2 at least
                if len(self.table_columns_block) < 2:
                    raise ValueError(f"{self.slug}: Table/columns block must be of length 2 at least")
                # 3.2) table/columns must be of length 2n
                if len(self.table_columns_block) % 2 != 0:
                    raise ValueError(f"{self.slug}: Table/columns block must be of length 2n")

    def obtain_display_settings(self):
        """Build dictionary like:

        {"uri": display settings}
        """
        if self.types == {"csv"}:
            print("WIP")
            # Iterate
            display_settings = {}
            table_slugs = {}
            for current, nxt in zip(self.table_columns_block, self.table_columns_block[1:]):
                assert current["type"] == "table"
                assert nxt["type"] == "columns"
                assert len(current["args"]) == 2
                table_slugs[current["args"][1]] = current["args"][0]

                for indicator in nxt["block"]:
                    uri = f"{current['args'][0]}/{indicator['slug']}"
                    _display_settings = {k: v for k, v in indicator.items() if k != "slug"}
                    display_settings[uri] = _display_settings
            return display_settings
        elif self.types == {"indicator"}:
            print("Easily supported")
        else:
            print("Not supported")


import pandas as pd

analysis = []
types_rename = {
    "grapher": "G",
    "indicator": "I",
    "csv": "C",
}
for name, explorer in explorers.items():
    migration = ExplorerMigration(explorer, name)
    # print(migration.grapher_block["args"])
    analysis.append(
        {
            "name": name,
            "types": migration.types,
        }
    )

df = pd.DataFrame(analysis).sort_values("name")
"""Things to do:

- [x] Detect the type of explorer (csv, grapher, indicator)
    - if grapher: can't migrate
    - if indicator: go ahead
    - if csv: all tables, should have catalog URLs; otherwise can't migrate
- [ ] All but blocks, goes into `config`, except for comments
- [ ] Check:
    - blocks:
        - [x] Should exist
        - [x] length 1 allowed for indicator/grapher - based
        - [x] length 2 allowed for indicator - based
        - [x] Otherwise, must be of length 2n+1
- [ ] Columns
- [ ] Graph
"""
