"""Migrate old explorer config to new one."""

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from owid.catalog.utils import underscore
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
PATTERN_CSV = r"https://catalog\.ourworldindata\.org/(?P<group>[^/]+/[^/]+/[^/]+/[^/]+/[^/]+)\.csv"


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
    lines = [line for line in lines if line.strip() or line.startswith("##")]

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


class ExplorerMigration:
    def __init__(self, explorer_as_json: Dict[str, Any], slug: str):
        """Parse DB"""
        self.slug = slug
        self.explorer_as_json = explorer_as_json
        self.config = self.extract_config()
        self.grapher_block, self.table_columns_block = self.get_blocks()
        self.types = self.get_explorer_types()
        self._sanity_checks()

    def extract_config(self):
        config = {}
        for key, value in self.explorer_as_json.items():
            if (key not in {"_version", "blocks"}) and not key.startswith("#"):
                config[key] = value
        return config

    def get_blocks(self) -> Tuple[Any, List[Any]]:
        """Get grapher and table/columns blocks."""
        # Get blocks
        blocks = self.explorer_as_json.get("blocks", [])
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

        # Check required columns
        if self.types == {"csv"}:
            columns = pd.DataFrame(self.grapher_block["block"]).columns
            if "tableSlug" not in columns:
                raise ValueError(f"{self.slug}: tableSlug not found in grapher block")
            if "tableSlugs" in columns:
                raise ValueError(f"{self.slug}: tableSlugs not supported at the moment. Please report!")

        # Table/columns block
        ## 1) If no table/column is given, it must be indicator- or grapher-based
        if len(self.table_columns_block) == 0:
            if self.types != {"indicator"} and self.types != {"grapher"}:
                raise ValueError(f"{self.slug}: You must provide a table or columns block for CSV-based explorers")
        else:
            ## 2) table/columns is not accepted for grapher-based
            if self.types == {"grapher"}:
                raise ValueError(f"{self.slug}: table/column block not expected for grapher-based explorers")
            # ## 3) length of table/columns must be an even number, at least 2, except for indicator-based
            # elif self.types != {"indicator"}:
            #     # 3.1) table/columns must be of length 2 at least
            #     if len(self.table_columns_block) < 2:
            #         raise ValueError(f"{self.slug}: Table/columns block must be of length 2 at least")
            #     # 3.2) table/columns must be of length 2n
            #     if len(self.table_columns_block) % 2 != 0:
            #         raise ValueError(f"{self.slug}: Table/columns block must be of length 2n")

    def run_csv(self):
        """Build dictionary like:

        {"uri": display settings}
        """
        # Iterate over all blocks
        display_settings = {}
        table_slugs = {}

        # Get table information
        for block in self.table_columns_block:
            if block["type"] == "table":
                # Sanity checks
                assert len(block["args"]) == 2

                # Relevant information about table
                table_uri = _extract_table_uri(block["args"][0])
                table_slug = block["args"][1]

                # Save table slug to URI mapping
                table_slugs[table_slug] = table_uri

        # Get columns information
        for block in self.table_columns_block:
            if block["type"] == "columns":
                # Sanity checks
                assert (
                    len(block["args"]) == 1
                ), f"columns is expected to have only one argument. Instead got: {block['args']}"

                # Get table URI
                table_slug = block["args"][0]
                table_uri = table_slugs[table_slug]

                for indicator in block["block"]:
                    indicator_uri = f"{table_uri}#{indicator['slug']}"
                    _display_settings = {k: v for k, v in indicator.items() if k != "slug"}
                    display_settings[indicator_uri] = _display_settings

        # return display_settings

        # Get graphers information
        dimension_slug_to_raw_name = {}
        dimensions = []
        df = pd.DataFrame(self.grapher_block["block"])
        for column in df.columns:
            if column.endswith(" Dropdown"):
                ui_type = "dropdown"
            elif column.endswith(" Radio"):
                ui_type = "radio"
            elif column.endswith(" Checkbox"):
                ui_type = "checkbox"
            else:
                continue

            # Get name, then underscore it for slug
            dim_name = " ".join(column.split()[:-1])
            dim_slug = underscore(str(dim_name))
            # Get choices
            choices = []
            for choice_name in df[column].unique():
                choice_slug = underscore(str(choice_name))
                choices.append(
                    {
                        "slug": choice_slug,
                        "name": choice_name,
                    }
                )
            # Build dimension element
            dimensions.append(
                {
                    "slug": dim_slug,
                    "name": dim_name,
                    "choices": choices,
                    "presentation": {
                        "type": ui_type,
                    },
                }
            )

            # For reference
            dimension_slug_to_raw_name[dim_slug] = column

        # Get views
        views = []

        def _bake_indicator(table_slug, indicator_slug):
            indicator_uri = f"{table_slugs[table_slug]}#{indicator_slug}"
            indicator = {
                "catalogPath": indicator_uri,
            }
            if indicator_uri in display_settings:
                indicator["display"] = display_settings[indicator_uri]

            return indicator

        for block in self.grapher_block["block"]:
            # Get indicators
            table_slug = block["tableSlug"]
            indicators = defaultdict(list)
            if "ySlugs" in block:
                y_slugs = block["ySlugs"].split()
                for y in y_slugs:
                    indicator = _bake_indicator(table_slug, y)
                    indicators["y"].append(indicator)
            if "xSlug" in block:
                slugs = block["xSlug"].split()
                assert len(slugs) == 1
                indicator = _bake_indicator(table_slug, slugs[0])
                indicators["x"].append(indicator)
            if "colorSlug" in block:
                slugs = block["colorSlug"].split()
                assert len(slugs) == 1
                indicator = _bake_indicator(table_slug, slugs[0])
                indicators["color"].append(indicator)
            if "sizeSlug" in block:
                slugs = block["sizeSlug"].split()
                assert len(slugs) == 1
                indicator = _bake_indicator(table_slug, slugs[0])
                indicators["size"].append(indicator)

            indicators = dict(indicators)

            # Get dimensions
            dimensions_view = {}
            for dim in dimensions:
                raw_name = dimension_slug_to_raw_name[dim["slug"]]
                if raw_name not in block:
                    dimensions_view[dim["slug"]] = None
                else:
                    dimensions_view[dim["slug"]] = underscore(block[raw_name])

            # Get config (remainder)
            config = {
                k: v
                for k, v in block.items()
                if k
                not in {
                    "tableSlug",
                    "ySlugs",
                    "xSlug",
                    "colorSlug",
                    "sizeSlug",
                }
            }

            views.append(
                {
                    "indicators": indicators,
                    "dimensions": dimensions_view,
                    "config": config,
                }
            )

        return {
            "config": self.config,
            "views": views,
            "dimensions": dimensions,
        }

    def run(self):
        if self.types == {"csv"}:
            return self.run_csv()
        elif self.types == {"indicator"}:
            raise NotSupportedException("Not supported. Soon will be.")
        else:
            raise NotSupportedException("Not supported")


class NotSupportedException(Exception):
    pass


class TableURLNotInCataloException(Exception):
    pass


def _extract_table_uri(catalog_url: str):
    match = re.fullmatch(PATTERN_CSV, catalog_url)

    if match:
        extracted_fragment = match.group("group")
    else:
        raise TableURLNotInCataloException(f"{catalog_url}")

    return f"{extracted_fragment}"


def migrate_csv_explorer(explorer_path: Union[Path, str]):
    """Migrate the TSV-based config of a CSV-based explorer to the new format.

    Note:
        - Only works for CSV-based explorers which use ETL data (i.e. have a catalog URL for all tables)
        - The output config is not fully functional yet. It might use a catalog path from a table that is not in Grapher (e.g. an 'explorer' table). Modify it so it points to a table in Grapher.

    Local path to explorer."""
    if isinstance(explorer_path, str):
        explorer_path = Path(explorer_path)
    name = Path(explorer_path.stem).stem
    explorer_json = parse_explorer(name, explorer_path)
    migration = ExplorerMigration(explorer_json, name)

    if migration.types != {"csv"}:
        raise ValueError(f"{name}: Not a CSV explorer")

    config = migration.run()

    return config


################ WIP
# 1/ Actual migration example
# import yaml

# config = migrate_csv_explorer("/home/lucas/repos/owid-content/explorers/monkeypox.explorer.tsv")
# print(yaml.dump(config))

# path_new = ""
# with open(path_new):
#     yaml.dump(config, default_flow_style=False, sort_keys=False, width=float("inf"))

# 2/ Read all explorers, more raw experimenting
# import pandas as pd

# # Read and parse all config
# explorers = {}
# explorer_dir = Path("/home/lucas/repos/owid-content/explorers/")
# explorers_path = explorer_dir.glob("*.explorer.tsv")
# explorers_path = sorted(list(explorers_path))
# for path in explorers_path:
#     name = Path(path.stem).stem
#     print(name)
#     explorer_json = parse_explorer(name, path)
#     explorers[name] = explorer_json

# Filter and keep public ones
# explorers = {k: v for k, v in explorers.items() if v["isPublished"] == "true"}


# analysis = []
# types_rename = {
#     "grapher": "G",
#     "indicator": "I",
#     "csv": "C",
# }
# settings = []
# for name, explorer in explorers.items():
#     if name in {"global-food"}:
#         continue
#     migration = ExplorerMigration(explorer, name)
#     try:
#         settings_ = migration.run()
#     except TableURLNotInCataloException as e:
#         print(f"{name}: {e}")
#     except NotSupportedException as e:
#         print(f"{name}: {e}")
#     else:
#         settings.append(settings_)

# df = pd.DataFrame(analysis).sort_values("name")
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
