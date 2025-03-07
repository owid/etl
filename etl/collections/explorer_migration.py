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


# path = Path("/home/lucas/repos/owid-content/explorers/monkeypox.explorer.tsv")
# name = Path(path.stem).stem
# explorer_json = parse_explorer(name, path)
