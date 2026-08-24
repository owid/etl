#!/usr/bin/env python3
"""Read from the Figma DESKTOP MCP server, which serves this skill's reads ~30x faster than the
hosted connector — 0.37 s a screenshot against 12 s, and it parallelizes where the hosted one queues.

Only for reads. The desktop server offers six tools, all read-only, and none of what this skill
writes with (`use_figma`, `upload_assets`, `search_design_system`); those stay on the hosted
connector. See reference/GOTCHAS.md for the measurements and the full constraint list.

Two constraints decide whether this is usable at all, and both fail loudly here rather than quietly:

  * There is no fileKey parameter. The server renders from whatever document is the ACTIVE TAB in
    the running desktop app, so the Charts file must be open and frontmost. A node it cannot see
    reports "No node could be found ... make sure ... the document containing the node is the
    active tab" — which is also what you get when the right file is open but a different tab is on
    top, so treat that message as "check the tab", not "wrong node id".
  * A cloud session has no desktop app, so none of this exists there. Fall back to the hosted
    connector's `get_screenshot`.

Screenshots come back as inline base64 PNGs, with none of the hosted tool's
original_width/original_height JSON — so this reads the real pixel size out of each PNG header and
reports it. Note there is no maxDimension AND a silent 1024 px cap on the longer edge: the printed
size is the RENDERED size, which equals the natural size only below that cap (the Reel template,
natural 616x1096, arrives 576x1024). Needing true natural size above 1024 px means the hosted
get_screenshot, which reports it whatever it renders.

Usage:
    # Screenshot nodes, concurrently, into a directory:
    python3 scripts/figma_desktop_read.py shot 798:161 6689:8 --out-dir /tmp/shots

    # Structure of a page or frame as XML (all 198 pages, if you omit the node):
    python3 scripts/figma_desktop_read.py meta 798:54
    python3 scripts/figma_desktop_read.py meta

Exits non-zero if any requested node failed, so a caller can trust a zero exit.
"""

from __future__ import annotations

import argparse
import base64
import json
import struct
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SERVER = "http://127.0.0.1:3845/mcp"
TIMEOUT = 120
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def rpc(payload: dict, session: str | None = None) -> dict:
    """One JSON-RPC call. The server answers in SSE framing even for single responses."""
    headers = {
        "Content-Type": "application/json",
        # Required: the server refuses a request that does not accept the event-stream framing.
        "Accept": "application/json, text/event-stream",
    }
    if session:
        headers["mcp-session-id"] = session
    req = urllib.request.Request(SERVER, data=json.dumps(payload).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read().decode()
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"cannot reach the Figma desktop MCP server at {SERVER}: {exc}\n"
            "Is the Figma desktop app running? (A cloud session has no desktop app — use the "
            "hosted connector's get_screenshot instead.)"
        ) from exc
    # Strip SSE framing: the JSON payload is spread over the "data: " lines.
    data = "".join(line[6:] for line in body.splitlines() if line.startswith("data: "))
    return json.loads(data) if data else {}


def connect() -> str | None:
    """Initialize a session. One handshake serves many calls, so callers should reuse the id."""
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "owid-create-figma-chart", "version": "1"},
        },
    }
    req = urllib.request.Request(SERVER, data=json.dumps(payload).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            session = resp.headers.get("mcp-session-id")
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"cannot reach the Figma desktop MCP server at {SERVER}: {exc}\nIs the Figma desktop app running?"
        ) from exc
    rpc({"jsonrpc": "2.0", "method": "notifications/initialized"}, session)
    return session


def call_tool(name: str, arguments: dict, req_id: int, session: str | None) -> dict:
    return rpc(
        {"jsonrpc": "2.0", "id": req_id, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
        session,
    )


def result_text(result: dict) -> str:
    return "".join(item.get("text", "") for item in result.get("content", []))


def shot(nodes: list[str], out_dir: Path) -> int:
    session = connect()
    out_dir.mkdir(parents=True, exist_ok=True)

    def one(index_node: tuple[int, str]) -> tuple[str, str]:
        index, node = index_node
        reply = call_tool("get_screenshot", {"nodeId": node}, 100 + index, session)
        result = reply.get("result", {})
        if result.get("isError"):
            return node, f"FAILED {result_text(result).strip()}"
        for item in result.get("content", []):
            if item.get("type") != "image":
                continue
            png = base64.b64decode(item.get("data", ""))
            if not png.startswith(PNG_MAGIC):
                return node, "FAILED response was not a PNG"
            # PNG IHDR carries width/height as big-endian uint32 at byte offset 16.
            width, height = struct.unpack(">II", png[16:24])
            path = out_dir / f"{node.replace(':', '-')}.png"
            path.write_bytes(png)
            return node, f"{path}  {width}x{height}  {len(png):,}B"
        return node, "FAILED no image in response"

    # The server serves these concurrently — six at once measured 0.51 s against 2.2 s serially.
    with ThreadPoolExecutor(max_workers=min(8, len(nodes))) as pool:
        results = list(pool.map(one, enumerate(nodes)))

    failures = 0
    for node, line in results:
        print(f"{node}: {line}")
        failures += line.startswith("FAILED")
    return failures


def meta(node: str | None) -> int:
    session = connect()
    args = {"clientLanguages": "unknown", "clientFrameworks": "unknown"}
    if node:
        args["nodeId"] = node
    reply = call_tool("get_metadata", args, 200, session)
    result = reply.get("result", {})
    text = result_text(result)
    if result.get("isError"):
        print(f"FAILED {text.strip()}", file=sys.stderr)
        return 1
    print(text)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)

    p_shot = sub.add_parser("shot", help="screenshot one or more nodes, concurrently")
    p_shot.add_argument("nodes", nargs="+", metavar="NODE_ID", help="e.g. 798:161 (or 798-161)")
    p_shot.add_argument("--out-dir", type=Path, default=Path("."), help="where to write the PNGs")

    p_meta = sub.add_parser("meta", help="XML structure of a node, or the page list with no node")
    p_meta.add_argument("node", nargs="?", metavar="NODE_ID")

    args = ap.parse_args()
    if args.mode == "shot":
        return 1 if shot(args.nodes, args.out_dir) else 0
    return meta(args.node)


if __name__ == "__main__":
    sys.exit(main())
