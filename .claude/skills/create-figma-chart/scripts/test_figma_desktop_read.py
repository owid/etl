#!/usr/bin/env python3
"""Tests for figma_desktop_read.py's exit-code contract, against a stub MCP server.

Run:  .venv/bin/python .claude/skills/create-figma-chart/scripts/test_figma_desktop_read.py

What this pins is the one distinction the script exists to make: `2` means the desktop server's
daily read quota is exhausted — stop calling it today and use the hosted connector — and `1` means
an ordinary failure whose remedy is to fix the node id or the active tab. Folding them together
sends a caller to re-check ids that were fine, and it happened twice: `shot` reported a mid-batch
quota hit as a generic failure, and `meta` did the same after `check` and `shot` were fixed.

The server refuses in TWO shapes and both are covered — a tool result carrying `isError`, and a
transport-level JSON-RPC `error`. No Figma desktop app is needed, and none is contacted: the stub
binds a free port and the module's SERVER is pointed at it. It must never be pointed at 3845, the
real app's port, because every call there spends the real daily allowance — which is exactly what
this script's own docs forbid.
"""

from __future__ import annotations

import importlib.util
import io
import json
import socket
import sys
import threading
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

SCRIPT = Path(__file__).with_name("figma_desktop_read.py")
REAL_SERVER = "http://127.0.0.1:3845/mcp"
QUOTA = "Rate limit exceeded, please try again tomorrow"
BAD_NODE = "No node could be found for the provided nodeId"

# What the stub answers with on the next call: which refusal shape, and the message inside it.
STATE = {"shape": "isError", "text": QUOTA}

FAILURES = 0


def check(name: str, got, want) -> None:
    global FAILURES
    if got == want:
        print(f"  ok   {name}")
    else:
        FAILURES += 1
        print(f"  FAIL {name}: got {got!r}, want {want!r}")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass  # keep the harness output readable

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        req = json.loads(body) if body else {}
        method = req.get("method", "")
        if method == "notifications/initialized":
            self.send_response(202)
            self.send_header("mcp-session-id", "stub")
            self.end_headers()
            return
        if method == "initialize":
            payload = {"jsonrpc": "2.0", "id": req.get("id"), "result": {"protocolVersion": "2024-11-05"}}
        elif STATE["shape"] == "jsonrpc":
            payload = {"jsonrpc": "2.0", "id": req.get("id"), "error": {"code": -32000, "message": STATE["text"]}}
        else:
            payload = {
                "jsonrpc": "2.0",
                "id": req.get("id"),
                "result": {"isError": True, "content": [{"type": "text", "text": STATE["text"]}]},
            }
        data = f"data: {json.dumps(payload)}\n\n".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("mcp-session-id", "stub")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def load_module(url: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location("figma_desktop_read_under_test", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Fail loudly if the constant is renamed, rather than silently testing against the real app.
    assert mod.SERVER == REAL_SERVER, f"SERVER is {mod.SERVER!r}, expected the real default"
    mod.SERVER = url
    assert mod.SERVER != REAL_SERVER
    return mod


def invoke(mod: ModuleType, argv: list[str]) -> tuple[int, str]:
    """Run the script's main() with argv, returning its exit code and captured output."""
    old = sys.argv
    sys.argv = ["figma_desktop_read.py", *argv]
    out, err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(out), redirect_stderr(err):
            code = mod.main()
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
    finally:
        sys.argv = old
    return code, out.getvalue() + err.getvalue()


def main() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    mod = load_module(f"http://127.0.0.1:{port}/mcp")

    with TemporaryDirectory() as tmp:
        modes = [
            (["check", "--expect-node", "1:2"], "check"),
            (["shot", "1:2", "--out-dir", tmp], "shot"),
            (["meta", "1:2"], "meta"),
            (["meta"], "meta (page list)"),
        ]

        for shape in ("isError", "jsonrpc"):
            print(f"quota exhaustion arriving as {shape} exits 2 in every mode")
            STATE["shape"], STATE["text"] = shape, QUOTA
            for argv, label in modes:
                code, text = invoke(mod, argv)
                check(f"{label} exits 2", code, 2)
                check(f"{label} names the quota", "quota" in text.lower(), True)
                check(f"{label} points at the hosted path", "hosted" in text.lower(), True)

        print("an ordinary failure stays 1, or the distinction buys nothing")
        STATE["shape"], STATE["text"] = "isError", BAD_NODE
        for argv, label in modes:
            code, text = invoke(mod, argv)
            check(f"{label} exits 1", code, 1)
            check(f"{label} does not claim quota", "quota" in text.lower(), False)

    server.shutdown()
    print("\nall checks passed" if not FAILURES else f"\n{FAILURES} FAILURES")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
