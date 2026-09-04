#!/usr/bin/env python3
"""Harness for sweep_session.py.

The two claims worth testing are the two BENCHMARK.md warns about, because both fail SILENTLY —
a wrong number, never an error. Concurrency read off a per-message count reports a provably
concurrent run as singletons; a turn measured without checking the message id counts the gap
between two calls in the SAME message as thinking time.
"""

import json
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SWEEP = HERE / "sweep_session.py"
T0 = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), str(detail)[:220]))


def stamp(offset):
    return (T0 + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def transcript(calls):
    """calls: (msg_id, tool_use_id, tool, start, end). One assistant record per message id."""
    lines, by_msg = [], {}
    for msg, tid, tool, start, _end in calls:
        by_msg.setdefault(msg, []).append((tid, tool, start))
    for msg, blocks in by_msg.items():
        lines.append({"type": "assistant", "timestamp": stamp(min(b[2] for b in blocks)),
                      "message": {"id": msg, "role": "assistant",
                                  "content": [{"type": "tool_use", "id": t, "name": n} for t, n, _ in blocks]}})
    for _msg, tid, _tool, _start, end in calls:
        lines.append({"type": "user", "timestamp": stamp(end),
                      "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tid}]}})
    # A record the sweep must skip rather than crash on.
    lines.append({"type": "bridge-session", "sessionId": "x"})
    lines.append({"type": "assistant", "timestamp": stamp(0), "message": {"id": "m0", "content": "plain string"}})
    return "\n".join(json.dumps(x) for x in lines) + "\n"


def run(calls, *args):
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
        fh.write(transcript(calls))
        path = fh.name
    proc = subprocess.run([sys.executable, str(SWEEP), path, *args], capture_output=True, text=True)
    Path(path).unlink()
    return proc


F = "mcp__claude_ai_Figma__"

# Four calls issued in ONE message, all overlapping: the batched shape.
BATCHED = [("m1", "t1", F + "get_screenshot", 0, 10),
           ("m1", "t2", F + "get_screenshot", 1, 11),
           ("m1", "t3", F + "get_screenshot", 2, 12),
           ("m1", "t4", F + "get_screenshot", 3, 13)]

# The same four, one per message, strictly serial.
SERIAL = [(f"m{i}", f"t{i}", F + "get_screenshot", i * 12, i * 12 + 10) for i in range(1, 5)]

out = run(BATCHED)
check("batched: exits 0", out.returncode == 0, out.stderr)
check("batched: peak in flight is 4, not 1",
      "peak in flight: 4" in out.stdout, out.stdout)
check("batched: counts one message", " in 1 message(s)" in out.stdout, out.stdout)
check("batched: no turn is invented between calls in the same message",
      "turns between calls: 0" in out.stdout, out.stdout)

out = run(SERIAL)
check("serial: peak in flight is 1", "peak in flight: 1" in out.stdout, out.stdout)
check("serial: counts four messages", " in 4 message(s)" in out.stdout, out.stdout)
check("serial: finds three turns", "turns between calls: 3" in out.stdout, out.stdout)
check("serial: turn is the GAP, not the call (2s, not 10s)",
      "median 2.0s" in out.stdout, out.stdout)

# The A/B shape: two tools, told apart by --list.
MIXED = [("m1", "t1", F + "use_figma", 0, 3), ("m2", "t2", F + "get_screenshot", 10, 19)]
out = run(MIXED, "--list")
check("--list prints one line per call in order",
      out.stdout.count("use_figma") >= 2 and "  1  " in out.stdout and "  2  " in out.stdout, out.stdout)
check("median is per tool, not pooled",
      "3.00s" in out.stdout and "9.00s" in out.stdout, out.stdout)

out = run(MIXED, "--tool", "use_figma")
check("--tool filters to one shape", "1 call(s)" in out.stdout, out.stdout)

out = run(MIXED, "--since", stamp(5))
check("--since drops earlier calls", "1 call(s)" in out.stdout, out.stdout)

# A tool_use with no tool_result is still in flight; counting it would report a call that never ended.
UNPAIRED = [("m1", "t1", F + "use_figma", 0, 3)]
with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
    fh.write(json.dumps({"type": "assistant", "timestamp": stamp(0), "message": {
        "id": "m1", "content": [{"type": "tool_use", "id": "orphan", "name": F + "use_figma"}]}}) + "\n")
    orphan_path = fh.name
proc = subprocess.run([sys.executable, str(SWEEP), orphan_path], capture_output=True, text=True)
Path(orphan_path).unlink()
check("an unfinished call is not counted", proc.returncode == 1 and "no matching" in proc.stderr,
      proc.stdout + proc.stderr)

bad = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'' if ok else '  >> ' + detail}")
print(f"\n{len(bad)} FAILURES" if bad else f"\nall checks passed ({len(results)} checks)")
sys.exit(1 if bad else 0)
