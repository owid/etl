#!/usr/bin/env python3
"""What a run actually cost: Figma calls, turns, and how much of each was the model.

BENCHMARK.md prescribes this sweep in prose and it gets hand-rolled every time. The prose is the
spec; this is it executed, so two runs are scored the same way.

    .venv/bin/python .claude/skills/create-figma-chart/scripts/sweep_session.py <session.jsonl>
    ... --tool use_figma          # one tool only
    ... --since 2026-09-04T18:00  # a slice of a long session

Transcripts live under ~/.claude/projects/<slugified-cwd>/<session-id>.jsonl. A worktree session has
its OWN projects dir, so pass the path rather than globbing for the newest file.

Two things this deliberately does NOT do, both from BENCHMARK.md:

  - It never scores batching from a calls-per-message histogram. An eight-call probe measured at
    4.12x was scored by that histogram as eight singletons. Concurrency is read off overlapping
    tool_use -> tool_result INTERVALS, which is what `peak in flight` below reports.
  - It reports `sum/wall` only beside the honest figures, never instead of them: it counts a queued
    call's own wait as work, so it flatters batching locally and understates it in the cloud.

`turn` is the gap from a message's LAST tool_result back to the next assistant message's first
tool_use, MINUS whatever other tool was running inside that gap -- the model thinking, which
BENCHMARK.md measures at ~60s of every ~66s call. That is the term to watch, so it is the one term
that must not quietly absorb a shell call's runtime.
"""

import argparse
import json
import sys
from datetime import datetime
from operator import itemgetter

FIGMA = "mcp__claude_ai_Figma__"


def parse_ts(value):
    """ISO-8601 with a trailing Z, which datetime.fromisoformat rejects before 3.11."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load(path, tool_filter=None, since=None):
    """Pair every tool_use with its tool_result, keeping the issuing assistant message id.

    Returns (matched, others): the calls the filter kept, and every OTHER completed tool call in the
    same window. The second list is what keeps the turn honest -- a 40s shell call between two Figma
    calls is not the model thinking, and without it that runtime is reported as thinking time.
    """
    uses, results = {}, {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg = rec.get("message")
        if not isinstance(msg, dict) or not rec.get("timestamp"):
            continue
        ts = parse_ts(rec["timestamp"])
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "tool_use":
                uses[block["id"]] = {"name": block.get("name", "?"), "start": ts, "msg": msg.get("id")}
            elif block.get("type") == "tool_result":
                results[block.get("tool_use_id")] = ts

    matched, others = [], []
    for tid, use in uses.items():
        if tid not in results:
            continue
        if since and use["start"] < since:
            continue
        name = use["name"]
        call = {"tool": name.replace(FIGMA, ""), "msg": use["msg"],
                "start": use["start"], "end": results[tid],
                "secs": (results[tid] - use["start"]).total_seconds()}
        (others if tool_filter and tool_filter not in name else matched).append(call)
    return sorted(matched, key=itemgetter("start")), sorted(others, key=itemgetter("start"))


def peak_in_flight(calls):
    """Concurrency from overlapping intervals -- never from a per-message count.

    The tie-break is load-bearing: at an identical timestamp an end (-1) must be applied before a
    start (+1), or two touching but strictly serial calls -- [0,10] and [10,20] -- report a peak of
    2 and overstate batching, which is the one direction this number must never err in.
    """
    events = [(c["start"], 1) for c in calls] + [(c["end"], -1) for c in calls]
    events.sort(key=lambda e: (e[0], e[1]))
    peak = live = 0
    for _, delta in events:
        live += delta
        peak = max(peak, live)
    return peak


def occupied(start, end, calls):
    """Seconds of [start, end) that some call was running -- their UNION, never their sum."""
    clipped = sorted((max(c["start"], start), min(c["end"], end))
                     for c in calls if c["end"] > start and c["start"] < end)
    total, cursor = 0.0, start
    for lo, hi in clipped:
        if hi <= cursor:
            continue
        total += (hi - max(lo, cursor)).total_seconds()
        cursor = hi
    return total


def batches_of(calls):
    """One entry per issuing assistant message -- [first start, last end] -- in start order."""
    out = {}
    for call in calls:
        b = out.setdefault(call["msg"], {"start": call["start"], "end": call["end"]})
        b["start"] = min(b["start"], call["start"])
        b["end"] = max(b["end"], call["end"])
    return sorted(out.values(), key=itemgetter("start"))


def spanned_messages(calls, others):
    """OTHER assistant messages sitting inside the turns.

    A turn is deliberately the model time to get from one MATCHING call to the next -- the term
    BENCHMARK.md multiplies by the call count -- so a turn interrupted by two shell calls is three
    thinking episodes reported as one number. That is worth knowing rather than hiding, so the count
    is printed: it says when the median is an aggregate instead of a single think.
    """
    ordered, seen = batches_of(calls), set()
    for prev, nxt in zip(ordered, ordered[1:]):
        for c in others:
            if prev["end"] <= c["start"] < nxt["start"]:
                seen.add(c["msg"])
    return len(seen)


def turn_gaps(calls, others=()):
    """Model time: from a message's LAST result back to the next message's first call.

    Measured per message, not between adjacent calls: the model cannot start the next turn until
    every result in the batch is back, so the gap runs from the batch's MAX end. Adjacent pairs
    would measure it from whichever call happened to sort last by start time, and a batch whose
    longest call started first would then be credited with thinking time it never spent.

    `others` -- the calls the tool filter dropped -- are subtracted from each gap. A 40s shell call
    between two Figma calls is another tool running, not the model thinking, and counting it here
    inflates the one figure BENCHMARK.md calls dominant.
    """
    ordered = batches_of(calls)
    gaps = []
    for prev, nxt in zip(ordered, ordered[1:]):
        if nxt["start"] > prev["end"]:
            gaps.append(max(0.0, (nxt["start"] - prev["end"]).total_seconds()
                            - occupied(prev["end"], nxt["start"], others)))
    return gaps


def median(xs):
    if not xs:
        return 0.0
    s = sorted(xs)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("session", help="path to a session .jsonl transcript")
    ap.add_argument("--tool", help="substring filter, e.g. use_figma or get_screenshot")
    ap.add_argument("--since", help="ISO timestamp; ignore calls before it")
    ap.add_argument("--all-tools", action="store_true", help="include non-Figma tools too")
    ap.add_argument("--list", action="store_true", help="one line per call, in order — for an A/B where"
                                                       " two shapes of the same tool have to be told apart")
    args = ap.parse_args()

    since = parse_ts(args.since) if args.since else None
    calls, others = load(args.session, args.tool or (None if args.all_tools else FIGMA), since)
    if not calls:
        print("no matching tool calls", file=sys.stderr)
        return 1

    by_tool = {}
    for call in calls:
        by_tool.setdefault(call["tool"], []).append(call["secs"])

    wall = (max(c["end"] for c in calls) - min(c["start"] for c in calls)).total_seconds()
    total = sum(c["secs"] for c in calls)
    gaps = turn_gaps(calls, others)
    # What the gaps would have claimed as thinking before the other tools were taken out of them.
    elsewhere = sum(turn_gaps(calls)) - sum(gaps) if others else 0.0
    messages = len({c["msg"] for c in calls})

    print(f"{len(calls)} call(s) in {messages} message(s), {wall / 60:.1f} min from first to last\n")
    if args.list:
        for i, call in enumerate(calls, 1):
            print(f"  {i:>3}  {call['start'].strftime('%H:%M:%S')}  {call['secs']:>6.2f}s  {call['tool']}")
        print()
    print(f"{'tool':<28} {'n':>3} {'median':>8} {'min':>8} {'max':>8}")
    for tool, secs in sorted(by_tool.items(), key=lambda kv: -len(kv[1])):
        print(f"{tool:<28} {len(secs):>3} {median(secs):>7.2f}s {min(secs):>7.2f}s {max(secs):>7.2f}s")

    print(f"\nturns between calls: {len(gaps)}, median {median(gaps):.1f}s, total {sum(gaps) / 60:.1f} min")
    if others:
        spanned = spanned_messages(calls, others)
        print(f"  net of {elsewhere / 60:.1f} min that other tools were running inside those turns, across"
              f" {spanned} other message(s) — a turn spanning one is several thinks reported as one number")
    print(f"peak in flight: {peak_in_flight(calls)}  (from overlapping intervals, not a per-message count)")
    if wall:
        print(f"sum/wall: {total / wall:.2f}x  -- read this ONLY beside the figures above; it counts a"
              " queued call's own wait as work")
    return 0


if __name__ == "__main__":
    sys.exit(main())
