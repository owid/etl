"""Per-step wall-time & token cost report for the update-dataset workflow.

Joins the `## Step timing log` section of `workbench/<short_name>/progress.md`
with the Claude Code session transcripts (`~/.claude/projects/<encoded-cwd>/`)
and writes a markdown table of duration and token usage per workflow step.

Attribution model:
- Main-session API requests are bucketed into steps by timestamp (a step covers
  the interval between the previous log line and its own `DONE` line).
- Each subagent transcript (`<session-id>/subagents/agent-*.jsonl`) is
  attributed wholly to the step during which its first request fired.
- Transcript lines are deduplicated by `requestId` — Claude Code writes one
  line per content block, all carrying the same usage object, so naive summing
  over-counts several-fold.

Usage:
    .venv/bin/python .claude/skills/update-dataset/scripts/cost_report.py workbench/<short_name> \
        [--session <session-id> ...] [--project-dir <dir>] [--output <path>]

With no --session, sessions are auto-discovered: every transcript in the
project dir whose text mentions the workbench directory.
"""

import argparse
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Relative price multipliers vs. one input token (stable across current Claude
# models: output 5x, cache write 1.25x, cache read 0.1x). They produce a single
# comparable "input-equivalent tokens" number per step — not a USD estimate.
WEIGHTS = {"input": 1.0, "output": 5.0, "cache_write": 1.25, "cache_read": 0.1}

TIMING_LINE = re.compile(r"^- (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) (START|DONE)(?: (.*))?$")


@dataclass
class Usage:
    requests: int = 0
    agents: int = 0
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0

    def add(self, other: "Usage") -> None:
        self.requests += other.requests
        self.agents += other.agents
        self.input += other.input
        self.output += other.output
        self.cache_read += other.cache_read
        self.cache_write += other.cache_write

    @property
    def weighted(self) -> int:
        return round(
            WEIGHTS["input"] * self.input
            + WEIGHTS["output"] * self.output
            + WEIGHTS["cache_read"] * self.cache_read
            + WEIGHTS["cache_write"] * self.cache_write
        )


@dataclass
class Request:
    ts: datetime
    usage: Usage


@dataclass
class Interval:
    start: datetime | None  # exclusive; None = open
    end: datetime | None  # inclusive; None = open
    label: str
    usage: Usage = field(default_factory=Usage)

    def contains(self, ts: datetime) -> bool:
        return (self.start is None or ts > self.start) and (self.end is None or ts <= self.end)


def parse_ts(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_timing_log(progress_md: Path) -> list[Interval]:
    """Build step intervals from the `## Step timing log` lines in progress.md."""
    entries: list[tuple[datetime, str, str]] = []
    for line in progress_md.read_text().splitlines():
        m = TIMING_LINE.match(line.strip())
        if m:
            entries.append((parse_ts(m.group(1)), m.group(2), (m.group(3) or "").strip()))
    if not entries:
        raise SystemExit(
            f"No '## Step timing log' entries found in {progress_md} — expected lines like "
            "'- 2026-07-20T09:14:03Z DONE etl-update'. Was this update run with timing enabled?"
        )
    entries.sort(key=lambda e: e[0])

    intervals: list[Interval] = []
    prev: datetime | None = None
    for ts, kind, label in entries:
        if kind == "START":
            intervals.append(Interval(prev, ts, "(before START)"))
        else:
            intervals.append(Interval(prev, ts, label or "(unlabeled step)"))
        prev = ts
    intervals.append(Interval(prev, None, "(after last logged step)"))
    return intervals


def encoded_project_dir(cwd: Path) -> Path:
    """Claude Code stores transcripts under ~/.claude/projects/<cwd with non-alphanumerics as '-'>."""
    encoded = re.sub(r"[^A-Za-z0-9]", "-", str(cwd.resolve()))
    return Path.home() / ".claude" / "projects" / encoded


def parse_transcript(path: Path, seen_requests: set[str]) -> list[Request]:
    """Extract one deduplicated Request per API call from a transcript JSONL file."""
    requests: list[Request] = []
    with path.open() as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            usage = (rec.get("message") or {}).get("usage")
            ts = rec.get("timestamp")
            if not usage or not ts:
                continue
            req_id = rec.get("requestId") or (rec.get("message") or {}).get("id") or rec.get("uuid")
            if req_id in seen_requests:
                continue
            seen_requests.add(req_id)
            requests.append(
                Request(
                    ts=parse_ts(ts),
                    usage=Usage(
                        requests=1,
                        input=usage.get("input_tokens") or 0,
                        output=usage.get("output_tokens") or 0,
                        cache_read=usage.get("cache_read_input_tokens") or 0,
                        cache_write=usage.get("cache_creation_input_tokens") or 0,
                    ),
                )
            )
    return requests


def discover_sessions(project_dir: Path, workbench_dir: Path) -> list[str]:
    """Session IDs of transcripts in project_dir that mention the workbench directory."""
    needle = str(workbench_dir).rstrip("/")
    short_needle = f"workbench/{workbench_dir.name}"
    sessions = []
    for path in sorted(project_dir.glob("*.jsonl")):
        text = path.read_text(errors="replace")
        if needle in text or short_needle in text:
            sessions.append(path.stem)
    return sessions


def assign(intervals: list[Interval], ts: datetime) -> Interval:
    for interval in intervals:
        if interval.contains(ts):
            return interval
    return intervals[-1]


def fmt_duration(interval: Interval) -> str:
    if interval.start is None or interval.end is None:
        return "—"
    secs = int((interval.end - interval.start).total_seconds())
    return f"{secs // 3600}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def build_report(intervals: list[Interval], sessions: list[str], project_dir: Path) -> str:
    lines = [
        "# Update cost report",
        "",
        f"Sessions: {', '.join(f'`{s}`' for s in sessions)}",
        "",
        "| Step | Wall time | Requests | Agents | Input | Output | Cache read | Cache write | Weighted* |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    total = Usage()
    for iv in intervals:
        if iv.usage.requests == 0 and iv.label.startswith("("):
            continue
        total.add(iv.usage)
        lines.append(
            f"| {iv.label} | {fmt_duration(iv)} | {iv.usage.requests:,} | {iv.usage.agents:,} "
            f"| {iv.usage.input:,} | {iv.usage.output:,} | {iv.usage.cache_read:,} "
            f"| {iv.usage.cache_write:,} | {iv.usage.weighted:,} |"
        )
    lines += [
        f"| **Total** | | **{total.requests:,}** | **{total.agents:,}** | **{total.input:,}** "
        f"| **{total.output:,}** | **{total.cache_read:,}** | **{total.cache_write:,}** "
        f"| **{total.weighted:,}** |",
        "",
        "\\* Weighted = input-equivalent tokens (output ×5, cache write ×1.25, cache read ×0.1) — "
        "a relative cost proxy, not USD.",
        "",
        "Caveats: wall time includes waiting for the user; main-session tokens are bucketed by "
        "timestamp so attribution at step boundaries is approximate; each subagent is attributed "
        f"to the step during which it started. Transcripts read from `{project_dir}`.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workbench_dir", type=Path, help="e.g. workbench/<short_name>")
    parser.add_argument("--session", action="append", default=[], help="session ID(s); default: auto-discover")
    parser.add_argument("--project-dir", type=Path, default=None, help="override the transcript project dir")
    parser.add_argument("--output", type=Path, default=None, help="default: <workbench_dir>/cost_report.md")
    args = parser.parse_args()

    progress_md = args.workbench_dir / "progress.md"
    if not progress_md.exists():
        raise SystemExit(f"{progress_md} not found — run from the repo root.")
    intervals = parse_timing_log(progress_md)

    project_dir = args.project_dir or encoded_project_dir(Path.cwd())
    if not project_dir.exists():
        raise SystemExit(f"Transcript project dir {project_dir} not found — pass --project-dir explicitly.")

    sessions = args.session or discover_sessions(project_dir, args.workbench_dir)
    if not sessions:
        raise SystemExit(
            f"No transcripts in {project_dir} mention {args.workbench_dir} — pass --session explicitly."
        )

    seen_requests: set[str] = set()
    for session in sessions:
        # Subagent transcripts first: their step attribution is more specific, and the global
        # requestId dedup then protects against transcript layouts that also inline them.
        for agent_file in sorted((project_dir / session).rglob("agent-*.jsonl")):
            agent_requests = parse_transcript(agent_file, seen_requests)
            if not agent_requests:
                continue
            agent_usage = Usage(agents=1)
            for req in agent_requests:
                agent_usage.add(req.usage)
            assign(intervals, min(req.ts for req in agent_requests)).usage.add(agent_usage)

        main_file = project_dir / f"{session}.jsonl"
        if not main_file.exists():
            raise SystemExit(f"Transcript {main_file} not found.")
        for req in parse_transcript(main_file, seen_requests):
            assign(intervals, req.ts).usage.add(req.usage)

    output = args.output or args.workbench_dir / "cost_report.md"
    output.write_text(build_report(intervals, sessions, project_dir))
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
