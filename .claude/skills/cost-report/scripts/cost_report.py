"""Retrospective per-step wall-time & token cost report for a past Claude Code task.

Joins a `## Step timing log` section in `<workbench_dir>/progress.md` — written
live during the task, or reconstructed after the fact from a session's own
narrative (see the `cost-report` skill for how) — with the Claude Code session
transcripts (`~/.claude/projects/<encoded-cwd>/`), and writes a markdown table
of duration and token usage per step. Not tied to any particular workflow: it
works for a `/update-dataset` run, a `/review-data-pr` run, or anything else
that has session transcripts and a reconstructable timeline.

Attribution model:
- Every individual API request (main-session and subagent alike) is bucketed
  into a step by its own timestamp — a step covers the interval between the
  previous log line and its own `DONE` line. A subagent's *count* is credited
  to the step during which it started, but its token usage is split by request
  if it happens to straddle a step boundary.
- Transcript lines are deduplicated by `requestId` — Claude Code writes one
  line per content block, all carrying the same usage object, so naive summing
  over-counts several-fold.
- Reports both wall time (raw calendar delta between step boundaries) and
  active time (sum of gaps between consecutive requests, each capped at
  ACTIVE_GAP_CAP_SECONDS) — wall time balloons when a step's boundary spans a
  multi-day pause between sessions; active time approximates real work instead.

Usage:
    .venv/bin/python .claude/skills/cost-report/scripts/cost_report.py workbench/<short_name> \
        [--session <session-id> ...] [--project-dir <dir>] [--output <path>]

With no --session, sessions are auto-discovered: every transcript in the
project dir whose text mentions the workbench directory.

Alongside the markdown, writes a `.json` sidecar with the same per-step figures
(`cost_report.md` -> `cost_report.json`) — `aggregate_cost_reports.py` reads
these to roll up cost across every instrumented update without re-parsing
transcripts or markdown tables.
"""

import argparse
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Relative price multipliers vs. one input token (stable across current Claude
# models: output 5x, cache write 1.25x, cache read 0.1x). They produce a single
# comparable "input-equivalent tokens" number per step — not a USD estimate.
WEIGHTS = {"input": 1.0, "output": 5.0, "cache_write": 1.25, "cache_read": 0.1}

# A gap between consecutive requests longer than this is presumed idle (the
# human stepped away, or a multi-day pause between sessions) rather than real
# work, and is capped down to this value when summing "active time" per step.
ACTIVE_GAP_CAP_SECONDS = 300

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
    request_times: list[datetime] = field(default_factory=list)

    def contains(self, ts: datetime) -> bool:
        return (self.start is None or ts > self.start) and (self.end is None or ts <= self.end)

    def active_time(self) -> timedelta:
        """Sum of gaps between consecutive requests, each capped at ACTIVE_GAP_CAP_SECONDS."""
        times = sorted(self.request_times)
        cap = timedelta(seconds=ACTIVE_GAP_CAP_SECONDS)
        total = timedelta()
        for prev, curr in zip(times, times[1:]):
            total += min(curr - prev, cap)
        return total


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


def fmt_timedelta(td: timedelta) -> str:
    secs = int(td.total_seconds())
    return f"{secs // 3600}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def fmt_wall_time(interval: Interval) -> str:
    if interval.start is None or interval.end is None:
        return "—"
    return fmt_timedelta(interval.end - interval.start)


def is_bookend(label: str) -> bool:
    """True for the synthetic '(before START)' / '(after last logged step)' intervals.

    These cover requests outside the logged workflow window (e.g. earlier unrelated work in a
    reused session, or later work in a session resumed after the update finished) and must never
    be folded into the report — otherwise cost_report.md can overstate the update's cost with
    unrelated activity, and re-running from a continued session makes the total grow indefinitely.
    """
    return label.startswith("(")


def excluded_bookend_activity(intervals: list[Interval]) -> list[Interval]:
    """Bookend intervals that captured real requests — worth a visible warning, not a silent drop."""
    return [iv for iv in intervals if is_bookend(iv.label) and iv.usage.requests > 0]


def usage_dict(usage: Usage) -> dict:
    return {
        "requests": usage.requests,
        "agents": usage.agents,
        "input": usage.input,
        "output": usage.output,
        "cache_read": usage.cache_read,
        "cache_write": usage.cache_write,
        "weighted": usage.weighted,
    }


def build_report_dict(intervals: list[Interval], sessions: list[str], workbench_dir: Path) -> dict:
    """Machine-readable twin of build_report(), for aggregate_cost_reports.py to consume."""
    steps = []
    total = Usage()
    total_active = timedelta()
    for iv in intervals:
        if is_bookend(iv.label):
            continue
        total.add(iv.usage)
        active = iv.active_time()
        total_active += active
        steps.append(
            {
                "label": iv.label,
                "wall_seconds": None if (iv.start is None or iv.end is None) else int((iv.end - iv.start).total_seconds()),
                "active_seconds": int(active.total_seconds()),
                **usage_dict(iv.usage),
            }
        )
    return {
        "workbench_dir": str(workbench_dir),
        "sessions": sessions,
        "steps": steps,
        "total": {"active_seconds": int(total_active.total_seconds()), **usage_dict(total)},
        "excluded_bookend_requests": {
            iv.label: iv.usage.requests for iv in excluded_bookend_activity(intervals)
        },
    }


def build_report(intervals: list[Interval], sessions: list[str], project_dir: Path, excluded: list[Interval]) -> str:
    lines = [
        "# Update cost report",
        "",
        f"Sessions: {', '.join(f'`{s}`' for s in sessions)}",
        "",
        "| Step | Wall time | Active time | Requests | Agents | Input | Output | Cache read | Cache write | Weighted* |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    total = Usage()
    total_active = timedelta()
    for iv in intervals:
        if is_bookend(iv.label):
            continue
        total.add(iv.usage)
        active = iv.active_time()
        total_active += active
        lines.append(
            f"| {iv.label} | {fmt_wall_time(iv)} | {fmt_timedelta(active)} | {iv.usage.requests:,} "
            f"| {iv.usage.agents:,} | {iv.usage.input:,} | {iv.usage.output:,} | {iv.usage.cache_read:,} "
            f"| {iv.usage.cache_write:,} | {iv.usage.weighted:,} |"
        )
    lines += [
        f"| **Total** | | {fmt_timedelta(total_active)} | **{total.requests:,}** | **{total.agents:,}** "
        f"| **{total.input:,}** | **{total.output:,}** | **{total.cache_read:,}** "
        f"| **{total.cache_write:,}** | **{total.weighted:,}** |",
        "",
        "\\* Weighted = input-equivalent tokens (output ×5, cache write ×1.25, cache read ×0.1) — "
        "a relative cost proxy, not USD.",
        "",
        f"Active time sums gaps between consecutive requests, each capped at {ACTIVE_GAP_CAP_SECONDS // 60} "
        "minutes, to approximate real work and exclude idle waiting — it undercounts a step with a "
        "genuinely long single operation (e.g. a multi-minute ETL run with no LLM requests in between) "
        "and its Total can therefore differ from summing per-step wall time.",
        "",
        "Caveats: wall time is the raw calendar delta between step boundaries, so it balloons when a "
        "step's boundary spans a multi-day pause between sessions — active time is the more reliable "
        "cost signal in that case. Token attribution is per-request by timestamp; a subagent's request "
        "count is credited to the step during which it started, but its own token usage is still split "
        f"by request if it straddles a step boundary. Transcripts read from `{project_dir}`.",
        "",
    ]
    if excluded:
        lines += [
            "**Excluded from this report:** requests outside the logged workflow window "
            "(before the `START` line or after the last `DONE` line) are never counted, since they "
            "belong to unrelated work — e.g. earlier activity in a reused session, or later activity "
            "in a session resumed after the update finished. This run found:",
            "",
        ]
        for iv in excluded:
            lines.append(f"- {iv.usage.requests:,} request(s) in `{iv.label}`")
        lines.append("")
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
            # Credit the agent-count to whichever step it started in, but split its token usage
            # (and feed its own request timestamps into "active time") per request, in case a
            # long-running subagent happens to straddle a step boundary.
            assign(intervals, min(req.ts for req in agent_requests)).usage.agents += 1
            for req in agent_requests:
                iv = assign(intervals, req.ts)
                iv.usage.add(req.usage)
                iv.request_times.append(req.ts)

        main_file = project_dir / f"{session}.jsonl"
        if not main_file.exists():
            raise SystemExit(f"Transcript {main_file} not found.")
        for req in parse_transcript(main_file, seen_requests):
            iv = assign(intervals, req.ts)
            iv.usage.add(req.usage)
            iv.request_times.append(req.ts)

    excluded = excluded_bookend_activity(intervals)
    for iv in excluded:
        print(f"Warning: excluded {iv.usage.requests} request(s) in '{iv.label}' (outside the logged workflow window).")

    output = args.output or args.workbench_dir / "cost_report.md"
    output.write_text(build_report(intervals, sessions, project_dir, excluded))
    json_output = output.with_suffix(".json")
    json_output.write_text(json.dumps(build_report_dict(intervals, sessions, args.workbench_dir), indent=2) + "\n")
    print(f"Wrote {output}")
    print(f"Wrote {json_output}")


if __name__ == "__main__":
    main()
