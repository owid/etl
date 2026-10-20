"""Utilities for fetching and processing LXC staging server data."""

import os
import subprocess
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st
from structlog import get_logger

from apps.owidbot.cli import get_cloudflare_subdomain
from etl.config import OWIDBOT_ACCESS_TOKEN, get_container_name
from etl.http import session as http_session

log = get_logger()

# The staging API on lxc-manager-1 (ops: infra/staging_api). It lists, wakes, stops,
# restarts and destroys the staging servers with owid-lxc, and reports their LXC host's
# memory. Override STAGING_API_URL to point the dashboard at another instance.
STAGING_API_URL = os.environ.get("STAGING_API_URL", "http://lxc-manager-1.tail6e23.ts.net:8090")

# Auto-stop lifecycle, mirrored from the ops reaper cron
# (ops/templates/lxc-manager/stop_staging_containers.py). A running container is stopped (idled,
# data preserved) once its most recent sign of activity — max(latest commit, last deliberate
# *wake*) — is older than STOP_AFTER_DAYS. We recompute that here so the dashboard's "Auto-stop"
# column agrees with what the cron will actually do. Keep these in sync with that script.
STOP_AFTER_DAYS = 14
# Container the reaper skips explicitly — never auto-stopped.
PERSISTENT_CONTAINERS = {"staging-site-master"}
# Sentinel the LXC `info` command returns when a repo's commit can't be read transiently. The cron
# refuses to stop on this (the other repo might have recent work it couldn't see), so neither do we.
COMMIT_LOOKUP_FAILED = "Unable to retrieve"


def _call_api(method: str, path: str, timeout: float) -> tuple[Any, str | None]:
    """Call the staging API. Returns (JSON body, None), or (None, an error message for the UI)."""
    url = f"{STAGING_API_URL}{path}"
    try:
        resp = http_session.request(method, url, timeout=timeout)
    except requests.RequestException as e:
        log.error("Staging API unreachable", url=url, error=str(e))
        return None, f"Staging API unreachable ({url}): {e}"
    if not resp.ok:
        try:
            detail = resp.json().get("detail", resp.text)
        except ValueError:
            detail = resp.text
        log.error("Staging API error", url=url, status=resp.status_code, detail=detail)
        return None, f"Staging API returned {resp.status_code}: {detail}"
    return resp.json(), None


@st.cache_data(ttl=300, show_spinner=False, refresh_mode="background")  # 5 min; stale value served while refreshing
def fetch_host_memory_stats() -> tuple[dict | None, str | None]:
    """Memory and swap of the LXC host the staging servers live on, in GB and percent."""
    return _call_api("GET", "/host/memory", timeout=20)


@st.cache_data(ttl=300, show_spinner=False, refresh_mode="background")  # 5 min; stale value served while refreshing
def fetch_lxc_servers_data() -> tuple[pd.DataFrame | None, str | None]:
    """Every staging server, as `owid-lxc info --json` reports it, processed for display."""
    # the API reads each container's git commits, which takes a while with ~200 of them
    data, error = _call_api("GET", "/servers", timeout=120)
    if error:
        return None, error

    df = pd.DataFrame(data)
    if df.empty:
        return df, None

    df = _process_server_data(df)
    log.info("Fetched staging server data", server_count=len(df))
    return df, None


def _process_server_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process and enrich the raw LXC server data.

    Args:
        df: Raw DataFrame from LXC command

    Returns:
        Processed DataFrame with additional computed columns
    """
    # Create a copy to avoid modifying the original
    df = df.copy()

    # Extract branch name from container name (remove 'staging-site-' prefix)
    df["branch"] = df["name"].str.replace("staging-site-", "", regex=False)

    # Parse memory information (only "used" is shown in the table)
    df["memory_used_gb"] = df["memory"].apply(_extract_memory_used_gb)

    # Parse creation time
    df["created_parsed"] = pd.to_datetime(df["created"], errors="coerce")
    df["days_old"] = (datetime.now(timezone.utc) - df["created_parsed"]).dt.days

    # Parse commit timestamps
    df["etl_commit_parsed"] = pd.to_datetime(df["etl_last_commit"], errors="coerce")
    df["grapher_commit_parsed"] = pd.to_datetime(df["grapher_last_commit"], errors="coerce")

    # Last deliberate wake, stamped on the container by `owid-lxc start` (which is what the Wake
    # button runs). This is what the reaper counts alongside commits, so a freshly-woken server
    # isn't stopped the same night.
    #
    # Not LXD's own `last_used_at`: that is bumped by *any* start, and the weekly gaia-1 reboot
    # restores every running container, so it read as "restarted last Monday" fleet-wide and the
    # reaper could never reach the 14-day threshold for anything.
    if "woken_at" not in df.columns:
        df["woken_at"] = None
    # format="ISO8601" handles both the plain "…Z" stamp we write and any fractional-second
    # variant (and None) without the per-element dateutil fallback that pandas warns about.
    df["woken_parsed"] = pd.to_datetime(df["woken_at"], errors="coerce", format="ISO8601")

    # Calculate days since last commit
    df["etl_days_old"] = _calculate_days_since_commit(df["etl_commit_parsed"])
    df["grapher_days_old"] = _calculate_days_since_commit(df["grapher_commit_parsed"])

    # Add status indicators. "Stopped" containers are idle: their data is preserved and
    # they can be woken with `owid-lxc start`, so we surface them as "Idle" to the team.
    df["status_indicator"] = df["status"].map(
        {
            "Running": "🟢",
            "Stopped": "⏸️",
        }
    )
    df["status_label"] = df["status"].map(
        {
            "Running": "Running",
            "Stopped": "Idle",
        }
    )

    # Build a single "last commit" column based on origin (used in the server list)
    df["unified_commit"] = df.apply(_get_unified_commit_info, axis=1)

    # Neutral, alarm-free relative time of the latest commit (informational only — the lifecycle
    # signal lives in the auto_stop column below, not here).
    df["last_commit_display"] = df.apply(_format_last_commit_neutral, axis=1)

    # Honest lifecycle column: when (and why) the reaper will stop this server.
    now = datetime.now(timezone.utc)
    df["auto_stop"] = df.apply(lambda row: _compute_auto_stop(row, now), axis=1)

    # Sort by status (running first) then by creation date (newest first)
    df = df.sort_values(
        ["status", "created_parsed"],
        ascending=[
            True,
            False,
        ],  # True for status to get Running before Stopped, False for creation date (newest first)
        na_position="last",
    ).reset_index(drop=True)

    return df


def _extract_memory_used_gb(memory_info: dict) -> float | None:
    """Extract used memory in GB from memory info dict."""
    try:
        if isinstance(memory_info, dict) and "used_mb" in memory_info:
            return round(memory_info["used_mb"] / 1024, 2)
        return None
    except (TypeError, KeyError):
        return None


def _calculate_days_since_commit(commit_series: pd.Series) -> pd.Series:
    """Calculate days since commit for a series of commit timestamps."""
    now = datetime.now(timezone.utc)

    # Calculate the difference in days, handling timezone-aware and naive datetimes
    def calc_days(dt):
        if pd.isna(dt):
            return None
        # Make datetime timezone-aware if it's naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = now - dt
        return max(0, diff.days)  # Ensure we don't get negative days

    return commit_series.apply(calc_days)  # ty: ignore


def get_server_summary_stats(df: pd.DataFrame, host_memory_stats: dict | None = None) -> dict[str, Any]:
    """
    Generate summary statistics for the servers.

    Args:
        df: Processed server DataFrame
        host_memory_stats: Host memory statistics from fetch_host_memory_stats

    Returns:
        Dictionary with summary statistics
    """
    if df.empty:
        return {
            "total_servers": 0,
            "running_servers": 0,
            "stopped_servers": 0,
            "host_memory_stats": host_memory_stats,
        }

    return {
        "total_servers": len(df),
        "running_servers": len(df[df["status"] == "Running"]),
        "stopped_servers": len(df[df["status"] == "Stopped"]),
        "host_memory_stats": host_memory_stats,
    }


def format_commit_info(commit_timestamp: str, days_old: int | None) -> str:
    """
    Format commit information for display.

    Args:
        commit_timestamp: Raw commit timestamp string
        days_old: Days since commit

    Returns:
        Formatted string for display
    """
    # Handle special cases first
    if pd.isna(commit_timestamp) or commit_timestamp in [
        "Container not running",
        "Unable to retrieve",
        "No git repository found",
    ]:
        if commit_timestamp == "Unable to retrieve":
            return "❓ Unable to retrieve"
        elif commit_timestamp == "No git repository found":
            return "❌ No git repo"
        else:
            return "❌ " + str(commit_timestamp)

    if days_old is None:
        return "❓ Unknown"

    if days_old == 0:
        return "✅ Today"
    elif days_old == 1:
        return "✅ 1 day ago"
    elif days_old <= 7:
        return f"⚠️ {int(days_old)} days ago"
    else:
        return f"❌ {int(days_old)} days ago"


def _get_unified_commit_info(row: pd.Series) -> str:
    """Create a single human-readable "last commit" string based on the server origin."""
    origin = (row.get("origin") or "").lower()

    if origin == "etl":
        # Use ETL commit for ETL-only servers
        return format_commit_info(row["etl_last_commit"], row["etl_days_old"])
    elif origin == "owid-grapher":
        # Use Grapher commit for Grapher-only servers
        return format_commit_info(row["grapher_last_commit"], row["grapher_days_old"])
    elif "owid-grapher" in origin and "etl" in origin:
        # For mixed origins (e.g., "owid-grapher,etl"), use the latest commit
        etl_date = row["etl_commit_parsed"]
        grapher_date = row["grapher_commit_parsed"]

        # Handle NaT/None values
        if pd.isna(etl_date) and pd.isna(grapher_date):
            return "❓ No commits found"
        elif pd.isna(etl_date):
            return format_commit_info(row["grapher_last_commit"], row["grapher_days_old"])
        elif pd.isna(grapher_date):
            return format_commit_info(row["etl_last_commit"], row["etl_days_old"])
        else:
            # Compare dates and use the latest
            if etl_date >= grapher_date:
                return format_commit_info(row["etl_last_commit"], row["etl_days_old"])
            else:
                return format_commit_info(row["grapher_last_commit"], row["grapher_days_old"])
    else:
        # Default fallback - try ETL first, then Grapher
        etl_commit = row["etl_last_commit"]
        if not pd.isna(etl_commit) and etl_commit not in [
            "Container not running",
            "Unable to retrieve",
            "No git repository found",
        ]:
            return format_commit_info(row["etl_last_commit"], row["etl_days_old"])
        else:
            return format_commit_info(row["grapher_last_commit"], row["grapher_days_old"])


def _as_utc(ts) -> pd.Timestamp:
    """Normalize a parsed timestamp to tz-aware UTC so commit and start times are comparable.

    LXC emits trailing-Z timestamps; pandas parses some as tz-aware and (when the column is mixed)
    others as naive. Localize naive values to UTC rather than guessing the host's offset.
    """
    ts = pd.Timestamp(ts)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _latest_commit_date(row: pd.Series) -> pd.Timestamp | None:
    """Newest of the ETL / Grapher commit timestamps (None if neither is a real date)."""
    dates = [_as_utc(d) for d in (row["etl_commit_parsed"], row["grapher_commit_parsed"]) if pd.notna(d)]
    return max(dates) if dates else None


def _commit_lookup_failed(row: pd.Series) -> bool:
    """True if either repo's commit came back as a transient failure (vs. a legitimate absence)."""
    return COMMIT_LOOKUP_FAILED in (row.get("etl_last_commit"), row.get("grapher_last_commit"))


def _format_last_commit_neutral(row: pd.Series) -> str:
    """Relative age of the latest commit, with no ✅/⚠️/❌ alarm — this column is reference info,
    not a lifecycle signal (that's `auto_stop`)."""
    latest = _latest_commit_date(row)
    if pd.isna(latest):
        return "unknown" if _commit_lookup_failed(row) else "no commits"
    days = max(0, (datetime.now(timezone.utc) - latest).days)
    if days == 0:
        return "today"
    if days == 1:
        return "1 day ago"
    return f"{days} days ago"


def _compute_auto_stop(row: pd.Series, now: datetime) -> str:
    """When (and why) the reaper will stop this server, mirroring stop_staging_containers.py.

    The cron stops a running container once max(latest commit, last wake) is older than
    STOP_AFTER_DAYS. We surface the countdown plus the reason it's still alive, so an old commit
    next to a recent wake no longer reads as "should be dead".
    """
    # Idle servers are already stopped — the action is Wake, not a countdown.
    if row["status"] != "Running":
        return "⏸️ idle · wake anytime"
    # Persistent container is never auto-stopped.
    if row["name"] in PERSISTENT_CONTAINERS:
        return "♾️ persistent"
    # The cron skips on a transient commit-lookup failure; don't imply imminent death.
    if _commit_lookup_failed(row):
        return "❓ won't auto-stop (commit unreadable)"

    commit_date = _latest_commit_date(row)
    woken = _as_utc(row["woken_parsed"]) if pd.notna(row["woken_parsed"]) else pd.NaT
    candidates = [d for d in (commit_date, woken) if pd.notna(d)]
    # No commit date at all → cron can't judge and skips it; mirror that rather than guess.
    if pd.isna(commit_date) or not candidates:
        return "❓ won't auto-stop (no commit date)"

    last_activity = max(candidates)
    days_left = STOP_AFTER_DAYS - (now - last_activity).days

    # Which signal is keeping it alive — a recent wake or a recent commit?
    if pd.notna(woken) and woken >= commit_date:
        reason = f"woken {woken.strftime('%b %-d')}"
    else:
        reason = f"commit {commit_date.strftime('%b %-d')}"

    if days_left <= 0:
        return f"🔴 stops tonight · {reason}"
    if days_left <= 4:
        return f"🟡 in {days_left}d · {reason}"
    return f"🟢 in {days_left}d · {reason}"


def reset_mysql_database(server_name: str) -> tuple[bool, str]:
    """
    Reset the MySQL database for a staging server by running 'make refresh' in owid-grapher
    and pruning associated R2 files.

    Args:
        server_name: Full server name (e.g., staging-site-branch-name)

    Returns:
        Tuple of (success, message)
    """
    r2_error_msg = None

    try:
        # First, prune R2 files for this server
        log.info("Pruning R2 files", server=server_name)
        r2_cmd = f"rclone purge r2:owid-api-staging/{server_name} --fast-list --transfers 64 --checkers 64"
        log.info("Executing R2 purge command", command=r2_cmd, server=server_name)

        r2_result = subprocess.run(
            r2_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout for R2 purge
        )

        if r2_result.returncode != 0:
            # Store error but continue with MySQL reset - R2 purge failure shouldn't block DB reset
            r2_error_msg = f"R2 purge failed: {r2_result.stderr}"
            log.warning(
                "R2 purge failed but continuing with MySQL reset",
                error=r2_result.stderr,
                server=server_name,
                stdout=r2_result.stdout,
            )
        else:
            log.info("R2 files successfully purged", server=server_name)

        # SSH into the server and run commands in parallel
        # Disable host key checking for staging servers since they're ephemeral
        mysql_cmd = f"ssh -o StrictHostKeyChecking=no owid@{server_name} 'cd owid-grapher && make refresh'"
        rsync_cmd = f"ssh -o StrictHostKeyChecking=no owid@{server_name} 'rsync -az --stats -e \"ssh -o StrictHostKeyChecking=no\" owid@staging-site-master:/home/owid/etl/data/ /home/owid/etl/data/'"

        log.info("Executing MySQL reset and rsync in parallel", server=server_name)

        # Start both processes in parallel
        processes = {
            "mysql": subprocess.Popen(mysql_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True),
            "rsync": subprocess.Popen(rsync_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True),
        }

        # Wait for both processes with timeout handling
        results = {}
        timeout_seconds = 1800  # 30 minutes

        for name, process in processes.items():
            try:
                stdout, stderr = process.communicate(timeout=timeout_seconds)
                results[name] = {"returncode": process.returncode, "stdout": stdout, "stderr": stderr}
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                log.warning(f"{name} timed out", server=server_name)
                results[name] = {
                    "returncode": -1,
                    "stdout": stdout,
                    "stderr": f"{name} timed out after {timeout_seconds // 60} minutes",
                }
                # If MySQL times out, kill other processes and fail
                if name == "mysql":
                    for other_name, other_process in processes.items():
                        if other_name != name:
                            other_process.kill()
                    return False, f"MySQL reset timed out after {timeout_seconds // 60} minutes"

        # Check MySQL result (critical)
        mysql_result = results["mysql"]
        if mysql_result["returncode"] != 0:
            error_msg = f"MySQL reset failed with return code {mysql_result['returncode']}: {mysql_result['stderr']}"
            log.error("MySQL reset failed", error=error_msg, server=server_name, stdout=mysql_result["stdout"])
            return False, error_msg

        # Check rsync result
        rsync_result = results["rsync"]
        errors = []

        if rsync_result["returncode"] != 0:
            log.error(
                "rsync failed",
                error=rsync_result["stderr"],
                server=server_name,
                stdout=rsync_result["stdout"],
            )
            errors.append(f"rsync from master failed: {rsync_result['stderr']}")
        else:
            log.info("ETL data successfully synced from master", server=server_name)

        # Add R2 error if it occurred
        if r2_error_msg:
            errors.append(r2_error_msg)

        # If there are errors, return failure
        if errors:
            error_msg = "; ".join(errors)
            return False, f"MySQL refresh completed but errors occurred: {error_msg}"

        log.info("MySQL reset completed successfully", server=server_name)
        return True, f"MySQL database successfully refreshed and ETL data synced for {server_name}"

    except subprocess.TimeoutExpired:
        error_msg = "Unexpected timeout in MySQL reset"
        log.error("MySQL reset timeout", server=server_name)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error during MySQL reset: {str(e)}"
        log.error("MySQL reset error", error=error_msg, server=server_name)
        return False, error_msg


def _server_action(method: str, server_name: str, action: str, timeout: float) -> tuple[Any, str | None]:
    path = f"/servers/{quote(server_name, safe='')}" + (f"/{action}" if method == "POST" else "")
    log.info(f"Staging API: {action}", server=server_name)
    return _call_api(method, path, timeout=timeout)


def _woken(server_name: str, result: dict) -> tuple[bool, str]:
    # The API checks that the container rejoined the tailnet: the Tailscale login on wake can
    # fail silently, leaving it Running but unreachable by hostname (admin/SSH won't work).
    if result["on_tailnet"]:
        return True, f"Server {server_name} is awake and on the tailnet."
    return False, (
        f"Server {server_name} started, but it is NOT on the Tailscale network, so it's "
        "unreachable by hostname (admin/SSH won't work). Tailscale failed to re-authenticate on "
        "wake. Try waking it again; if it keeps failing, Tailscale must be re-authenticated on the host."
    )


def start_server(server_name: str) -> tuple[bool, str]:
    """
    Wake up an idle (stopped) staging server.

    The container's data is preserved while it's stopped, so starting it brings the
    server back in seconds — no commit or rebuild needed. This replaces the obscure
    "push an empty commit" trick for idle servers.
    """
    # starting also logs the container in to Tailscale and checks it rejoined, so allow time
    result, error = _server_action("POST", server_name, "start", timeout=300)
    if error:
        return False, error
    return _woken(server_name, result)


def stop_server(server_name: str) -> tuple[bool, str]:
    """
    Put a running staging server to sleep (make it idle).

    The API takes a ``pre-stop`` snapshot first, as the ops cron does, and doesn't stop the
    server if the snapshot fails. The data is preserved and the server can be woken later
    with :func:`start_server`.
    """
    _, error = _server_action("POST", server_name, "stop", timeout=260)
    if error:
        return False, error
    return True, f"Server {server_name} stop completed successfully"


def restart_server(server_name: str) -> tuple[bool, str]:
    """Restart a running staging server (stop, then start)."""
    result, error = _server_action("POST", server_name, "restart", timeout=420)
    if error:
        return False, error
    return _woken(server_name, result)


def destroy_server(server_name: str) -> tuple[bool, str]:
    """Destroy a staging server completely. Locked servers are refused."""
    _, error = _server_action("DELETE", server_name, "destroy", timeout=150)
    if error:
        return False, error
    return True, f"Server {server_name} successfully destroyed"


def build_quick_links(branch: str) -> dict[str, str]:
    """
    Build the quick-access links for a staging server.

    These mirror the links that owidbot posts on every ETL PR (see
    ``apps/owidbot/cli.py:create_comment_body``), so they can be accessed from the
    dashboard without hunting through GitHub.

    Args:
        branch: Branch name (without the ``staging-site-`` prefix)

    Returns:
        Ordered dict of {label: url}
    """
    container_name = get_container_name(branch)
    cloudflare_subdomain = get_cloudflare_subdomain(branch)

    return {
        "Site Dev": f"http://{container_name}/",
        "Site Preview": f"https://{cloudflare_subdomain}.owid.pages.dev/",
        "Admin": f"http://{container_name}/admin",
        "Wizard": f"http://{container_name}/etl/wizard/",
        "Chart Diff": f"http://{container_name}/etl/wizard/chart-diff",
    }


def _fetch_prs(repo_name: str, params: str, headers: dict, max_pages: int) -> list[dict]:
    """Fetch up to ``max_pages`` pages of PRs from a repo's pulls endpoint."""
    url: str | None = f"https://api.github.com/repos/owid/{repo_name}/pulls?{params}"
    prs: list[dict] = []
    pages = 0
    while url and pages < max_pages:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        prs.extend(response.json())
        url = response.links.get("next", {}).get("url")
        pages += 1
    return prs


@st.cache_data(ttl=300, show_spinner=False, refresh_mode="background")  # 5 min; stale value served while refreshing
def fetch_server_owners() -> dict[str, str]:
    """
    Map each staging server's container name to the GitHub login of its PR author.

    Staging servers don't record who created them, so we derive the owner from PRs on
    the etl and owid-grapher repos. We include both open PRs (full list) and the most
    recently-closed PRs, because a server survives for a few days after its PR is merged
    — so merged-but-not-yet-pruned servers still need an owner. Best-effort: returns an
    empty mapping if GitHub is unreachable or no token is configured, so the dashboard
    never breaks.

    Returns:
        Dict mapping container name (e.g. ``staging-site-my-branch``) to GitHub login.
    """
    owners: dict[str, str] = {}
    headers = {"Authorization": f"token {OWIDBOT_ACCESS_TOKEN}"} if OWIDBOT_ACCESS_TOKEN else {}

    for repo_name in ("etl", "owid-grapher"):
        try:
            # Open PRs first (their author wins on branch reuse), then recently-closed
            # PRs to cover servers whose PR merged within the prune window (~3 days).
            prs = _fetch_prs(repo_name, "state=open&per_page=100", headers, max_pages=5)
            prs += _fetch_prs(repo_name, "state=closed&sort=updated&direction=desc&per_page=100", headers, max_pages=2)
            for pr in prs:
                # Only OWID branches (exclude forks), and skip if author missing.
                if not pr["head"]["label"].startswith("owid:"):
                    continue
                login = (pr.get("user") or {}).get("login")
                if not login:
                    continue
                container_name = get_container_name(pr["head"]["ref"])
                # First occurrence wins (open PRs and most-recently-updated closed PRs first).
                owners.setdefault(container_name, login)
        except requests.RequestException as e:
            log.warning("Could not fetch PR owners from GitHub", repo=repo_name, error=str(e))

    return owners
