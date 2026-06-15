"""Utilities for fetching and processing LXC staging server data."""

import json
import subprocess
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
import streamlit as st
from structlog import get_logger

from apps.owidbot.cli import get_cloudflare_subdomain
from etl.config import OWIDBOT_ACCESS_TOKEN, get_container_name

log = get_logger()

# LXC host that staging servers live on. The fetch/destroy/stop/start commands all
# target this host (matching ops/templates/lxc-manager/*), so keep it in one place.
LXC_HOST = "gaia-1"


@st.cache_data(ttl=300, show_spinner=False)  # Cache for 5 minutes to avoid hammering the server
def fetch_host_memory_stats(host: str = "gaia-1") -> tuple[dict | None, str | None]:
    """
    Fetch host memory statistics from the LXC host.

    Args:
        host: LXC host to query (default: gaia-1)

    Returns:
        Tuple of (memory stats dict, error message if any)
    """
    try:
        cmd = f"ssh owid@{host} 'free -b'"  # Use bytes for precise calculation
        log.info("Fetching host memory stats", command=cmd, host=host)

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            error_msg = f"SSH command failed with return code {result.returncode}: {result.stderr}"
            log.error("Host memory command failed", error=error_msg)
            return None, error_msg

        # Parse free command output
        lines = result.stdout.strip().split("\n")
        if len(lines) < 3:
            return None, "Invalid free command output - missing swap line"

        # Parse memory line (second line)
        mem_line = lines[1].split()
        if len(mem_line) < 3:
            return None, "Could not parse memory information"

        # Parse swap line (third line)
        swap_line = lines[2].split()
        if len(swap_line) < 3:
            return None, "Could not parse swap information"

        # Memory calculations
        total_bytes = int(mem_line[1])
        # used_bytes = int(mem_line[2])
        available_bytes = int(mem_line[6]) if len(mem_line) >= 7 else int(mem_line[3])

        # Calculate "actually used" memory (total - available)
        actually_used_bytes = total_bytes - available_bytes

        # Swap calculations
        swap_total_bytes = int(swap_line[1])
        swap_used_bytes = int(swap_line[2])

        memory_stats = {
            "total_gb": round(total_bytes / (1024**3), 1),
            "used_gb": round(actually_used_bytes / (1024**3), 1),
            "available_gb": round(available_bytes / (1024**3), 1),
            "usage_pct": round((actually_used_bytes / total_bytes) * 100, 1),
            "swap_total_gb": round(swap_total_bytes / (1024**3), 1),
            "swap_used_gb": round(swap_used_bytes / (1024**3), 1),
            "swap_usage_pct": round((swap_used_bytes / swap_total_bytes) * 100, 1) if swap_total_bytes > 0 else 0,
        }

        log.info("Successfully fetched host memory stats", stats=memory_stats, host=host)
        return memory_stats, None

    except subprocess.TimeoutExpired:
        error_msg = "SSH command timed out after 10 seconds"
        log.error("Host memory command timeout", host=host)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        log.error("Unexpected error fetching host memory", error=error_msg, host=host)
        return None, error_msg


@st.cache_data(ttl=300, show_spinner=False)  # Cache for 5 minutes to avoid hammering the server
def fetch_lxc_servers_data(host: str = "gaia-1") -> tuple[pd.DataFrame | None, str | None]:
    """
    Fetch LXC server information from the specified host.

    Args:
        host: LXC host to query (default: gaia-1)

    Returns:
        Tuple of (DataFrame with server data, error message if any)
    """
    try:
        # Execute the LXC command to get server info
        cmd = f"LXC_HOST={host} owid-lxc info $(LXC_HOST={host} owid-lxc list) --json"
        log.info("Fetching LXC server data", command=cmd, host=host)

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,  # 30 second timeout
        )

        if result.returncode != 0:
            error_msg = f"Command failed with return code {result.returncode}: {result.stderr}"
            log.error("LXC command failed", cmd=cmd, error=error_msg, stdout=result.stdout)
            return None, error_msg

        # Parse JSON response
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {e}"
            log.error("JSON parsing failed", error=error_msg, stdout_preview=result.stdout[:200])
            return None, error_msg

        # Convert to DataFrame
        df = pd.DataFrame(data)

        if df.empty:
            return df, None

        # Process and enrich the data
        df = _process_server_data(df)

        log.info("Successfully fetched LXC server data", server_count=len(df), host=host)
        return df, None

    except subprocess.TimeoutExpired:
        error_msg = "Command timed out after 30 seconds"
        log.error("LXC command timeout", host=host)
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        log.error("Unexpected error fetching LXC data", error=error_msg, host=host)
        return None, error_msg


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

    # Parse memory information
    df["memory_used_gb"] = df["memory"].apply(_extract_memory_used_gb)
    df["memory_total_gb"] = df["memory"].apply(_extract_memory_total_gb)
    df["memory_usage_pct"] = df.apply(_calculate_memory_usage_pct, axis=1)

    # Parse creation time
    df["created_parsed"] = pd.to_datetime(df["created"], errors="coerce")
    df["days_old"] = (datetime.now(timezone.utc) - df["created_parsed"]).dt.days

    # Parse commit timestamps
    df["etl_commit_parsed"] = pd.to_datetime(df["etl_last_commit"], errors="coerce")
    df["grapher_commit_parsed"] = pd.to_datetime(df["grapher_last_commit"], errors="coerce")

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


def _extract_memory_total_gb(memory_info: dict) -> float | None:
    """Extract total memory in GB from memory info dict."""
    try:
        if isinstance(memory_info, dict) and "total_mb" in memory_info:
            return round(memory_info["total_mb"] / 1024, 2)
        return None
    except (TypeError, KeyError):
        return None


def _calculate_memory_usage_pct(row: pd.Series) -> float | None:
    """Calculate memory usage percentage."""
    try:
        if pd.isna(row["memory_used_gb"]) or pd.isna(row["memory_total_gb"]) or row["memory_total_gb"] == 0:
            return None
        return round((row["memory_used_gb"] / row["memory_total_gb"]) * 100, 1)
    except (TypeError, ZeroDivisionError):
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
            "total_memory_gb": 0,
            "host_memory_usage_pct": None,
        }

    running_servers = df[df["status"] == "Running"]

    return {
        "total_servers": len(df),
        "running_servers": len(running_servers),
        "stopped_servers": len(df[df["status"] == "Stopped"]),
        "total_memory_gb": running_servers["memory_used_gb"].sum() if not running_servers.empty else 0,
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


def start_server(server_name: str) -> tuple[bool, str]:
    """
    Wake up an idle (stopped) staging server with ``owid-lxc start``.

    The container's data is preserved while it's stopped, so starting it brings the
    server back in seconds — no commit or rebuild needed. This replaces the obscure
    "push an empty commit" trick for idle servers.

    Args:
        server_name: Full server name (e.g., staging-site-branch-name)

    Returns:
        Tuple of (success, message)
    """
    cmd = f"LXC_HOST={LXC_HOST} owid-lxc start {server_name}"
    # Starting also runs a tailscale login and a short readiness delay, so allow time.
    return _run_lxc_command(cmd, server_name, action="start", timeout=180)


def stop_server(server_name: str) -> tuple[bool, str]:
    """
    Put a running staging server to sleep (make it idle).

    Mirrors the ops cron (``stop_staging_containers.py``): take a ``pre-stop`` snapshot
    first so the state can be recovered, then stop the container. The data is preserved
    and the server can be woken later with :func:`start_server`.

    Args:
        server_name: Full server name (e.g., staging-site-branch-name)

    Returns:
        Tuple of (success, message)
    """
    # Snapshot first (best-effort safety net), then stop — same order as the cron.
    snapshot_ok, snapshot_msg = _run_lxc_command(
        f"LXC_HOST={LXC_HOST} owid-lxc snapshot {server_name} pre-stop",
        server_name,
        action="snapshot",
        timeout=120,
    )
    if not snapshot_ok:
        return False, f"Could not take pre-stop snapshot, aborting stop: {snapshot_msg}"

    return _run_lxc_command(
        f"LXC_HOST={LXC_HOST} owid-lxc stop {server_name}",
        server_name,
        action="stop",
        timeout=120,
    )


def restart_server(server_name: str) -> tuple[bool, str]:
    """
    Restart a running staging server (stop, then start).

    Args:
        server_name: Full server name (e.g., staging-site-branch-name)

    Returns:
        Tuple of (success, message)
    """
    stop_ok, stop_msg = _run_lxc_command(
        f"LXC_HOST={LXC_HOST} owid-lxc stop {server_name}",
        server_name,
        action="stop",
        timeout=120,
    )
    if not stop_ok:
        return False, f"Could not stop server before restart: {stop_msg}"

    return start_server(server_name)


def _run_lxc_command(cmd: str, server_name: str, action: str, timeout: int) -> tuple[bool, str]:
    """Run a simple ``owid-lxc`` action command and return (success, message)."""
    try:
        log.info(f"Running owid-lxc {action}", command=cmd, server=server_name)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)

        if result.returncode != 0:
            error_msg = f"`owid-lxc {action}` failed with return code {result.returncode}: {result.stderr.strip()}"
            log.error("owid-lxc command failed", error=error_msg, server=server_name, stdout=result.stdout)
            return False, error_msg

        log.info(f"owid-lxc {action} completed", server=server_name)
        return True, f"Server {server_name} {action} completed successfully"

    except subprocess.TimeoutExpired:
        error_msg = f"`owid-lxc {action}` timed out after {timeout} seconds"
        log.error("owid-lxc command timeout", server=server_name, action=action)
        return False, error_msg


def destroy_server(server_name: str) -> tuple[bool, str]:
    """
    Destroy a staging server completely.

    Args:
        server_name: Full server name (e.g., staging-site-branch-name)

    Returns:
        Tuple of (success, message)
    """
    try:
        # Command to destroy the LXC container
        cmd = f"LXC_HOST={LXC_HOST} owid-lxc destroy {server_name}"
        log.info("Destroying server", command=cmd, server=server_name)

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout for destruction
        )

        if result.returncode != 0:
            error_msg = f"Server destruction failed with return code {result.returncode}: {result.stderr}"
            log.error("Server destruction failed", error=error_msg, server=server_name)
            return False, error_msg

        success_msg = f"Server {server_name} successfully destroyed"
        log.info("Server destruction completed", server=server_name)
        return True, success_msg

    except subprocess.TimeoutExpired:
        error_msg = "Server destruction timed out after 2 minutes"
        log.error("Server destruction timeout", server=server_name)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error during server destruction: {str(e)}"
        log.error("Server destruction error", error=error_msg, server=server_name)
        return False, error_msg


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
        "Docs": f"http://{container_name}/etl/docs/",
    }


@st.cache_data(ttl=300, show_spinner=False)  # Cache for 5 minutes alongside the server data
def fetch_server_owners() -> dict[str, str]:
    """
    Map each staging server's container name to the GitHub login of its PR author.

    Staging servers don't record who created them, so we derive the owner from the
    open PRs on the etl and owid-grapher repos. Best-effort: returns an empty mapping
    if GitHub is unreachable or no token is configured, so the dashboard never breaks.

    Returns:
        Dict mapping container name (e.g. ``staging-site-my-branch``) to GitHub login.
    """
    owners: dict[str, str] = {}
    headers = {"Authorization": f"token {OWIDBOT_ACCESS_TOKEN}"} if OWIDBOT_ACCESS_TOKEN else {}

    for repo_name in ("etl", "owid-grapher"):
        url = f"https://api.github.com/repos/owid/{repo_name}/pulls?per_page=100&state=open"
        try:
            while url:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                for pr in response.json():
                    # Only OWID branches (exclude forks), and skip if author missing.
                    if not pr["head"]["label"].startswith("owid:"):
                        continue
                    login = (pr.get("user") or {}).get("login")
                    if not login:
                        continue
                    container_name = get_container_name(pr["head"]["ref"])
                    # First PR wins; both repos map to the same container name.
                    owners.setdefault(container_name, login)
                url = response.links.get("next", {}).get("url")
        except requests.RequestException as e:
            log.warning("Could not fetch PR owners from GitHub", repo=repo_name, error=str(e))

    return owners
