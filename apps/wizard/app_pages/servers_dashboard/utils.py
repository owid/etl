"""Utilities for fetching and processing LXC staging server data."""

import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st
from structlog import get_logger

log = get_logger()


@st.cache_data(ttl=300)  # Cache for 5 minutes to avoid hammering the server
def fetch_host_memory_stats(host: str = "gaia-1") -> Tuple[Optional[Dict], Optional[str]]:
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
        if len(lines) < 2:
            return None, "Invalid free command output"

        # Parse memory line (second line)
        mem_line = lines[1].split()
        if len(mem_line) < 3:
            return None, "Could not parse memory information"

        total_bytes = int(mem_line[1])
        used_bytes = int(mem_line[2])
        available_bytes = int(mem_line[6]) if len(mem_line) >= 7 else int(mem_line[3])

        memory_stats = {
            "total_gb": round(total_bytes / (1024**3), 1),
            "used_gb": round(used_bytes / (1024**3), 1),
            "available_gb": round(available_bytes / (1024**3), 1),
            "usage_pct": round((used_bytes / total_bytes) * 100, 1),
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


@st.cache_data(ttl=300)  # Cache for 5 minutes to avoid hammering the server
def fetch_lxc_servers_data(host: str = "gaia-1") -> Tuple[Optional[pd.DataFrame], Optional[str]]:
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
            log.error("LXC command failed", error=error_msg, stdout=result.stdout)
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

    # Add status indicators
    df["status_indicator"] = df["status"].map(
        {
            "Running": "🟢",
            "Stopped": "🔴",
        }
    )

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


def _extract_memory_used_gb(memory_info: Dict) -> Optional[float]:
    """Extract used memory in GB from memory info dict."""
    try:
        if isinstance(memory_info, dict) and "used_mb" in memory_info:
            return round(memory_info["used_mb"] / 1024, 2)
        return None
    except (TypeError, KeyError):
        return None


def _extract_memory_total_gb(memory_info: Dict) -> Optional[float]:
    """Extract total memory in GB from memory info dict."""
    try:
        if isinstance(memory_info, dict) and "total_mb" in memory_info:
            return round(memory_info["total_mb"] / 1024, 2)
        return None
    except (TypeError, KeyError):
        return None


def _calculate_memory_usage_pct(row: pd.Series) -> Optional[float]:
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

    return commit_series.apply(calc_days)  # type: ignore


def get_server_summary_stats(df: pd.DataFrame, host_memory_stats: Optional[Dict] = None) -> Dict[str, Any]:
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
        "host_memory_usage_pct": host_memory_stats["usage_pct"] if host_memory_stats else None,
    }


def format_commit_info(commit_timestamp: str, days_old: Optional[int]) -> str:
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


def get_display_columns() -> Dict[str, str]:
    """
    Get the column mapping for display in the table.

    Returns:
        Dictionary mapping internal column names to display names
    """
    return {
        "status_indicator": "Status",
        "branch": "Branch",
        "memory_display": "Memory",
        "days_old": "Age (days)",
        "etl_last_commit": "ETL Commit",
        "grapher_last_commit": "Grapher Commit",
    }


def prepare_display_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the dataframe for display in Streamlit.

    Args:
        df: Processed server DataFrame

    Returns:
        DataFrame ready for display
    """
    if df.empty:
        return df

    display_df = df.copy()

    # Format commit information
    display_df["etl_last_commit"] = display_df.apply(
        lambda row: format_commit_info(row["etl_last_commit"], row["etl_days_old"]), axis=1
    )
    display_df["grapher_last_commit"] = display_df.apply(
        lambda row: format_commit_info(row["grapher_last_commit"], row["grapher_days_old"]), axis=1
    )

    # For progress bar, we need the actual GB values
    # The ProgressColumn will show the bar based on value/max_value ratio, but display the actual GB value
    display_df["memory_display"] = display_df["memory_used_gb"].fillna(0)

    # Round memory values for internal use
    display_df["memory_used_gb"] = display_df["memory_used_gb"].round(2)
    display_df["memory_usage_pct"] = display_df["memory_usage_pct"].round(1)

    # Select and order columns for display
    column_mapping = get_display_columns()
    display_columns = list(column_mapping.keys())

    display_df = display_df[display_columns].rename(columns=column_mapping)

    return display_df
