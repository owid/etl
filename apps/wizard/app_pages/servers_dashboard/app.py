"""Staging Servers Dashboard - Monitor all LXC staging servers with real-time metrics."""

import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.servers_dashboard.utils import (
    build_quick_links,
    destroy_server,
    fetch_host_memory_stats,
    fetch_lxc_servers_data,
    fetch_server_owners,
    get_server_summary_stats,
    reset_mysql_database,
    restart_server,
    start_server,
    stop_server,
)
from apps.wizard.utils.components import st_title_with_expert

# Configure page
st.set_page_config(
    page_title="Wizard: Staging Servers Dashboard",
    layout="wide",
    page_icon="🖥️",
    initial_sidebar_state="collapsed",
)

log = get_logger()


# Functions
def handle_delta_memory(used_pct, ss_key):
    last_value = st.session_state["servers_metric_metrics"].get(ss_key)
    if last_value is None:
        delta = None
    else:
        delta = used_pct - last_value
        if abs(delta) <= 0.1:
            delta = None
        else:
            delta = f"{delta:.2f}%"
    st.session_state["servers_metric_metrics"][ss_key] = used_pct
    return delta


def st_metric_memory(label, stats, help, used_gb_key, total_gb_key, used_pct_key, last_stat_key: str):
    default_display = "N/A"
    ## Gaia memory usage
    delta = None
    if (stats is not None) and stats.get(total_gb_key, 0) > 0:
        used_pct = stats[used_pct_key]
        ### Get delta from last fetch
        delta = handle_delta_memory(used_pct, last_stat_key)
        ### Build display message
        mem_display = f"{used_pct:.1f}% ({stats[used_gb_key]:.0f}/{stats[total_gb_key]:.0f} GB)"
    else:
        mem_display = default_display
    st.metric(
        label,
        mem_display,
        delta=delta,
        help=help,
    )


def st_metric_int(stats, value_key, **kwargs):
    # Sanity check
    assert "delta" not in kwargs, "Delta should not be provided, it is calculated automatically."
    assert value_key in stats, f"{value_key} not found in stats."
    # Estimate delta
    value = stats[value_key]
    last_value = st.session_state["servers_metric_metrics"].get(value_key)
    diff_value = value - last_value if last_value is not None else None
    diff_value = diff_value if diff_value != 0 else None  # No delta if no change
    # Store last value
    st.session_state["servers_metric_metrics"][value_key] = value

    # Add value
    kwargs["value"] = stats[value_key]

    # Display metric
    st.metric(
        delta=diff_value,
        **kwargs,
    )


# Session state
st.session_state.setdefault("servers_metric_metrics", {})

st.session_state.setdefault("servers_metric_memory", None)
st.session_state.setdefault("servers_metric_swap", None)


# Header
st_title_with_expert("Staging Servers Dashboard", icon=":material/computer:")

# Add refresh button and auto-refresh controls
with st.container(horizontal=True, vertical_alignment="bottom", horizontal_alignment="distribute"):
    with st.container(horizontal=True, vertical_alignment="bottom"):
        if st.button("Refresh Data", type="primary", icon=":material/autorenew:"):
            st.cache_data.clear()  # Clear cache to force refresh
            st.rerun()
        st.badge("Data cached for 5 minutes", color="primary", icon=":material/info:")

    # Additional information section
    with st.popover("ℹ️ About this Dashboard"):
        st.markdown("""
        **Data Source**: This dashboard fetches real-time data from LXC containers using the `owid-lxc` command.

        **Status Indicators**:
        - 🟢 **Running**: Container is active and operational
        - ⏸️ **Idle**: Container is stopped to save resources, but its **data is preserved**. Wake it instantly with the **Wake** button (no commit needed).

        **Last commit**: age of the newest ETL/Grapher commit. This is reference info only — it does **not** by itself decide when a server is stopped.

        **Auto-stop** — when (and why) the reaper will stop a server:
        - A running server is auto-stopped once **max(last commit, last _start_) is older than 14 days**. Stopping just idles it (data preserved); it's not destroyed.
        - Because *last start* counts, **waking or restarting a server resets the clock** — so a server with an old commit can still be far from being stopped. The column shows the reason (e.g. *restarted Jun 19* vs *commit Jun 19*).
        - 🟢 healthy · 🟡 stops within a few days · 🔴 stops on the next nightly run · ⏸️ already idle · ♾️ persistent (`master`, never auto-stopped) · ❓ can't be judged (commit unreadable).
        - ⚠️ A **host restart bumps every container's last-start at once**, resetting the timer fleet-wide — so right after a `gaia-1` reboot most servers will read the same countdown regardless of commit age.

        **Destroy** (frees the disk) only happens after the **PR is merged/closed** (~3 days later) — auto-stop never destroys.

        **Memory Usage**:
        - Only running containers show memory usage
        - Percentage calculated as (used / total) * 100

        **Refresh**: Data is cached for 5 minutes to avoid overwhelming the LXC host. Click 'Refresh Data' to fetch latest information.

        **Branch Names**: The 'staging-site-' prefix is automatically removed for cleaner display.
        """)

# Show the outcome of the last server action (carried across the post-action rerun, so it's
# displayed alongside the freshly-refreshed server list).
_action_result = st.session_state.pop("servers_action_result", None)
if _action_result is not None:
    _ok, _label, _msg = _action_result
    if _ok:
        st.success(f"✅ {_label}: {_msg}")
    else:
        st.error(f"❌ {_label} failed: {_msg}")

# Fetch server data and host memory stats
with st.spinner("Fetching staging server data...", show_time=True):
    servers_df, error = fetch_lxc_servers_data()
    host_memory_stats, host_memory_error = fetch_host_memory_stats()
    owners = fetch_server_owners()  # {container_name: github_login}, best-effort

# Handle errors
if error:
    st.error(f"Failed to fetch server data: {error}")
    st.info("Please ensure you have access to the LXC host and the `owid-lxc` command is available.")
    st.stop()

if servers_df is None or servers_df.empty:
    st.warning("No staging servers found.")
    st.stop()

# Display warning for host memory stats if failed
if host_memory_error:
    st.warning(f"Could not fetch host memory stats: {host_memory_error}")

# Display summary statistics
stats = get_server_summary_stats(servers_df, host_memory_stats)

with st.container(horizontal=True, border=True):
    # Number of servers
    st_metric_int(
        stats=stats,
        label="Total Servers",
        value_key="total_servers",
    )
    # Number of running servers
    st_metric_int(
        stats=stats,
        label="Running",
        value_key="running_servers",
    )
    # Number of idle (stopped) servers
    st_metric_int(
        stats=stats,
        label="Idle",
        value_key="stopped_servers",
        delta_color="inverse",
    )

    # Memory
    ## Gaia memory usage
    st_metric_memory(
        label="gaia-1 Memory",
        stats=stats["host_memory_stats"],
        help="RAM used on Gaia. Arrow indicates if the usage has increased or decreased since last fetch.",
        used_gb_key="used_gb",
        total_gb_key="total_gb",
        used_pct_key="usage_pct",
        last_stat_key="servers_metric_memory",
    )
    ## Gaia swap usage
    st_metric_memory(
        label="gaia-1 Swap",
        stats=stats["host_memory_stats"],
        help="Swap memory used on Gaia. Arrow indicates if the usage has increased or decreased since last fetch.",
        used_gb_key="swap_used_gb",
        total_gb_key="swap_total_gb",
        used_pct_key="swap_usage_pct",
        last_stat_key="servers_metric_swap",
    )

# Enrich with owner derived from the GitHub PR author (best-effort, may be blank)
servers_df["owner"] = servers_df["name"].map(owners).fillna("")

# Filter controls
st.subheader("🖥️ Staging Servers")
col1, col2, col3, col4 = st.columns(4)

with col1:
    status_filter = st.selectbox("Status", options=["All", "Running", "Idle"], index=0)

with col2:
    all_owners = [o for o in servers_df["owner"].unique() if o]
    owner_filter = st.multiselect("Owners", options=sorted(all_owners), default=[])

with col3:
    all_origins = [o for o in servers_df["origin"].unique() if isinstance(o, str)]
    origin_filter = st.multiselect("Origins", options=sorted(set(all_origins)), default=[])

with col4:
    branch_query = st.text_input("Search branch", placeholder="filter by branch name…")

# Apply filters
filtered_df = servers_df.copy()

if status_filter != "All":
    filtered_df = filtered_df[filtered_df["status_label"] == status_filter]

if owner_filter:
    filtered_df = filtered_df[filtered_df["owner"].isin(owner_filter)]

if origin_filter:
    filtered_df = filtered_df[filtered_df["origin"].isin(origin_filter)]

if branch_query:
    # regex=False: treat the search box as literal text so metacharacters (e.g. "[") don't crash.
    filtered_df = filtered_df[filtered_df["branch"].str.contains(branch_query, case=False, na=False, regex=False)]

# Display filtered results count
if len(filtered_df) != len(servers_df):
    st.caption(f"Showing {len(filtered_df)} of {len(servers_df)} servers")


def _run_action(label: str, action_fn, server_name: str, spinner_msg: str) -> None:
    """Run a (non-destructive) server action with a spinner, then refresh the dashboard."""
    with st.spinner(spinner_msg, show_time=True):
        success, message = action_fn(server_name)
    # Always refresh the cached server list and rerun — even on failure the container's state may
    # have changed (e.g. a wake that started the container but couldn't reach Tailscale), and a
    # stale Idle row + Wake button would mislead. Carry the outcome across the rerun so it's still
    # shown once the fresh state is loaded.
    st.session_state["servers_action_result"] = (success, label, message)
    st.cache_data.clear()
    st.rerun()


def _confirm_destructive_action(
    title: str, bullets: list[str], warning: str, button_label: str, action_fn, server_name: str, spinner_msg: str
) -> None:
    """Open a confirmation dialog for a destructive action (Reset MySQL / Destroy)."""

    @st.dialog(title)
    def _dialog():
        st.markdown(f"**Server:** `{server_name}`")
        st.markdown("**This will:**")
        for bullet in bullets:
            st.markdown(f"- {bullet}")
        st.warning(warning)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("❌ Cancel", width="stretch", key=f"cancel_{title}_{server_name}"):
                st.rerun()
        with c2:
            if st.button(button_label, type="primary", width="stretch", key=f"go_{title}_{server_name}"):
                with st.spinner(spinner_msg, show_time=True):
                    success, message = action_fn(server_name)
                if success:
                    st.success(f"✅ {message}")
                    st.cache_data.clear()
                else:
                    st.error(f"❌ {message}")

    _dialog()


def _destroy_dialog(branch: str, server_name: str) -> None:
    """Shared confirmation dialog for destroying a server."""
    _confirm_destructive_action(
        title=f"Destroy server: {branch}",
        bullets=[
            "**DELETE** the entire container including all data",
            "Pushing a new commit triggers server recreation",
        ],
        warning="💥 This destroys the container and all its data!",
        button_label="💥 Destroy server",
        action_fn=destroy_server,
        server_name=server_name,
        spinner_msg="Destroying server…",
    )


def render_server_actions(server) -> None:
    """Render the action panel (links + buttons) for a single selected server."""
    branch = server["branch"]
    server_name = server["name"]
    is_running = server["status"] == "Running"

    # Quick links (same set owidbot posts on each PR)
    links = build_quick_links(branch)
    st.markdown(
        f"**{server['status_indicator']} {branch}** &nbsp; · &nbsp; "
        + " · ".join(f"[{label}]({url})" for label, url in links.items())
    )

    if is_running:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button(
                "⏸️ Make idle",
                width="stretch",
                key=f"idle_{server_name}",
                help="Snapshot, then stop the server to free resources. Data is preserved; wake it anytime.",
            ):
                _run_action("Make idle", stop_server, server_name, "Snapshotting and stopping server…")
        with c2:
            if st.button(
                "🔄 Restart", width="stretch", key=f"restart_{server_name}", help="Stop and start the server again."
            ):
                _run_action("Restart", restart_server, server_name, "Restarting server…")
        with c3:
            if st.button(
                "🗄️ Reset MySQL",
                width="stretch",
                key=f"mysql_{server_name}",
                help="Drop and reimport the staging database from master (~5 min).",
            ):
                _confirm_destructive_action(
                    title=f"Reset MySQL: {branch}",
                    bullets=[
                        "Purge R2 files for this server from owid-api-staging",
                        "Run `make refresh` in the owid-grapher directory",
                        "Drop and recreate the MySQL database",
                        "Import the latest data from staging",
                    ],
                    warning="⏱️ Takes ~5 minutes. All current DB data including charts and indicators will be lost!",
                    button_label="🗄️ Reset MySQL",
                    action_fn=reset_mysql_database,
                    server_name=server_name,
                    spinner_msg="Resetting MySQL database… This may take 5 minutes",
                )
        with c4:
            if st.button(
                "💥 Destroy",
                width="stretch",
                key=f"destroy_{server_name}",
                help="Delete the container entirely. Recreated on the next commit to this branch.",
            ):
                _destroy_dialog(branch, server_name)
    else:
        # Idle server: the key action is waking it up (no commit needed).
        c1, c2 = st.columns(2)
        with c1:
            if st.button(
                "☀️ Wake up",
                type="primary",
                width="stretch",
                key=f"wake_{server_name}",
                help="Start the server again. Data is preserved — this takes seconds, no commit required.",
            ):
                _run_action("Wake up", start_server, server_name, "Waking server up…")
        with c2:
            if st.button(
                "💥 Destroy",
                width="stretch",
                key=f"destroy_{server_name}",
                help="Delete the container entirely. Recreated on the next commit to this branch.",
            ):
                _destroy_dialog(branch, server_name)


# Display the management panel above a selectable table.
if filtered_df.empty:
    st.warning("No servers match the current filters.")
else:
    filtered_df = filtered_df.reset_index(drop=True)

    # Read the row selected on the previous run (the table widget is rendered below).
    table_state = st.session_state.get("servers_table")
    selected_rows = table_state["selection"]["rows"] if table_state else []

    # Action panel for the selected server (shown above the table).
    st.subheader("🔧 Server Management")
    if selected_rows and selected_rows[0] < len(filtered_df):
        render_server_actions(filtered_df.iloc[selected_rows[0]])
    else:
        st.caption("👆 Select a server in the table below to see its links and management actions.")

    # Build the display table (memory as numeric GB so the progress bar renders).
    display_df = pd.DataFrame(
        {
            "Status": filtered_df["status_indicator"],
            "Owner": filtered_df["owner"],
            "Origin": filtered_df["origin"],
            "Branch": filtered_df["branch"],
            "Memory": filtered_df["memory_used_gb"].fillna(0).round(2),
            "Age (days)": filtered_df["days_old"],
            "Last commit": filtered_df["last_commit_display"],
            "Auto-stop": filtered_df["auto_stop"],
        }
    )

    st.dataframe(
        display_df,
        width="stretch",
        column_config={
            "Status": st.column_config.TextColumn("Status", help="🟢 Running · ⏸️ Idle", width="small"),
            "Owner": st.column_config.TextColumn("Owner", help="GitHub author of the PR", width="small"),
            "Origin": st.column_config.TextColumn("Origin", help="Server origin (etl, grapher, etc.)", width="small"),
            "Branch": st.column_config.TextColumn(
                "Branch", help="Git branch name (staging-site- prefix removed)", width="medium"
            ),
            "Memory": st.column_config.ProgressColumn(
                "Memory",
                help="Memory usage with progress bar showing GB values",
                min_value=0,
                max_value=20,  # Set reasonable max for progress bar visualization
                format="%.1f GB",
                width="medium",
            ),
            "Age (days)": st.column_config.NumberColumn(
                "Age (days)", help="Days since container was created", format="%d days", width="small"
            ),
            "Last commit": st.column_config.TextColumn(
                "Last commit",
                help="Age of the newest ETL/Grapher commit. Informational only — it does not by "
                "itself decide when a server is stopped (see Auto-stop).",
                width="small",
            ),
            "Auto-stop": st.column_config.TextColumn(
                "Auto-stop",
                help="When the reaper will stop this server, and why it's still alive. A server is "
                "auto-stopped once max(last commit, last start) exceeds 14 days — so a recent "
                "restart keeps an old-commit server running. 🟢 healthy · 🟡 stops soon · "
                "🔴 stops tonight · ⏸️ already idle · ♾️ persistent.",
                width="medium",
            ),
        },
        hide_index=True,
        height=600,
        on_select="rerun",
        selection_mode="single-row",
        key="servers_table",
    )
