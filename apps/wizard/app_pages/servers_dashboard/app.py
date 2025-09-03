"""Staging Servers Dashboard - Monitor all LXC staging servers with real-time metrics."""

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.servers_dashboard.utils import (
    destroy_server,
    fetch_host_memory_stats,
    fetch_lxc_servers_data,
    get_server_summary_stats,
    prepare_display_dataframe,
    reset_mysql_database,
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
        - 🔴 **Stopped**: Container is stopped or inactive

        **Commit Status**:
        - ✅ **Today/Recent**: Commits within the last week
        - ⚠️ **Warning**: Commits older than 7 days
        - ❌ **Old**: Commits much older or containers not accessible
        - ❓ **Unknown**: Unable to determine commit status

        **Memory Usage**:
        - Only running containers show memory usage
        - Percentage calculated as (used / total) * 100

        **Refresh**: Data is cached for 5 minutes to avoid overwhelming the LXC host. Click 'Refresh Data' to fetch latest information.

        **Branch Names**: The 'staging-site-' prefix is automatically removed for cleaner display.
        """)

# Fetch server data and host memory stats
with st.spinner("Fetching staging server data...", show_time=True):
    servers_df, error = fetch_lxc_servers_data()
    host_memory_stats, host_memory_error = fetch_host_memory_stats()

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
    # Number of stopped servers
    st_metric_int(
        stats=stats,
        label="Stopped",
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

# Prepare data for display
display_df = prepare_display_dataframe(servers_df)

# Filter controls
st.subheader("🖥️ Staging Servers")
col1, col2, col3 = st.columns(3)

with col1:
    status_filter = st.selectbox("Status", options=["All", "Running", "Stopped"], index=0)

with col2:
    # Extract unique origins for filter
    all_origins = servers_df["origin"].unique()
    origin_filter = st.multiselect("Origins", options=sorted(all_origins), default=[])

with col3:
    # Extract unique branches for filter (excluding full container names)
    all_branches = servers_df["branch"].unique()
    branch_filter = st.multiselect("Branches", options=sorted(all_branches), default=[])

# Apply filters
filtered_df = display_df.copy()

# Status filter
if status_filter != "All":
    status_symbol = "🟢" if status_filter == "Running" else "🔴"
    filtered_df = filtered_df[filtered_df["Status"] == status_symbol]

# Origin filter
if origin_filter:
    # Map back to original server data for filtering
    origin_mask = servers_df["origin"].isin(origin_filter)
    filtered_df = filtered_df[origin_mask]

# Branch filter
if branch_filter:
    # Map back to original server data for filtering
    branch_mask = servers_df["branch"].isin(branch_filter)
    filtered_df = filtered_df[branch_mask]

# Display filtered results count
if len(filtered_df) != len(display_df):
    st.info(f"Showing {len(filtered_df)} of {len(display_df)} servers")

# Display data table
if filtered_df.empty:
    st.warning("No servers match the current filters.")
else:
    # Display the table with custom column configuration
    st.dataframe(
        filtered_df,
        use_container_width=True,
        column_config={
            "Status": st.column_config.TextColumn("Status", help="Server running status", width="small"),
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
            "Last Commit": st.column_config.TextColumn(
                "Last Commit", help="Most recent relevant commit based on server origin", width="medium"
            ),
        },
        hide_index=True,
        height=600,
    )

    # Server Management section
    st.subheader("🔧 Server Management")

    # Server selection
    server_options = ["Select a server..."] + filtered_df["Branch"].tolist()
    selected_server_branch = st.selectbox("Select server:", options=server_options, key="server_select")

    if selected_server_branch != "Select a server...":
        selected_server = filtered_df[filtered_df["Branch"] == selected_server_branch].iloc[0]
        server_name = f"staging-site-{selected_server['Branch']}"

        # Server info
        st.info(f"**Selected:** `{selected_server['Branch']}` ({selected_server['Status']})")

        # Action buttons
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🗄️ Reset MySQL", use_container_width=True):

                @st.dialog(f"Reset MySQL: {selected_server['Branch']}")
                def show_mysql_reset_modal():
                    st.markdown("⚠️ **You are about to reset the MySQL database:**")
                    st.markdown(f"**Server:** `{server_name}`")

                    st.markdown("---")
                    st.markdown("**This will:**")
                    st.markdown("- Run `make refresh` in the owid-grapher directory")
                    st.markdown("- Drop and recreate the MySQL database")
                    st.markdown("- Import the latest data from staging")

                    st.warning("⏱️ **This process takes about 5 minutes to complete!**")
                    st.error("⚠️ **All current database data including charts and indicators will be lost!**")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("❌ Cancel", use_container_width=True):
                            st.rerun()
                    with col2:
                        if st.button("🗄️ Reset MySQL", type="primary", use_container_width=True):
                            with st.spinner("Resetting MySQL database... This may take 5 minutes", show_time=True):
                                success, message = reset_mysql_database(server_name)

                            if success:
                                st.success(f"✅ {message}")
                                st.info("🎉 MySQL database has been refreshed with the latest data!")
                                # Clear cache to refresh data on next load
                                st.cache_data.clear()
                            else:
                                st.error(f"❌ MySQL reset failed: {message}")
                                st.info("Please check the server logs or try again later.")

                show_mysql_reset_modal()

        with col2:
            if st.button("💥 Destroy Server", type="secondary", use_container_width=True):

                @st.dialog(f"Destroy Server: {selected_server['Branch']}")
                def show_destroy_modal():
                    st.markdown("💥 **You are about to DESTROY the staging server:**")
                    st.markdown(f"**Server:** `{server_name}`")

                    st.markdown("---")
                    st.markdown("**This will:**")
                    st.markdown("- **DELETE** the entire container including all data")
                    st.markdown("- Pushing a new commit triggers server recreation")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("❌ Cancel", use_container_width=True):
                            st.rerun()
                    with col2:
                        if st.button("💥 DESTROY SERVER", type="primary", use_container_width=True):
                            with st.spinner("Destroying server...", show_time=True):
                                success, message = destroy_server(server_name)

                            if success:
                                st.success(f"✅ {message}")
                                st.info(
                                    "Server has been completely destroyed. It will be recreated on the next commit to this branch."
                                )
                                # Clear cache to refresh data on next load
                                st.cache_data.clear()
                                # Wait a moment then refresh the page
                                import time

                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error(f"❌ Server destruction failed: {message}")
                                st.info("Please check the server status or try again later.")

                show_destroy_modal()

        with col3:
            # st.markdown("**Quick Access:**")
            pass

        # Connection info
        st.markdown("---")
        st.markdown("**🔗 Connection Commands:**")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**MySQL Access:**")
            mysql_cmd = f"mysql -h {server_name} -u owid --port 3306 -D owid"
            st.code(mysql_cmd, language="bash")

        with col2:
            st.markdown("**SSH Access:**")
            ssh_cmd = f"ssh owid@{server_name}"
            st.code(ssh_cmd, language="bash")
