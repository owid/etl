import pandas as pd
import streamlit as st

from apps.step_update.cli import StepUpdater
from etl.config import OWID_ENV
from etl.db import can_connect

# List of identifiers of steps that should be considered as non-updateable.
# NOTE: The identifier is the step name without the version (and without the "data://").
NON_UPDATEABLE_IDENTIFIERS = [
    # All population-related datasets.
    "garden/demography/population",
    "garden/gapminder/population",
    "garden/hyde/baseline",
    "garden/un/un_wpp",
    "meadow/gapminder/population",
    "meadow/hyde/baseline",
    "meadow/hyde/general_files",
    "meadow/un/un_wpp",
    "open_numbers/open_numbers/gapminder__systema_globalis",
    "open-numbers/ddf--gapminder--systema_globalis",
    "snapshot/hyde/general_files.zip",
    "snapshot/hyde/baseline.zip",
    "snapshot/gapminder/population.xlsx",
    "snapshot/un/un_wpp.zip",
    # Regions dataset.
    "garden/regions/regions",
    # Old WB income groups.
    "garden/wb/wb_income",
    "meadow/wb/wb_income",
    "walden/wb/wb_income",
    # New WB income groups.
    "garden/wb/income_groups",
    "meadow/wb/income_groups",
    "snapshot/wb/income_groups.xlsx",
    # World Bank country shapes.
    "snapshot/countries/world_bank.zip",
    # World Bank WDI.
    "snapshot/worldbank_wdi/wdi.zip",
    "meadow/worldbank_wdi/wdi",
    "garden/worldbank_wdi/wdi",
    # Other steps we don't want to update (because the underlying data does not get updated).
    # TODO: We need a better way to achieve this, for example adding update_period_days to all steps and snapshots.
    #  A simpler alternative would be to move these steps to a separate file in a meaningful place.
    #  Another option is to have "playlists", e.g. "climate_change_explorer" with the identifiers of steps to update.
    "meadow/epa/ocean_heat_content",
    "snapshot/epa/ocean_heat_content_annual_world_700m.csv",
    "snapshot/epa/ocean_heat_content_annual_world_2000m.csv",
    "garden/epa/ocean_heat_content",
    "meadow/epa/ocean_heat_content",
    "meadow/epa/ice_sheet_mass_balance",
    "snapshot/epa/ice_sheet_mass_balance.csv",
    "garden/epa/ice_sheet_mass_balance",
    "meadow/epa/ice_sheet_mass_balance",
    "meadow/epa/ghg_concentration",
    "snapshot/epa/co2_concentration.csv",
    "snapshot/epa/ch4_concentration.csv",
    "snapshot/epa/n2o_concentration.csv",
    "garden/epa/ghg_concentration",
    "meadow/epa/ghg_concentration",
    "meadow/epa/mass_balance_us_glaciers",
    "snapshot/epa/mass_balance_us_glaciers.csv",
    "garden/epa/mass_balance_us_glaciers",
    "meadow/epa/mass_balance_us_glaciers",
    "meadow/climate/antarctic_ice_core_co2_concentration",
    "snapshot/climate/antarctic_ice_core_co2_concentration.xls",
    "garden/climate/antarctic_ice_core_co2_concentration",
    "meadow/climate/antarctic_ice_core_co2_concentration",
    "meadow/climate/global_sea_level",
    "snapshot/climate/global_sea_level.csv",
    "garden/climate/global_sea_level",
    "meadow/climate/global_sea_level",
]


def _create_html_button(text, border_color, background_color, color):
    html = f"""\
        <div
            style="border: 1px solid {border_color}; padding: 4px; display: inline-block; border-radius: 10px; background-color: {background_color}; cursor: pointer; color: {color}">
            {text}
        </div>
"""
    return html


def check_db():
    if not can_connect():
        st.error("Unable to connect to grapher DB.")


@st.cache_data(show_spinner=False)
def load_steps_df(reload_key: int) -> pd.DataFrame:
    """Generate and load the steps dataframe.

    This is just done once, at the beginning.
    """
    # Ensure that the function is re-run when the reload_key changes.
    _ = reload_key

    # Load steps dataframe.
    steps_df = StepUpdater().steps_df

    # Fix some columns.
    steps_df["full_path_to_script"] = steps_df["full_path_to_script"].fillna("").astype(str)
    steps_df["dag_file_path"] = steps_df["dag_file_path"].fillna("").astype(str)

    # For convenience, convert days to an arbitrarily big number.
    # Otherwise when sorting, nans are placed before negative numbers, and hence it's not easy to first see steps that
    # need to be updated more urgently.
    steps_df["days_to_update"] = steps_df["days_to_update"].fillna("9999")

    # For convenience, combine dataset name and url in a single column.
    # This will be useful when creating cells with the name of the dataset as a clickable link.
    # In principle, one can access different columns of the dataframe with UrlCellRenderer
    # (and then hide db_dataset_id column), however, then using "group by" fails.
    # So this is a workaround to allows to have both clickable cells with names, and "group by".
    steps_df["db_dataset_name_and_url"] = [
        f"[{row['db_dataset_name']}]({OWID_ENV.dataset_admin_site(int(row['db_dataset_id']))})"
        if row["db_dataset_name"]
        else None
        for row in steps_df.to_dict(orient="records")
    ]

    steps_df = steps_df.drop(columns=["db_dataset_name", "db_dataset_id"], errors="raise")

    return steps_df


@st.cache_data
def load_steps_df_to_display(show_all_channels: bool, reload_key: int) -> pd.DataFrame:
    """Load the steps dataframe, and filter it according to the user's choice."""
    # Load all data
    df = load_steps_df(reload_key=reload_key)

    # If toggle is not shown, pre-filter the DataFrame to show only rows where "channel" equals "grapher"
    if not show_all_channels:
        df = df[df["channel"].isin(["grapher", "explorers"])]

    # Sort displayed data conveniently.
    df = df.sort_values(
        by=["days_to_update", "n_chart_views_365d", "n_charts", "kind", "version"],
        na_position="last",
        ascending=[True, False, False, False, True],
    )

    # Prepare dataframe to be displayed in the dashboard.
    df = df[
        [
            "step",
            "db_dataset_name_and_url",
            "days_to_update",
            "update_state",
            "n_charts",
            # "n_chart_views_7d",
            "n_chart_views_365d",
            "update_period_days",
            "date_of_next_update",
            "namespace",
            "version",
            "channel",
            "name",
            "kind",
            "dag_file_name",
            "n_versions",
            # "state",
            "full_path_to_script",
            "dag_file_path",
            # "versions",
            # "role",
            # "all_usages",
            # "direct_usages",
            # "all_chart_ids",
            # "all_chart_slugs",
            # "all_chart_views_7d",
            # "all_chart_views_365d",
            # "all_active_dependencies",
            # "all_active_usages",
            # "direct_dependencies",
            # "chart_ids",
            # "same_steps_forward",
            # "all_dependencies",
            # "same_steps_all",
            # "same_steps_latest",
            # "latest_version",
            # "identifier",
            # "same_steps_backward",
            # "n_newer_versions",
            # "db_archived",
            # "db_private",
        ]
    ]

    dtypes_new = df.dtypes.replace("object", "string[pyarrow]")
    df = df.astype(dtypes_new)
    df["date_of_next_update"] = pd.to_datetime(df["date_of_next_update"], errors="coerce")
    return df
