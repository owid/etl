import streamlit.components.v1 as components
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder

from apps.step_update.cli import UpdateState

# Custom JS code
JSCODE_UPDATE_DAYS = JsCode(
    """
    function(params){
        if (params.value <= 0) {
            return {
                'color': 'black',
                'backgroundColor': 'red'
            }
        } else if (params.value > 0 && params.value <= 31) {
            return {
                'color': 'black',
                'backgroundColor': 'orange'
            }
        } else if (params.value > 31) {
            return {
                'color': 'black',
                'backgroundColor': 'green'
            }
        } else {
            return {
                'color': 'black',
                'backgroundColor': 'yellow'
            }
        }
    }
    """
)

JSCODE_UPDATE_STATE = JsCode(
    f"""
    function(params){{
        if (params.value === "{UpdateState.UP_TO_DATE.value}") {{
            return {{'color': 'black', 'backgroundColor': 'green'}}
        }} else if (params.value === "{UpdateState.OUTDATED.value}") {{
            return {{'color': 'black', 'backgroundColor': 'gray'}}
        }} else if (params.value === "{UpdateState.MAJOR_UPDATE.value}") {{
            return {{'color': 'black', 'backgroundColor': 'red'}}
        }} else if (params.value === "{UpdateState.MINOR_UPDATE.value}") {{
            return {{'color': 'black', 'backgroundColor': 'orange'}}
        }} else if (params.value === "{UpdateState.ARCHIVABLE.value}") {{
            return {{'color': 'black', 'backgroundColor': 'blue'}}
        }} else if (params.value === "{UpdateState.UNUSED.value}") {{
            return {{'color': 'black', 'backgroundColor': 'lightblue'}}
        }} else {{
            return {{'color': 'black', 'backgroundColor': 'yellow'}}
        }}
    }}
    """
)
JSCODE_DATASET_GRAPHER = JsCode(
    r"""
        class UrlCellRenderer {
        init(params) {
            this.eGui = document.createElement('a');
            if(params.value) {
                const match = params.value.match(/\[(.*?)\]\((.*?)\)/);
                if(match && match.length >= 3) {
                    const datasetName = match[1];
                    const datasetUrl = match[2];
                    this.eGui.innerText = datasetName;
                    this.eGui.setAttribute('href', datasetUrl);
                } else {
                    this.eGui.innerText = '';
                }
            } else {
                this.eGui.innerText = '';
            }
            this.eGui.setAttribute('style', "text-decoration:none");
            this.eGui.setAttribute('target', "_blank");
        }
        getGui() {
            return this.eGui;
        }
        }
        """
)
# Enable clearing selection via API
JS_CODE_ENABLE_CLEAR_SELECTION = js = JsCode("""
function(event) {
    const api = event.api;
    window.addEventListener('clear.rows', (e) => {
        api.deselectAll();
    });
}
""")


def make_agrid(steps_df_display):
    grid_options = make_grid_options(steps_df_display)
    grid_response = AgGrid(
        data=steps_df_display,
        gridOptions=grid_options,
        height=500,
        # height="100%",
        # width="100%",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        # fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        theme="streamlit",
        # The following ensures that the pagination controls are not cropped.
        # custom_css={
        #     "#gridToolBar": {
        #         "padding-bottom": "0px !important",
        #     }
        # },
        # columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
    )

    return grid_response


def make_grid_options(steps_df_display):
    # Initial build
    gb = GridOptionsBuilder.from_dataframe(
        dataframe=steps_df_display,
        editable=False,
        # groupable=True,
        sortable=True,
        # filterable=True,
        resizable=True,
    )

    # General settings
    gb.configure_grid_options(
        # domLayout="autoHeight",
        enableCellTextSelection=True,
        ColumnsAutoSizeMode=ColumnsAutoSizeMode.FIT_CONTENTS,
        onFirstDataRendered=JS_CODE_ENABLE_CLEAR_SELECTION,
    )
    gb.configure_selection(
        selection_mode="multiple",
        use_checkbox=True,
        rowMultiSelectWithClick=True,
        suppressRowDeselection=False,
        groupSelectsChildren=True,
        groupSelectsFiltered=True,
    )
    # gb.configure_side_bar(
    #     filters_panel=True,
    #     columns_panel=True,
    # )

    # Configure columns
    gb = _config_grid_columns(gb)

    # Pagination settings
    gb.configure_pagination(
        # paginationAutoPageSize=False,
        # paginationPageSize=20,
    )

    # Auto-height
    # gb.configure_auto_height()

    # Build
    grid_options = gb.build()

    return grid_options


def _config_grid_columns(gb):
    """Grid configuration"""
    # Column settings
    _config_column(
        gb,
        field="step",
        width=500,
        headerTooltip="Step URI, as it appears in the ETL DAG.",
    )
    _config_column(
        gb,
        field="channel",
        width=120,
        headerTooltip="Channel of the step (e.g. garden or grapher).",
    )
    _config_column(
        gb,
        field="namespace",
        width=150,
        headerTooltip="Namespace of the step.",
    )
    _config_column(
        gb,
        field="version",
        width=120,
        headerTooltip="Version of the step.",
    )
    _config_column(
        gb,
        field="name",
        headerName="Step name",
        width=140,
        headerTooltip="Short name of the step.",
    )
    _config_column(
        gb,
        field="kind",
        width=100,
        headerTooltip="Kind of step (i.e. public or private).",
    )
    _config_column(
        gb,
        field="n_charts",
        headerName="N. charts",
        width=120,
        text_filter=False,
        headerTooltip="Number of charts that use data from the step.",
    )
    # gb.configure_column(
    #     "n_chart_views_7d",
    #     headerName="7-day views",
    #     width=140,
    #     headerTooltip="Number of views of charts that use data from the step in the last 7 days.",
    # )
    _config_column(
        gb,
        field="n_chart_views_365d",
        headerName="365-day views",
        text_filter=False,
        width=140,
        headerTooltip="Number of views of charts that use data from the step in the last 365 days.",
    )
    _config_column(
        gb,
        field="date_of_next_update",
        headerName="Next update",
        text_filter=False,
        width=140,
        headerTooltip="Date of the next expected OWID update of the step.",
    )
    _config_column(
        gb,
        field="update_period_days",
        headerName="Update period",
        text_filter=False,
        width=150,
        headerTooltip="Number of days between consecutive OWID updates of the step.",
    )
    _config_column(
        gb,
        field="dag_file_name",
        headerName="Name of DAG file",
        width=160,
        headerTooltip="Name of the DAG file that defines the step.",
    )
    _config_column(
        gb,
        field="full_path_to_script",
        headerName="Path to script",
        width=150,
        headerTooltip="Path to the script that creates the ETL snapshot or dataset of this step.",
    )
    _config_column(
        gb,
        field="dag_file_path",
        headerName="Path to DAG file",
        width=160,
        headerTooltip="Path to the DAG file that defines the step.",
    )
    _config_column(
        gb,
        field="n_versions",
        headerName="N. versions",
        width=140,
        headerTooltip="Number of (active or archive) versions of the step.",
    )
    # Create a column with the number of days until the next expected update, colored according to its value.
    _config_column(
        gb,
        field="days_to_update",
        cellStyle=JSCODE_UPDATE_DAYS,
        width=120,
        headerTooltip="Number of days until the next expected OWID update of the step (if negative, an update is due).",
    )

    # Create a column colored depending on the update state.
    _config_column(
        gb,
        field="update_state",
        cellStyle=JSCODE_UPDATE_STATE,
        width=150,
        headerTooltip=f'Update state of the step: "{UpdateState.UP_TO_DATE.value}" (up to date), "{UpdateState.MINOR_UPDATE.value}" (a dependency is outdated, but all origins are up to date), "{UpdateState.MAJOR_UPDATE.value}" (an origin is outdated), "{UpdateState.OUTDATED.value}" (there is a newer version of the step), "{UpdateState.ARCHIVABLE.value}" (the step is outdated and not used in charts, therefore can safely be archived).',
    )
    ## Create a column with grapher dataset names that are clickable and open in a new tab.
    _config_column(
        gb,
        field="db_dataset_name_and_url",  # This will be the displayed column
        headerName="Grapher dataset",
        cellRenderer=JSCODE_DATASET_GRAPHER,
        headerTooltip="Name of the grapher dataset (if any), linked to its corresponding dataset admin page.",
    )

    return gb


def _config_column(gb, field: str, text_filter: bool = True, **kwargs):
    if "headerName" not in kwargs:
        kwargs["headerName"] = field.replace("_", " ").capitalize()

    params = {
        "field": field,
        **kwargs,
    }
    if text_filter:
        params["filter"] = "agTextColumnFilter"

    # Build column
    gb.configure_column(**params)

    return gb


def clear_aggrid_selections():
    """Unselect all rows from AgGrid table.
    Reference: https://discuss.streamlit.io/t/aggrid-unselect-all-rows/21367/11
    """
    clearJs = """<script>
    ((e) => {
        const iframe = window.parent.document.querySelectorAll('[title="st_aggrid.agGrid"]')[0] || null;
        if(!iframe) return;
        iframe.contentWindow.dispatchEvent( new Event('clear.rows'));
    })()
    </script>
    """
    components.html(clearJs)
