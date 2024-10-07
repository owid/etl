import pandas as pd
import streamlit as st

data = {
    "anomaly": ["Anomaly 1", "Anomaly 2", "Anomaly 3"],
    "description": ["Description 1", "Description 2", "Description 3"],
}

df = pd.DataFrame(data)
st.session_state.df = df


@st.dialog("Show anomaly")
def action():
    # st.write(st.session_state.anomalies)
    row_num = st.session_state.anomalies["selection"]["rows"][0]
    st.dataframe(st.session_state.df.iloc[row_num])


event = st.dataframe(df, on_select=action, selection_mode="single-row", key="anomalies")
