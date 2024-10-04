import streamlit as st

from apps.wizard.utils.data import load_variable_data, load_variable_metadata

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
)

# INDICATOR SPECS (INPUT)
DATASETS = [
    {
        "dataset": "grapher/climate_watch/2023-10-31/emissions_by_sector",
        "indicators": [
            {
                "slug": "greenhouse_gas_emissions_by_sector#land_use_change_and_forestry_ghg_emissions",
                "anomalies": [
                    {
                        "title": "Afghanistan's Early Internet Usage",
                        "description": "Between 2001 and 2006, Afghanistan showed extremely low internet usage, remaining at approximately 0% for several years before a gradual increase began.",
                    },
                    {
                        "title": "Significant Jump in Angola's Internet Usage",
                        "description": "In 2012, Angola witnessed a significant jump in the share of individuals using the internet from 4.7% in 2011 to 7.7%, indicating a rapid growth phase.",
                    },
                    {
                        "title": "Explosive Growth in United Arab Emirates",
                        "description": "From 2007 to 2008, the United Arab Emirates saw an explosive growth in internet usage, rising from 61% to 63%, continuing its trend towards universal access.",
                    },
                ],
            },
        ],
    }
]
st.session_state.datasets = st.session_state.get("datasets", DATASETS)


# ANOMALY STATUS
# Initialise/update anomaly-review status
for d_i, d in enumerate(st.session_state.datasets):
    # print(f"dataset {d_i}")
    for i_i, i in enumerate(d["indicators"]):
        # print(f"indicator {i_i}")
        for a_i, a in enumerate(i["anomalies"]):
            print(f"anomaly {a_i}")
            if "resolved" not in a:
                # print("> initialising")
                a["resolved"] = False
            else:
                # print("> updating")
                a["resolved"] = st.session_state[f"resolved_{d_i}_{i_i}_{a_i}"]


# PAGE TITLE
st.title(":material/planner_review: Anomalist")
st.markdown("Detect anomalies in your data!")
# st.write(st.session_state.datasets)
st.divider()

################################################
# FUNCTIONS
################################################


@st.dialog("Vizualize the indicator", width="large")
def show_indicator(indicator_uri):
    """Plot the indicator in a modal window."""
    # Modal title
    st.markdown(f"`{indicator_uri}`")

    # Get data and metadata from catalog
    data = load_variable_data(indicator_uri)
    # metadata = load_variable_metadata(indicator_uri)

    # Get list of entities available
    entities = list(data["entity"].unique())
    entities_agg = {
        "Africa",
        "Asia",
        "Europe",
        "North America",
        "Oceania",
        "South America",
        "World",
    }
    entities_default = [e for e in entities if e not in entities_agg][:5]
    countries = st.multiselect("Entities", entities, default=entities_default)

    # Filter data
    data_ = data[data["entity"].isin(countries)]
    # data_ = data_.pivot(index="years", columns="entities", values="values")
    # columns = [col for col in data_.columns if col != "years"]
    # st.dataframe(data_)

    # Plot data
    st.line_chart(data=data_, x="years", y="values", color="entity")


# Block per dataset
for dataset_index, d in enumerate(st.session_state.datasets):
    st.markdown(f'##### :material/dataset: {d["dataset"]}')
    indicators = d["indicators"]

    # Block per indicator in dataset
    for indicator_index, i in enumerate(indicators):
        indicator_uri = f"{d['dataset']}/{i['slug']}"
        with st.container(border=True):
            # Title
            st.markdown(f"`{i['slug']}`")

            # Show indicator button
            if st.button("Show chart", icon=":material/show_chart:", use_container_width=False):
                show_indicator(indicator_uri)

            # Anomalies detected
            anomalies = i["anomalies"]
            st.markdown(f"{len(anomalies)} anomalies detected.")

            for anomaly_index, a in enumerate(anomalies):
                # Review icon
                if a["resolved"]:
                    icon = "✅"
                else:
                    icon = "⏳"

                # Anomaly explained (expander)
                with st.expander(f'{anomaly_index+1}/ {a["title"]}', expanded=False, icon=icon):
                    # Check if resolved
                    key = f"resolved_{dataset_index}_{indicator_index}_{anomaly_index}"

                    # Checkbox (if resolved)
                    st.checkbox(
                        "Mark as resolved",
                        value=a["resolved"],
                        key=key,
                    )

                    # Anomaly description
                    st.markdown(a["description"])
