import streamlit as st

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
)

# INDICATOR SPECS (INPUT)
DATASETS = [
    {
        "dataset": "grapher/climate_watch/2023-10-31/emissions_by_sector/",
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

# Block per dataset
for dataset_index, d in enumerate(st.session_state.datasets):
    st.markdown(f'##### :material/dataset: {d["dataset"]}')
    indicators = d["indicators"]

    # Block per indicator in dataset
    for indicator_index, i in enumerate(indicators):
        with st.container(border=True):
            st.markdown(f"`{i['slug']}`")
            anomalies = i["anomalies"]
            st.markdown(f"{len(anomalies)} anomalies detected.")

            # Expander per anomaly
            for anomaly_index, a in enumerate(anomalies):
                if a["resolved"]:
                    icon = "✅"
                else:
                    icon = "⏳"
                # print(f"{anomaly_index}/ {icon}")
                with st.expander(f'{anomaly_index+1}/ {a["title"]}', expanded=False, icon=icon):
                    # Check if resolved
                    key = f"resolved_{dataset_index}_{indicator_index}_{anomaly_index}"
                    st.checkbox(
                        "Mark as resolved",
                        value=a["resolved"],
                        key=key,
                    )

                    st.markdown(a["description"])
