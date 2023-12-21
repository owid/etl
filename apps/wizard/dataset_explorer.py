"""Explore a dataset from ETL.

- [ ] See its dependencies
- [ ] Preview its metadata
"""
from typing import Any, Dict, cast

import streamlit as st
from st_pages import add_indentation
from streamlit_agraph import Config, Edge, Node, agraph

from etl.steps import extract_step_attributes, filter_to_subgraph, load_dag

# CONFIG
st.set_page_config(
    page_title="Dataset Explorer",
    layout="wide",
    page_icon="🕵️",
    initial_sidebar_state="collapsed",
)
st.title("🕵️ Dataset Explorer")
add_indentation()

COLORS = {
    "snapshot": "#FC9090",
    # "walden": "#D66868",
    "meadow": "#F5DB49",
    "garden": "#87E752",
    "grapher": "#67AAE1",
}
COLOR_OTHER = "#B6B6B6"
COLOR_MAIN = "#81429A"


def activate():
    st.session_state["show_gpt"] = True


def generate_graph(dag: Dict[str, Any], uri_main: str):
    def _friendly_label(attributes: Dict[str, str], length_limit: int = 32) -> str:
        label_1 = f"{attributes['namespace']}/{attributes['name']}"
        if len(label_1) > length_limit:
            label_1 = label_1[:length_limit] + "..."
        label = f"{label_1}\n{attributes['version']}"
        return label

    def _friendly_title(attributes, children):
        deps = "\n- ".join(children)
        title = f"""{attributes['identifier'].upper()}
        version {attributes['version']} ({attributes['kind']})

        dependencies:
        - {deps}
        """
        return title

    # Create edges
    edges = []
    nodes = []
    for parent, children in dag.items():
        attributes = extract_step_attributes(parent)
        if parent == uri_main:
            kwargs = {
                "color": COLORS.get(attributes["channel"], COLOR_OTHER),
                "label": f"{attributes['namespace'].upper()}/{attributes['name'].upper()}\n{attributes['version']}",
                "title": _friendly_title(attributes, children),
                "shape": "box",
                "borderWidth": 2,
                "font": {
                    "size": 40,
                    "face": "courier",
                    "align": "left",
                },
                "mass": 2,
            }
        else:
            kwargs = {
                "color": COLORS.get(attributes["channel"], COLOR_OTHER),
                "label": _friendly_label(attributes),
                "title": _friendly_title(attributes, children),
                "shape": "box",
                "borderWidth": 1,
                "chosen": {
                    "label": _friendly_label(attributes, 100),
                },
                "font": {
                    "size": 20,
                    "face": "courier",
                    "align": "left",
                },
                "mass": 1,
                "opacity": 0.9,
            }
        node = Node(
            id=parent,
            borderWidthSelected=5,
            margin=10,
            **kwargs,
        )
        nodes.append(node)
        for child in children:
            edge = Edge(
                source=child,
                target=parent,
                width=2,
            )
            edges.append(edge)

    # config_builder = ConfigBuilder(nodes)
    # config = config_builder.build()

    config = Config(
        width=1920,
        height=1080,
        directed=True,
        physics=True,
        minVelocity=20,
        maxVelocity=100,
        # hierarchical=True,
        # nodeSpacing=200,
        # **kwargs
    )

    # config.physics["barnesHut"] = {"springConstant": 0, "avoidOverlap": 0.1}

    return agraph(nodes=nodes, edges=edges, config=config)


with st.form("form"):
    dag = load_dag()
    options = sorted(list(dag.keys()))
    option = st.selectbox("Select a dataset", options)

    st.form_submit_button("Explore", on_click=activate)


if st.session_state.get("show_gpt"):
    with st.spinner(f"Generating DOT file for {option}..."):
        dag = filter_to_subgraph(dag, includes=[cast(str, option)])

        with st.expander("DAG"):
            st.write(dag)
        if option is None:
            option = options[0]
        graph = generate_graph(dag, option)

    # Set back to False
    # st.session_state["show_gpt"] = False
