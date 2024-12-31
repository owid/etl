"""Explore a dataset from ETL.

- [ ] See its dependencies
- [ ] Preview its metadata
"""

from typing import Any, Dict, List, cast

import streamlit as st
from streamlit_agraph import Config, Edge, Node, agraph

from etl.steps import extract_step_attributes, filter_to_subgraph, load_dag

COLORS = {
    "snapshot": "#FC9090",
    "walden": "#FC9090",
    "meadow": "#F5DB49",
    "garden": "#87E752",
    "grapher": "#67AAE1",
}
COLOR_OTHER = "#B6B6B6"
COLOR_MAIN = "#81429A"


@st.dialog("Dependency graph", width="large")
def show_modal_dependency_graph(dataset, dag, downstream=False):
    # Get step uri
    step_uri = f"data://grapher/{dataset['catalogPath']}"
    st.write(step_uri)
    with st.spinner(f"Generating graph {step_uri}..."):
        dag = filter_to_subgraph(dag, downstream=downstream, includes=[cast(str, step_uri)])

        with st.expander("Show raw DAG"):
            st.write(dag)

        _ = generate_graph(dag, step_uri)


@st.cache_data
def load_dag_cached():
    return load_dag()


def generate_graph(
    dag: Dict[str, Any],
    uri_main: str,
    collapse_snapshot: bool = True,
    collapse_others: bool = True,
    collapse_meadow: bool = True,
) -> Any:
    def _friendly_label(attributes: Dict[str, str], length_limit: int = 32) -> str:
        label_1 = f"{attributes['namespace']}/{attributes['name']}"
        if len(label_1) > length_limit:
            label_1 = label_1[:length_limit] + "..."
        label = f"{label_1}\n{attributes['version']}"
        return label

    def _friendly_title(attributes: Dict[str, str], children: List[str]) -> str:
        deps = "\n- ".join(children)
        title = f"""{attributes['identifier'].upper()}
        version {attributes['version']} ({attributes['kind']})
        """
        title = attributes["step"].upper()
        if deps:
            title = title + "\n\ndependencies:\n- " + deps
        return title

    def _collapse_node(attributes: Dict[str, str]) -> bool:
        if collapse_snapshot and (attributes["channel"] in ["snapshot", "walden"]):
            return True
        if collapse_meadow and (attributes["channel"] in ["meadow"]):
            return True
        if collapse_others and (attributes["channel"] not in ["snapshot", "walden", "meadow", "garden", "grapher"]):
            return True
        return False

    # Create edges
    edges = []
    nodes = []
    for parent, children in dag.items():
        attributes = extract_step_attributes(parent)

        # Main node
        if parent == uri_main:
            kwargs = {
                "color": COLORS.get(attributes["channel"], COLOR_OTHER),
                "label": f"{attributes['namespace'].upper()}/{attributes['name'].upper()}\n{attributes['version']}",
                "title": _friendly_title(attributes, children),
                "font": {
                    "size": 40,
                    "face": "courier",
                    "align": "left",
                },
                "mass": 2,
            }
        # Oth nod (dependencies)
        else:
            # Nodes that will not show label within them (user chose to hide them)
            kwargs = {
                "color": COLORS.get(attributes["channel"], COLOR_OTHER),
                "mass": 1,
                "opacity": 0.9,
            }
            if _collapse_node(attributes):
                kwargs = {
                    **kwargs,
                    "title": _friendly_title(attributes, children),
                    "mass": 1,
                    "opacity": 0.9,
                }
            # Nodes that will show label within them
            else:
                kwargs = {
                    **kwargs,
                    "label": _friendly_label(attributes),
                    "title": _friendly_title(attributes, children),
                    "font": {
                        "size": 20,
                        "face": "courier",
                        "align": "left",
                    },
                }

        # Change if step is private
        if attributes["kind"] == "private":
            kwargs["label"] = "🔒 " + kwargs.get("label", "")

        node = Node(
            id=parent,
            borderWidthSelected=5,
            margin=10,
            shape="box",
            borderWidth=5,
            **kwargs,
        )
        nodes.append(node)
        for child in children:
            edge = Edge(
                source=child,
                target=parent,
                width=5,
            )
            edges.append(edge)

    # config_builder = ConfigBuilder(nodes)
    # config = config_builder.build()

    # node_config = {
    #     "labelProperty": "label",
    #     "renderLabel": "true",
    # }

    # config.layout["hierarchical"]["enabled"] = True
    config = Config(
        width=1000,
        height=500,
        nodeHighlightBehavior=True,
        # highlightColor="#F7A7A6",
        # collapsible=True,
        # node=node_config,
        directed=True,
        physics=False,
        minVelocity=20,
        maxVelocity=1000,
        # nodeSpacing=10000,
        stabilization=False,
        fit=False,
        hierarchical=True,
        # nodeSpacing=200,
        # **kwargs
    )

    # config.physics["barnesHut"] = {"springConstant": 0, "avoidOverlap": 0.1}

    return agraph(nodes=nodes, edges=edges, config=config)
