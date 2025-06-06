#
#  to_graphviz.py
#

from typing import Optional

import rich_click as click

from etl.dag_helpers import load_dag
from etl.steps import filter_to_subgraph


@click.command(name="graphviz")
@click.argument("output_file")
@click.option(
    "--filter",
    help="Filter the DAG by regex",
)
@click.option(
    "--targets",
    help="Show target nodes.",
    is_flag=True,
    default=False,
)
def to_graphviz(output_file: str, filter: Optional[str] = None, targets: bool = False) -> None:
    """Generate a [Graphviz DOT file](https://graphviz.org/doc/info/lang.html) to see all dependencies.

    Saves the output as a file in `OUTPUT_PATH`."""
    dag = load_dag()
    if filter:
        dag = filter_to_subgraph(dag, includes=[filter])

    font_name = "Lato"

    nodes = set()
    for child, parents in dag.items():
        nodes.update(parents)
        if not child.startswith("grapher://"):
            nodes.add(child)

    with open(output_file, "w") as ostream:

        def p(x: str) -> None:
            print(x, file=ostream)

        def node(name: str) -> None:
            p(f'"{name}" [fontname="{font_name}"];')

        def link(parent: str, child: str) -> None:
            p(f'"{parent}" -> "{child}" [fontname="{font_name}"];')

        p("digraph D {")
        p('graph [rankdir="LR"];')
        for n in nodes:
            node(n)

        p("subgraph { rank=same")
        node("Snapshot")
        node("Github")
        p("}")

        if targets:
            p("subgraph { rank=same")
            node("Grapher")
            node("Garden")
            node("Meadow")
            node("Reference")
            p("}")

        p("subgraph { rank=same")
        node("Grapher DB")
        node("Data catalog")
        p("}")

        for child, parents in dag.items():
            if targets:
                if child.startswith("grapher://"):
                    for parent in parents:
                        link(parent, "Grapher DB")
                    continue

            for parent in parents:
                link(parent, child)

            if targets:
                if child.startswith("data://"):
                    if child.startswith("data://meadow"):
                        link(child, "Meadow")
                    elif child.startswith("data://garden"):
                        link(child, "Garden")
                    elif child.startswith("data://grapher"):
                        link(child, "Grapher")

        if targets:
            link("Meadow", "Data catalog")
            link("Garden", "Data catalog")
            link("Grapher", "Data catalog")
            link("Grapher", "Grapher DB")
        link("data://garden/reference", "Reference")
        link("Reference", "Data catalog")

        for n in nodes:
            if n.startswith("snapshot://"):
                link("Snapshot", n)

            if n.startswith("github://"):
                link("Github", n)

        for prefix in ["snapshot://", "data://grapher", "data://garden/", "data://meadow"]:
            p("subgraph {")
            p("rank = same;")
            for n in nodes:
                if n.startswith(prefix):
                    node(n)
            p("}")

        p("}")


if __name__ == "__main__":
    to_graphviz()
