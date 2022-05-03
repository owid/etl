#
#  to_graphviz.py
#

import click

from etl.steps import load_dag


@click.command()
@click.argument("output_file")
def to_graphviz(output_file: str) -> None:
    """
    Generate a DOT file that can be rendered by Graphviz to see all dependencies.
    """
    dag = load_dag()

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
        node("Walden")
        node("Github")
        p("}")

        p("subgraph { rank=same")
        node("Garden")
        node("Meadow")
        node("Reference")
        p("}")

        p("subgraph { rank=same")
        node("Grapher DB")
        node("Data catalog")
        p("}")

        for child, parents in dag.items():
            if child.startswith("grapher://"):
                for parent in parents:
                    link(parent, "Grapher DB")
                continue

            for parent in parents:
                link(parent, child)

            if child.startswith("data://"):
                if child.startswith("data://meadow"):
                    link(child, "Meadow")
                elif child.startswith("data://garden"):
                    link(child, "Garden")

        link("Meadow", "Data catalog")
        link("Garden", "Data catalog")
        link("data://garden/reference", "Reference")
        link("Reference", "Data catalog")

        for n in nodes:
            if n.startswith("walden://"):
                link("Walden", n)

            if n.startswith("github://"):
                link("Github", n)

        for prefix in ["walden://", "data://garden/", "data://meadow"]:
            p("subgraph {")
            p("rank = same;")
            for n in nodes:
                if n.startswith(prefix):
                    node(n)
            p("}")

        p("}")


if __name__ == "__main__":
    to_graphviz()
