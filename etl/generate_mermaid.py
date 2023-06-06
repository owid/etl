#
#  to_graphviz.py
#

import click
import ruamel.yaml

from etl.paths import STEP_DIR
from etl.steps import extract_step_attributes, filter_to_subgraph, load_dag


@click.command()
@click.argument("path")
def generate_mermaid(path: str) -> None:
    """
    Generate a readme.TABLENAME.md file with a mermaid graph of the dependencies of the table.
    Call it like this:
    poetry run generate-mermaid garden/who/2022-07-17/who_vaccination
    """

    file_path = STEP_DIR / "data" / (path + ".py")

    if not file_path.exists():
        raise Exception(f"File {file_path} does not exist - can't generate mermaid graph")

    dag = load_dag()
    dag = filter_to_subgraph(dag, includes=[path])

    mermaid = "flowchart TD\n"

    internal_ids = {}
    current_id = 0

    for child, parents in dag.items():
        internal_ids[child] = current_id
        current_id += 1
    for child, parents in dag.items():
        for parent in parents:
            parent_id = internal_ids[parent]
            attributes = extract_step_attributes(parent)


            path = "https://github.com/owid/etl/tree/master/"
            if attributes["channel"] == "snapshot":
                namespace = attributes["namespace"]
                version = attributes["version"]
                name = attributes["name"]
                path += f"{namespace}/{version}/{name}"
            elif attributes["channel"] == "walden":
                path = "https://github.com/owid/walden/tree/master/owid/walden/index/"
                namespace = attributes["namespace"]
                version = attributes["version"]
                name = attributes["name"]
                path += f"{namespace}/{version}/{name}"
            elif attributes["channel"] == "github" or attributes["channel"] == "etag":
                path = "https://github.com/"
                path += attributes["name"]
            else:
                path += "etl/steps/data/"
                channel = attributes["channel"]
                namespace = attributes["namespace"]
                version = attributes["version"]
                name = attributes["name"]
                path += f"{channel}/{namespace}/{version}/{name}.py"

            mermaid += (f"    {parent_id}[\"{parent}\"] --> {internal_ids[child]}[\"{child}\"]\n")
            mermaid += (f"    click {parent_id} href \"{path}\"\n")

    about_this_dataset = ""
    metadata_filename = file_path.with_suffix(".meta.yml")
    if metadata_filename.exists():

        with open(metadata_filename, "r") as f:
            yaml = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)
            title = yaml.get("dataset", {}).get("title", "")
            about_this_dataset = ""
            if title:
                about_this_dataset = f"""# {title}\n\n"""
            about_this_dataset += "## About this dataset\n\n"
            about_this_dataset += yaml.get("dataset", {}).get("description", "")

    readme_path = file_path.with_suffix(".readme.md")
    readme_path.write_text(f"""{about_this_dataset}

## Dependencies

This diagram shows the inputs that are used in constructing this table. Dependencies are tracked
on the level of "datasets" only, where one "dataset" is a collection of tables in one directory.

To make sense of these dependencies, it helps to understand our terminology of the different processing levels,
namely snaphots/walden, then meadow, then garden and finally grapher. See [our documentation](https://docs.owid.io/projects/etl/en/latest/) for more details.

```mermaid
{mermaid}
```
""")
    print(f"Generated {readme_path}")


if __name__ == "__main__":
    generate_mermaid()
