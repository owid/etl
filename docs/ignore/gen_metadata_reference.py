from typing import Any, Dict, List

import mkdocs_gen_files

from etl.helpers import read_json_schema
from etl.paths import SCHEMAS_DIR

SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")
DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")
TEMPLATE_PROPERTY = """
{name}

!!! abstract "{type}{requirement_level}"

    === "Description"
        {description}

    {guidelines}
    {examples}
"""


def guidelines_to_markdown(guidelines: List[Any]) -> str:
    """Render guidelines to markdown from given list in schema."""
    text = ""
    for guideline in guidelines:
        # Main guideline
        if isinstance(guideline[0], str):
            # Add main guideline
            text += f"\n\t\t- {guideline[0]}"
        else:
            raise TypeError("The first element of an element in `guidelines` must be a string!")

        # Additions to the guideline (nested bullet points, exceptions, etc.)
        if len(guideline) == 2:
            if isinstance(guideline[1], dict):
                # Sanity checks
                if "type" not in guideline[1]:
                    raise ValueError("The second element of an element in `guidelines` must have a `type` key!")
                if "value" not in guideline[1]:
                    raise ValueError("The second element of an element in `guidelines` must have a `value` key!")

                # Render exceptions
                if guideline[1]["type"] == "exceptions":
                    text += " Exceptions:"
                    for exception in guideline[1]["value"]:
                        text += f"\n\t\t\t- {exception}"
                # Render nested list
                elif guideline[1]["type"] == "list":
                    for subitem in guideline[1]["value"]:
                        text += f"\n\t\t\t- {subitem}"
                # Exception
                else:
                    raise ValueError(f"Unknown guideline type: {guideline[1]['type']}!")
            else:
                raise TypeError("The second element of an element in `guidelines` must be a dictionary!")

        # Element in guideliens is more than 2 items long
        if len(guideline) > 2:
            raise ValueError("Each element in `guidelines` must have at most 2 elements!")
    return text


def examples_to_markdown(examples: List[str], examples_bad: List[Any]) -> str:
    """Render examples (good and bad) to markdown from given lists in schema."""
    text = ""
    # Only good examples
    if len(examples_bad) == 0:
        print("No bad examples for this property!")
        text = ""
        for example in examples:
            text += f"\n\t\t- «`{example}`» "
        return text
    # Sanity check
    elif len(examples) != len(examples_bad):
        raise ValueError(
            f"Examples and examples_bad must have the same length! Examples: {examples}, examples_bad: {examples_bad}"
        )
    # Combine good and bad examples
    text = """
        | ✅ DO      | ❌ DON'T  |
        | ----------- | --------- |"""
    for good, bad in zip(examples, examples_bad):
        assert isinstance(bad, list), "Bad examples must be a list!"
        bad = [f"«`{b}`»" for b in bad]
        text += f"\n\t\t| «`{good}`» | {', '.join(bad)} |"
    return text


def render_prop_doc(prop: Dict[str, Any], prop_name: str, level: int = 1, top_level: bool = False) -> str:
    """Render a particular property."""
    prop_title = f"{'#' * (level)} `{prop_name}`"
    if top_level:
        return f"""
{prop_title}

{prop["description"]}
"""
    # Prepare requirement_level
    requirement_level = ""
    if "requirement_level" in prop:
        requirement_level = f" | {prop['requirement_level']}"
    # Prepare guidelines
    guidelines = ""
    if "guidelines" in prop and prop.get("guidelines"):
        guidelines = f"""
    === "Guidelines"
        {guidelines_to_markdown(prop['guidelines'])}
    """
    # Prepare examples
    examples = ""
    if "examples" in prop and prop.get("examples"):
        examples = f"""
    === "Examples"
        {examples_to_markdown(prop['examples'], prop['examples_bad'])}
    """

    # Bake documentation for property
    if "type" not in prop:
        if "oneOf" not in prop:
            raise ValueError(f"Property {prop_name} has no type!")
        type_ = ', '.join([f"`{p['type']}`" for p in prop["oneOf"]])
    else:
        if isinstance(prop["type"], list):
            type_ = ', '.join([f"`{p}`" for p in prop["type"]])
        else:
            type_ = f"`{prop['type']}`"
    prop_docs = TEMPLATE_PROPERTY.format(**{
        "name": prop_title,
        "type": type_,
        "description": prop["description"],
        "requirement_level": requirement_level,
        "guidelines": guidelines,
        "examples": examples,
    })
    return prop_docs


def render_props_recursive(prop: Dict[str, Any], prop_name: str, level: int, text: str) -> str:
    """Render all properties."""
    print(prop_name)
    if "type" in prop and prop["type"] == "object":
        text += render_prop_doc(prop, prop_name=prop_name, level=level, top_level=True)

        # Do not go deeper
        if prop_name == "tables[].variables[].presentation.grapher_config":
            return text

        if "properties" not in prop and "additionalProperties" in prop:
            props_children = prop["additionalProperties"]["properties"]
            prop_name = f"{prop_name}[]"
        elif "properties" in prop:
            props_children = prop["properties"]
        else:
            return text
        for prop_name_child, prop_child in props_children.items():
            text += render_props_recursive(prop_child, prop_name=f"{prop_name}.{prop_name_child}", level=level + 1, text="")
    else:
        text += render_prop_doc(prop, prop_name=prop_name, level=level)
    return text


def render_origin() -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]
    documentation = render_props_recursive(origin, "origin", 1, "")
    return documentation


def render_dataset() -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    dataset = DATASET_SCHEMA["properties"]["dataset"]
    documentation = render_props_recursive(dataset, "dataset", 1, "")
    return documentation


def render_table() -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    dataset = DATASET_SCHEMA["properties"]["tables"]
    documentation = render_props_recursive(dataset, "tables", 1, "")
    return documentation


# Origin reference
with mkdocs_gen_files.open("architecture/metadata/reference-origin.md", "w") as f:
    text_origin = render_origin()
    print(text_origin, file=f)

# Dataset reference
with mkdocs_gen_files.open("architecture/metadata/reference-dataset.md", "w") as f:
    text_dataset = render_dataset()
    print(text_dataset, file=f)

# Tables reference
with mkdocs_gen_files.open("architecture/metadata/reference-tables.md", "w") as f:
    text_tables = render_table()
    print(text_tables, file=f)



# with open("docs/architecture/metadata/reference2.md", "w") as f:
#     text_origin = render_origin()
#     text_dataset = render_dataset()
#     tet_tables = render_table()

#     text = f"{text_origin}\n\n{text_dataset}\n\n{tet_tables}"
#     # text = "hello"
#     print(text, file=f)
