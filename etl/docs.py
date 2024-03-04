from typing import Any, Dict, List, Optional

from etl.helpers import read_json_schema
from etl.paths import SCHEMAS_DIR

SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")
DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")
TEMPLATE_PROPERTY = """
{name}

*type*: {type}{requirement_level}

{description}

{guidelines}
{examples}
{faqs}

---

"""


def render_prop_doc(prop: Dict[str, Any], prop_name: str, level: int = 1, top_level: bool = False) -> str:
    """Render a particular property."""
    prop_title = f"{'#' * (level)} `{prop_name}`"
    if top_level:
        return f"""
{prop_title}

{prop.get("description", "")}
"""
    # Prepare requirement_level
    requirement_level = ""
    if "requirement_level" in prop:
        if "required" in prop["requirement_level"]:
            requirement_level = f" | {{=={prop['requirement_level']}==}}"
        else:
            requirement_level = f" | {prop['requirement_level']}"
    # Prepare guidelines
    guidelines = ""
    if "guidelines" in prop and prop.get("guidelines"):
        guidelines = f"""=== ":fontawesome-solid-list:  Guidelines"
        {guidelines_to_markdown(prop['guidelines'], extra_tab=1)}
    """
    # Prepare examples
    examples = ""
    if "examples" in prop and prop.get("examples"):
        examples = f"""=== ":material-note-edit: Examples"
        {examples_to_markdown(prop['examples'], prop['examples_bad'], extra_tab=1)}
    """

    # Prepare FAQs
    faqs = ""
    if "faqs" in prop and prop.get("examples"):
        faqs = f"""=== ":material-chat-question: FAQs"
        {faqs_to_markdown(prop['faqs'], extra_tab=1)}
    """

    # Bake documentation for property
    if "type" not in prop:
        if "oneOf" not in prop:
            raise ValueError(f"Property {prop_name} has no type!")
        type_ = ", ".join([f"`{p['type']}`" for p in prop["oneOf"]])
    else:
        if isinstance(prop["type"], list):
            type_ = ", ".join([f"`{p}`" for p in prop["type"]])
        else:
            type_ = f"`{prop['type']}`"
    prop_docs = TEMPLATE_PROPERTY.format(
        **{
            "name": prop_title,
            "type": type_,
            "description": prop["description"],
            "requirement_level": requirement_level,
            "guidelines": guidelines,
            "examples": examples,
            "faqs": faqs,
        }
    )
    return prop_docs


def render_props_recursive(
    prop: Dict[str, Any],
    prop_name: str,
    level: int,
    text: str,
    ignore_fields: Optional[List[str]] = None,
    render_top_as_scalar: bool = True,
) -> str:
    """Render all properties."""
    if ignore_fields is None:
        ignore_fields = []

    if "type" in prop and prop["type"] == "object":
        text += render_prop_doc(prop, prop_name=prop_name, level=level, top_level=True)

        # Do not got deeper either
        if prop_name in ignore_fields:
            return text

        if "properties" not in prop and "additionalProperties" in prop:
            props_children = prop["additionalProperties"]["properties"]
            if not render_top_as_scalar:
                prop_name = f"{prop_name}[]"
        elif "properties" in prop:
            props_children = prop["properties"]
        else:
            return text
        props_children_sorted = dict(sorted(props_children.items()))
        for prop_name_child, prop_child in props_children_sorted.items():
            text += render_props_recursive(
                prop_child,
                prop_name=f"{prop_name}.{prop_name_child}",
                level=level + 1,
                text="",
                ignore_fields=ignore_fields,
            )
    else:
        text += render_prop_doc(prop, prop_name=prop_name, level=level)
    return text


def render_origin(level: int = 1) -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]
    documentation = render_props_recursive(origin, "origin", level, "")
    return documentation


def render_dataset(level: int = 1) -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    dataset = DATASET_SCHEMA["properties"]["dataset"]
    documentation = render_props_recursive(dataset, "dataset", level, "")
    return documentation


def render_table(level: int = 1) -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    tables = DATASET_SCHEMA["properties"]["tables"]
    documentation = render_props_recursive(
        tables, "table", level, "", ignore_fields=["table.variables", "table.common"]
    )
    return documentation


def render_indicator(level: int = 1) -> str:
    """Render documentation for origin."""
    # Rendering of 'snapshot' is only meta.origin and meta.license
    ## Origin
    variables = DATASET_SCHEMA["properties"]["tables"]["additionalProperties"]["properties"]["variables"]
    documentation = render_props_recursive(
        variables, "variable", level, "", ignore_fields=["variable.presentation.grapher_config"]
    )
    return documentation


def guidelines_to_markdown(guidelines: List[Any], extra_tab: int = 0) -> str:
    """Render guidelines to markdown from given list in schema."""
    tab = "\t" * extra_tab
    text = ""
    for guideline in guidelines:
        text = _guideline_to_markdown(text, tab, guideline)
    return text


def _guideline_to_markdown(text: str, tab: str, guideline):
    # Main guideline
    if isinstance(guideline, str):
        text += f"\n{tab}- {guideline}"
    else:
        if isinstance(guideline[0], str):
            # Add main guideline
            text += f"\n{tab}- {guideline[0]}"
        else:
            raise TypeError(f"The first element of an element in `guidelines` must be a string! {guideline}")

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
                    text += " **Exceptions:**"
                    for sub_guideline in guideline[1]["value"]:
                        text = _guideline_to_markdown(text, f"{tab}\t", sub_guideline)
                    # for exception in guideline[1]["value"]:
                    #     text += f"\n{tab}\t- {exception}"
                # Render nested list
                elif guideline[1]["type"] == "list":
                    for sub_guideline in guideline[1]["value"]:
                        text = _guideline_to_markdown(text, f"{tab}\t", sub_guideline)
                    # for subitem in guideline[1]["value"]:
                    #     text += f"\n{tab}\t- {subitem}"
                # Exception
                else:
                    raise ValueError(f"Unknown guideline type: {guideline[1]['type']}!")
            else:
                raise TypeError("The second element of an element in `guidelines` must be a dictionary!")

        # Element in guideliens is more than 2 items long
        if len(guideline) > 2:
            raise ValueError(
                f"Each element in `guidelines` must have at most 2 elements! Found {len(guideline)} instead in '{guideline}'!"
            )

    return text


def examples_to_markdown(
    examples: List[str],
    examples_bad: List[Any],
    extra_tab: int = 0,
    do_sign: str = ":material-check:",
    dont_sign: str = ":material-close:",
) -> str:
    """Render examples (good and bad) to markdown from given lists in schema."""
    tab = "\t" * extra_tab
    text = ""
    # Only good examples
    if len(examples_bad) == 0:
        print("No bad examples for this property!")
        text = ""
        for example in examples:
            text += f"\n\n{tab}{do_sign} «`{example}`» "
        return text
    # Sanity check
    elif len(examples) != len(examples_bad):
        raise ValueError(
            f"Examples and examples_bad must have the same length! Examples: {examples}, examples_bad: {examples_bad}"
        )
    # Combine good and bad examples
    text = f"""
{tab}| {do_sign} DO      | {dont_sign} DON'T  |
{tab}| ----------- | --------- |"""
    for good, bad in zip(examples, examples_bad):
        assert isinstance(bad, list), "Bad examples must be a list!"
        bad = [f"«`{b}`»" for b in bad]
        text += f"\n{tab}| «`{good}`» | {', '.join(bad)} |"
    return text


def faqs_to_markdown(faqs: List[Any], extra_tab: int = 0) -> str:
    """Render FAQs to markdown from given list in schema."""
    tab = "\t" * extra_tab
    texts = []
    for faq in faqs:
        text = ""
        if not isinstance(faq, dict):
            raise TypeError("Each element in `faqs` must be a dictionary!")

        # Get question
        if "question" in faq and isinstance(faq["question"], str):
            # Add main guideline
            text += f"\n{tab}**_{faq['question']}_**"
        else:
            raise TypeError("Check that faqs element has the key 'question' and that it is a string!")

        # Get answer
        if "answer" in faq and isinstance(faq["question"], str):
            # Add main guideline
            text += f"\n\n{tab}{faq['answer']}"
        else:
            raise TypeError("Check that faqs element has the key 'answer' and that it is a string!")

        # Get link
        if "link" in faq and isinstance(faq["question"], str):
            # Add main guideline
            text += f"\n\n{tab}[See discussion on Github]({faq['link']})"

        texts.append(text)

    text = "\n\n".join(texts)
    return text
