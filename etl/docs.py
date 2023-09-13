from typing import Any, List


def guidelines_to_markdown(guidelines: List[Any], extra_tab: int = 0) -> str:
    """Render guidelines to markdown from given list in schema."""
    tab = "\t" * extra_tab
    text = ""
    for guideline in guidelines:
        # Main guideline
        if isinstance(guideline[0], str):
            # Add main guideline
            text += f"\n{tab}- {guideline[0]}"
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
                        text += f"\n{tab}\t- {exception}"
                # Render nested list
                elif guideline[1]["type"] == "list":
                    for subitem in guideline[1]["value"]:
                        text += f"\n{tab}\t- {subitem}"
                # Exception
                else:
                    raise ValueError(f"Unknown guideline type: {guideline[1]['type']}!")
            else:
                raise TypeError("The second element of an element in `guidelines` must be a dictionary!")

        # Element in guideliens is more than 2 items long
        if len(guideline) > 2:
            raise ValueError("Each element in `guidelines` must have at most 2 elements!")
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
