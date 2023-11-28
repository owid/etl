from typing import Any, List


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
