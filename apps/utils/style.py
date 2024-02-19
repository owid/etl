import rich_click as click


def set_rich_click_style() -> None:
    """Set rich click style.

    More details: https://github.com/ewels/rich-click
    """
    # click.rich_click.USE_RICH_MARKUP = True
    click.rich_click.USE_MARKDOWN = True
    click.rich_click.SHOW_ARGUMENTS = True
    # click.rich_click.STYLE_HEADER_TEXT = "bold"
    # click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
    # Show variable types under description
    click.rich_click.SHOW_METAVARS_COLUMN = False
    click.rich_click.APPEND_METAVARS_HELP = True
    click.rich_click.OPTION_ENVVAR_FIRST = True
