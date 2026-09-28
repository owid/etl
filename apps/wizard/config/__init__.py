"""This module helps with the configuration of the wizard app.

It reads `config.yml` (the list of pages, grouped in sections) and derives what the navigation and
the home page need: which apps are enabled in the current environment, and the alias of each app.
"""

import yaml

from etl.config import ENV
from etl.paths import APPS_DIR

_config_path = APPS_DIR / "wizard" / "config" / "config.yml"


def load_wizard_config():  # -> Any:
    """Load config."""
    # Load file
    with open(_config_path) as file:
        config = yaml.safe_load(file)
    # Some input checks
    _check_wizard_config(config)

    # Add `enable` property to each app
    def _get_enable(props):
        # Default for `disable` is False
        if "disable" not in props:
            return True
        else:
            disable = props.get("disable", False)
            # Disable in *all* settings
            if isinstance(disable, bool):
                return not disable
            # Disable in some settings
            elif isinstance(disable, dict):
                if ENV == "staging":
                    return not disable.get("staging", False)
                if ENV == "production":
                    return not disable.get("production", False)
                elif ENV == "dev":
                    return not disable.get("dev", False)

        raise ValueError(f"Invalid disable property: {disable}")

    ## Sections
    for section in config["sections"]:
        for app in section["apps"]:
            app["enable"] = _get_enable(app)
    ## Section legacy
    if "legacy" in config:
        for app in config["legacy"]["apps"]:
            app["enable"] = _get_enable(app)

    # Add alias if not there by lowering the title
    for section in config["sections"]:
        for app in section["apps"]:
            if "alias" not in app:
                app["alias"] = app["title"].lower().replace(" ", "-")
    return config


def _check_wizard_config(config: dict):
    """Check if the wizard config is valid."""
    pages_properties_expected = ["title", "entrypoint", "icon", "alias", "description"]

    # Check `sections` property
    assert "sections" in config, "sections property is required in wizard config!"
    for section in config["sections"]:
        assert "title" in section, "`sections.title` property is required in wizard config!"
        assert "description" in section, "`sections.description` property is required in wizard config!"
        assert "apps" in section, "`sections.apps` property is required in wizard config!"
        for app in section["apps"]:
            for prop in pages_properties_expected:
                assert prop in app, (
                    f"`sections.apps.{app['title']}.{prop}` property is required in sections.apps property in wizard config!"
                )


WIZARD_CONFIG = load_wizard_config()

# Get all pages by alias
_pages = [ww for w in WIZARD_CONFIG["sections"] for ww in w["apps"]]
PAGES_BY_ALIAS = {
    **WIZARD_CONFIG["main"],
    **{p["alias"]: {k: v for k, v in p.items() if k not in ["alias"]} for p in _pages},
}
