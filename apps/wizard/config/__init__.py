"""This modules help with the configuration of the wizard app.

It basically reads the configuration from .wizard.yml and renders the home page and other details.
"""
from typing import Literal

import yaml

from etl.config import ENV
from etl.paths import APPS_DIR

_config_path = APPS_DIR / "wizard" / "config" / "config.yml"
WIZARD_PORT = 8053


def load_wizard_config():  # -> Any:
    """Load config."""
    # Load file
    with open(_config_path, "r") as file:
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

    ## ETL steps
    for _, step in config["etl"]["steps"].items():
        step["enable"] = _get_enable(step)
    ## Sections
    for section in config["sections"]:
        for app in section["apps"]:
            app["enable"] = _get_enable(app)

    # Add alias if not there by lowering the title
    for _, step in config["etl"]["steps"].items():
        if "alias" not in step:
            step["alias"] = step["title"].lower().replace(" ", "-")
    for section in config["sections"]:
        for app in section["apps"]:
            if "alias" not in app:
                app["alias"] = app["title"].lower().replace(" ", "-")
    return config


def _check_wizard_config(config: dict):
    """Check if the wizard config is valid."""
    _app_properties_expected = ["title", "entrypoint", "icon", "image_url"]
    pages_properties_expected = _app_properties_expected + ["alias", "description"]
    etl_steps_properties_expected = _app_properties_expected

    # Check `etl` property
    assert "etl" in config, "`etl` property is required in wizard config!"
    assert "title" in config["etl"], "`etl.title` property is required in wizard config!"
    assert "description" in config["etl"], "`etl.description` property is required in wizard config!"
    assert "steps" in config["etl"], "`etl.steps` property is required in wizard config!"
    steps = config["etl"]["steps"]
    steps_expected = ["snapshot", "express", "meadow", "garden", "fasttrack", "grapher"]
    for step_expected in steps_expected:
        assert step_expected in steps, f"{step_expected} property is required in etl.steps property in wizard config!"
        for prop in etl_steps_properties_expected:
            assert (
                prop in steps[step_expected]
            ), f"`etl.steps.{step_expected}.{prop}` property is required in `etl.steps` property in wizard config!"
    # Check `sections` property
    assert "sections" in config, "sections property is required in wizard config!"
    for section in config["sections"]:
        assert "title" in section, "`sections.title` property is required in wizard config!"
        assert "description" in section, "`sections.description` property is required in wizard config!"
        assert "apps" in section, "`sections.apps` property is required in wizard config!"
        for app in section["apps"]:
            for prop in pages_properties_expected:
                assert (
                    prop in app
                ), f"`sections.apps.{app['title']}.{prop}` property is required in sections.apps property in wizard config!"


WIZARD_CONFIG = load_wizard_config()

# Phases accepted
_aliases = []
## Aliases from pages
for section in WIZARD_CONFIG["sections"]:
    for app in section["apps"]:
        _aliases.append(app["alias"])
## Add aliases from etl steps and 'all'
_aliases = tuple(_aliases + list(WIZARD_CONFIG["etl"]["steps"].keys()) + ["all"])
WIZARD_PHASES = Literal[_aliases]  # type: ignore

# Get all pages by alias
_pages = [ww for w in WIZARD_CONFIG["sections"] for ww in w["apps"]]
PAGES_BY_ALIAS = {
    **WIZARD_CONFIG["main"],
    **WIZARD_CONFIG["etl"]["steps"],
    **{p["alias"]: {k: v for k, v in p.items() if k not in ["alias"]} for p in _pages},
}
