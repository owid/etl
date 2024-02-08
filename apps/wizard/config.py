"""This modules help with the configuration of the wizard app.

It basically reads the configuration from .wizard.yml and renders the home page and other details.
"""
from typing import Literal

import yaml

from etl.paths import APPS_DIR

_config_path = APPS_DIR / "wizard" / "config.yml"


def load_wizard_config():
    """Load config."""
    with open(_config_path, "r") as file:
        config = yaml.safe_load(file)
    _check_wizard_config(config)
    return config


def _check_wizard_config(config: dict):
    """Check if the wizard config is valid."""
    app_properties_expected = ["title", "alias", "entrypoint", "emoji", "image_url"]

    # Check `etl` property
    assert "etl" in config, "etl property is required in wizard config!"
    assert "title" in config["etl"], "etl.title property is required in wizard config!"
    assert "description" in config["etl"], "etl.description property is required in wizard config!"
    assert "steps" in config["etl"], "etl.steps property is required in wizard config!"
    steps = config["etl"]["steps"]
    steps_expected = ["snapshot", "meadow", "garden", "fasttrack", "grapher"]
    for step_expected in steps_expected:
        assert step_expected in steps, f"{step_expected} property is required in etl.steps property in wizard config!"
        for prop in app_properties_expected:
            assert (
                prop in steps[step_expected]
            ), f"etl.steps.{step_expected}.{prop} property is required in etl.steps property in wizard config!"
    # Check `sections` property
    assert "sections" in config, "sections property is required in wizard config!"
    for section in config["sections"]:
        assert "title" in section, "sections.title property is required in wizard config!"
        assert "description" in section, "sections.description property is required in wizard config!"
        assert "apps" in section, "sections.apps property is required in wizard config!"
        for app in section["apps"]:
            for prop in app_properties_expected + ["description"]:
                assert (
                    prop in app
                ), f"sections.apps.{app['title']}.{prop} property is required in sections.apps property in wizard config!"


WIZARD_CONFIG = load_wizard_config()

# Phases accepted
_aliases = []
## Load all aliases
for section in WIZARD_CONFIG["sections"]:
    for app in section["apps"]:
        _aliases.append(app["alias"])
for step in WIZARD_CONFIG["etl"]["steps"].values():
    _aliases.append(step["alias"])
_aliases = tuple(_aliases + ["all"])
WIZARD_PHASES = Literal[_aliases]  # type: ignore
