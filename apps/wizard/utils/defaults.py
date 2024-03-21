import json
from typing import Any, Dict

from apps.wizard.utils.step_form import StepForm
from etl.paths import BASE_DIR

# PATH WIZARD CONFIG
WIZARD_VARIABLES_DEFAULTS_OLD = BASE_DIR / ".wizard"
WIZARD_VARIABLES_DEFAULTS = BASE_DIR / ".wizardcfg" / "defaults.json"


def load_wizard_defaults() -> Dict[str, Any]:
    """Load default wizard defaults.

    Additionally, if no default defaults file is found, one is created.
    """
    if WIZARD_VARIABLES_DEFAULTS.exists():
        with WIZARD_VARIABLES_DEFAULTS.open("r") as f:
            return json.load(f)
    else:
        # Migrate from old config
        if WIZARD_VARIABLES_DEFAULTS_OLD.exists():
            with WIZARD_VARIABLES_DEFAULTS_OLD.open("r") as f:
                defaults = json.load(f)
        defaults = {
            "template": {
                "meadow": {"generate_notebook": False},
                "garden": {"generate_notebook": False},
            }
        }
        # Create defaults file
        create_wizard_defaults(defaults)
        return defaults


def create_wizard_defaults(defaults: Dict[str, Any], overwrite: bool = False) -> None:
    """Create wizard defaults file.

    Set overwrite=True to overwrite the file if it already exists.
    """
    # Only create if it does not exist
    if (not WIZARD_VARIABLES_DEFAULTS.exists()) or overwrite:
        # Create the directories needed to contain the file, if they don't already exist
        WIZARD_VARIABLES_DEFAULTS.parent.mkdir(parents=True, exist_ok=True)
        with WIZARD_VARIABLES_DEFAULTS.open("w") as f:
            json.dump(defaults, f)


def update_wizard_defaults_from_form(form: StepForm) -> None:
    """Update wizard defaults file."""
    # Load config
    config = load_wizard_defaults()

    # Update config
    if form.step_name in ["meadow", "garden"]:
        form_dix = form.dict()
        config["template"][form.step_name]["generate_notebook"] = form_dix.get("generate_notebook", False)

    # Export config
    create_wizard_defaults(config, overwrite=True)
