"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_explorer_config()

    # Add annotations to all columns
    for view in config["views"]:
        if view["dimensions"]["metric"] in ("primary_energy", "secondary_energy", "final_energy"):
            view["config"]["relatedQuestionUrl"] = "https://ourworldindata.org/energy-definitions"
            view["config"]["relatedQuestionText"] = (
                "Primary, secondary, final, useful: What are the four ways of measuring energy?"
            )

    # Create explorer
    # NOTE: `avoid_duplicate_hack` is necessary to show annotations
    explorer = paths.create_explorer(config=config, explorer_name="ipcc-scenarios", avoid_duplicate_hack=True)

    # explorer.save(tolerate_extra_indicators=True)
    explorer.save()
