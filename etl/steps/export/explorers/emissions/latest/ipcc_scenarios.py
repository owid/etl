"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_collection_config()

    # Add annotations to all columns
    for view in config["views"]:
        if view["dimensions"]["metric"] in ("primary_energy", "secondary_energy", "final_energy"):
            view["config"]["relatedQuestionUrl"] = "https://ourworldindata.org/energy-definitions"
            view["config"]["relatedQuestionText"] = (
                "Primary, secondary, final, useful: What are the four ways of measuring energy?"
            )

    # Create explorer
    explorer = paths.create_collection(
        config=config,
        short_name="ipcc-scenarios",
        explorer=True,
    )

    # explorer.save(tolerate_extra_indicators=True)
    explorer.save()
