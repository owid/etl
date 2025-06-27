"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "chartTypes": ["DiscreteBar"],
    "tab": "map",
    "originUrl": "ourworldindata.org/corruption",
}


def run() -> None:
    #
    # Load inputs.
    #

    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # Load grapher dataset.
    ds = paths.load_dataset("corruption_barometer")
    tb = ds.read("corruption_barometer")

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        dimensions={
            "question": [
                "How many of the following people do you think are involved in corruption, or haven’t you heard enough about them to say? "
            ],
            "institution": [
                "Business executives",
                "Government officials",
                "Judges and magistrates",
                "Local government councilors",
                "MPs or senators",
                "Police",
                "Prime Minister / President",
                "Religious leaders",
                "Tax officials",
            ],
            "answer": [
                "Most or all of them",
                "Some or none of them",
                "Don't know / haven't heard",
            ],
        },
        common_view_config=MULTIDIM_CONFIG,
    )

    CHOICE_NAMES = c.get_choice_names("answer")

    # Add grouped view
    c.group_views(
        groups=[
            {
                "dimension": "institution",
                "choice_new_slug": "side_by_side",
                "view_config": {
                    "hasMapTab": False,
                    "chartTypes": ["DiscreteBar"],
                    "tab": "chart",
                    # "facettingLabelByYVariables": "institution",
                    # "selectedFacetStrategy": "metric",
                    "title": "{title_public}",
                    "subtitle": 'Percentage of respondents who answered {answer} to the question "How many of the following people do you think are involved in corruption?".',
                },
                "view_metadata": {
                    "description_short": 'Percentage of respondents who answered "{answer}" to the question "How many of the following people do you think are involved in corruption?".',
                    "presentation": {"title_public": "{title_public}"},
                },
            },
        ],
        params={
            "answer": lambda view: CHOICE_NAMES.get(view.dimensions["answer"]),
            "title_public": lambda view: f"How many of the following people do you think are involved in corruption? {view.dimensions['answer']}",
        },
    )
    # Sort choices alphabetically
    c.sort_choices({"institution": lambda x: sorted(x)})

    #
    # Save garden dataset.
    #
    c.save()
