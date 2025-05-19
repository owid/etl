"""Load a meadow dataset and create a garden dataset."""

from etl.collection.model.view import View
from etl.collection.utils import group_views
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

    # Load grapher dataset.
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

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
    grouped_views = group_views(c.to_dict()["views"], by=["answer"])
    grouped_views = [View.from_dict(view) for view in grouped_views]
    for view in grouped_views:
        view.dimensions["institution"] = "Side-by-side comparison of institutions"
        choice_names = c.get_choice_names("answer")
        answer = choice_names.get(view.dimensions["answer"])
        view.config = {
            **(view.config or {}),
            "hasMapTab": False,
            "chartTypes": ["DiscreteBar"],
            "tab": "chart",
            "facettingLabelByYVariables": "institution",
            "selectedFacetStrategy": "metric",
            "title": f"How many of the following people do you think are involved in corruption? {answer}",
            "subtitle": f'Percentage of respondents who answered "{answer}" to the question "How many of the following people do you think are involved in corruption?".',
        }

    c.views.extend(grouped_views)

    #
    # Save garden dataset.
    #
    c.save()
