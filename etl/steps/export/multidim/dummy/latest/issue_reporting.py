"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load grapher dataset.
    # ds = paths.load_dataset("cases_deaths")
    # tb = ds.read("cases_deaths", load_data=False)

    #
    # (optional) Adjust dimensions if needed
    #

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="issue_reporting",
    )

    c.group_views(
        [
            {
                "dimension": "view",
                "choice_new_slug": "view_combined",
                "view_metadata": {
                    "presentation": {
                        "title_public": "Combined View",
                    }
                },
            }
        ]
    )

    #
    # Save garden dataset.
    #
    c.save()
