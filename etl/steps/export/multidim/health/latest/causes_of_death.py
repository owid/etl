import structlog

from etl.collection.model.view import View
from etl.collection.utils import group_views_legacy
from etl.helpers import PathFinder

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    table = paths.load_dataset("gbd_cause").read("gbd_cause_deaths", load_data=False)

    # Create collection
    config = paths.load_collection_config()
    c = paths.create_collection(
        config=config,
        tb=table,
    )

    # Add views for all dimensions
    grouped_views = group_views_legacy(c.to_dict()["views"], by=["age", "metric"])
    grouped_views = [View.from_dict(view) for view in grouped_views]
    for view in grouped_views:
        view.dimensions["cause"] = "Side-by-side comparison of causes"
    c.views.extend(grouped_views)

    # Sort choices
    c.sort_choices(
        {
            "age": [
                "All ages",
                "Age-standardized",
                "<5 years",
                "5-14 years",
                "15-49 years",
                "50-69 years",
                "70+ years",
            ]
        }
    )

    # Limit views to something manageable. This will ahve to improved before publishing.
    MAX_INDICATORS = 10
    for view in c.views:
        if len(view.indicators.y) > MAX_INDICATORS:
            log.warning(f"Limiting view with dimensions {view.dimensions} to {MAX_INDICATORS} indicators.")
            view.indicators.y = view.indicators.y[:MAX_INDICATORS]

    # Save the collection
    c.save()
