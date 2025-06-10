from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=False)

    # Create and save collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["coverage", "unvaccinated", "vaccinated"],
        dimensions=["antigen"],
        indicators_slug="metric",
    )

    c.group_views(
        groups={
            "dimension": "antigen",
            "choice_new_slug": "comparison",
            "view_config": {
                "hasMapTab": False,
                "addCountryMode": "change-country",
                "tab": "chart",
                "selectedFacetStrategy": "entity",
                "title": "Vaccination coverage",
                "subtitle": "Share of one-year-olds who have been immunized against a disease or a pathogen.",
                "note": "This includes diphtheria, pertussis and tetanus (3rd dose), measles (1st dose), hepatitis B (3rd dose), polio (3rd dose), Haemophilus influenzae b (3rd dose), rubella (1st dose), rotavirus (final dose), yellow fever (1st dose), and inactivated polio (first dose).",
            },
        }
    )

    c.save()
