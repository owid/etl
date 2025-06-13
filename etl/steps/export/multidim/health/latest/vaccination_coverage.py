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

    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart", "SlopeChart"],
        "hasMapTab": True,
        "tab": "chart",
    }
    # Create and save collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["coverage", "unvaccinated", "vaccinated"],
        dimensions=["antigen"],
        indicators_slug="metric",
        common_view_config=common_view_config,
    )

    c.group_views(
        groups=[
            {
                "dimension": "antigen",
                "choice_new_slug": "comparison",
                "choices": ["MCV1", "HEPB3", "DTPCV3", "IPV1", "POL3", "HIB3", "RCV1", "PCV3", "ROTAC"],
                "view_config": {
                    "hasMapTab": False,
                    "addCountryMode": "change-country",
                    "tab": "chart",
                    "chartTypes": ["SlopeChart", "LineChart"],
                    "selectedFacetStrategy": "entity",
                    "title": "Vaccination coverage",
                    "subtitle": "Share of one-year-olds who have been immunized against a disease or a pathogen.",
                    "note": "This includes [diphtheria](#dod:diphtheria), [pertussis](#dod:pertussis) and [tetanus](#dod:tetanus) (3rd dose), [measles](#dod:measles) (1st dose), [hepatitis B](#dod:hepatitis-virus) (3rd dose), [polio](#dod:polio) (3rd dose), Haemophilus influenzae b (3rd dose), [rubella](#dod:rubella) (1st dose), [rotavirus](#dod:rotavirus) (final dose), and [inactivated polio](#dod:inactivated-polio-vaccine) (first dose).",
                },
                "view_metadata": {
                    "description_short": "Share of one-year-olds who have been immunized against a disease or a pathogen.",
                },
            }
        ]
    )

    for view in c.views:
        metric = view.dimensions["metric"]
        antigen = view.dimensions["antigen"]
        # print(f"Creating view for {antigen} - {metric}")

        view.config = view.config.copy()

        if (antigen == "comparison") & (metric == "vaccinated"):
            view.config["title"] = "Number of one-year-olds who have had each vaccination"
            view.config["subtitle"] = (
                "Estimated number of one-year-olds who have received vaccinations for different diseases."
            )
            view.config["note"] = (
                "This includes [diphtheria](#dod:diphtheria), [pertussis](#dod:pertussis) and [tetanus](#dod:tetanus) "
                "(3rd dose), [measles](#dod:measles) (1st dose), [hepatitis B](#dod:hepatitis-virus) (3rd dose), "
                "[polio](#dod:polio) (3rd dose), Haemophilus influenzae b (3rd dose), [rubella](#dod:rubella) (1st dose), "
                "[rotavirus](#dod:rotavirus) (final dose), and [inactivated polio](#dod:inactivated-polio-vaccine) (first dose)."
            )
            view.metadata = {
                "description_short": "Estimated number of one-year-olds who have had vaccinations for different diseases.",
            }
        elif (antigen == "comparison") & (metric == "unvaccinated"):
            view.config["title"] = "Number of one-year-olds who have not had each vaccination"
            view.config["subtitle"] = (
                "Estimated number of one-year-olds who have not received vaccinations for different diseases."
            )
            view.config["note"] = (
                "This includes [diphtheria](#dod:diphtheria), [pertussis](#dod:pertussis) and [tetanus](#dod:tetanus) "
                "(3rd dose), [measles](#dod:measles) (1st dose), [hepatitis B](#dod:hepatitis-virus) (3rd dose), "
                "[polio](#dod:polio) (3rd dose), Haemophilus influenzae b (3rd dose), [rubella](#dod:rubella) (1st dose), "
                "[rotavirus](#dod:rotavirus) (final dose), and [inactivated polio](#dod:inactivated-polio-vaccine) (first dose)."
            )

            view.metadata = {
                "description_short": "Estimated number of one-year-olds who have not received vaccinations for different diseases.",
            }
    c.save()
