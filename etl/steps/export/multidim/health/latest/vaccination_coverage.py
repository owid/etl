from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


### Add bullet points for description_key for combined views here
WHO_IMMUNIZATION = "This chart shows official estimates of national immunization coverage published by the WHO and UNICEF. The estimates include all WHO member states, even those that did not report 2023 data. For non-reporting countries, WHO uses statistical methods to extrapolate from previously reported data, ensuring global coverage can be assessed."
WHO_UNVACCINATED = "This chart shows the estimated  number of one-year-olds who have not received vaccinations for different diseases. These are calculated by multiplying immunization coverage estimates published by the WHO and UNICEF with by population counts from the United Nations World Population Prospects (UN WPP). For most vaccines, the denominator is the number of infants who survived their first year of life; for vaccines given at birth (hepatitis B birth dose and BCG), it is the number of live births."
WHO_VACCINATED = "This chart shows the estimated number of one-year-olds who have received vaccinations for different diseases. These are calculated by multiplying immunization coverage estimates published by the WHO and UNICEF with by population counts from the United Nations World Population Prospects (UN WPP). For most vaccines, the denominator is the number of infants who survived their first year of life; for vaccines given at birth (hepatitis B birth dose and BCG), it is the number of live births."
POPULATION_WEIGHT = "Global and regional vaccination coverage is calculated using population-weighted averages. In 2023, approximately 5% of countries did not report data, requiring extrapolation from their 2022 data to maintain complete global estimates."
ESTIMATE_SOURCES = "These estimates combine several sources: official administrative data from health facilities, coverage surveys that meet WHO quality standards, and other relevant information like vaccine supply issues or schedule changes. The accuracy of these estimates depends on how complete and reliable each country’s reporting systems are."
ALL_DISEASES = "The chart includes data for multiple childhood infections: [Diphtheria](#dod:diphtheria), [pertussis](#dod:pertussis) and [tetanus](#dod:tetanus) (3rd dose), [measles](#dod:measles) (1st dose), [hepatitis B](#dod:hepatitis-virus) (3rd dose), [polio](#dod:polio) (3rd dose), Haemophilus influenzae b (3rd dose), [rubella](#dod:rubella) (1st dose), [rotavirus](#dod:rotavirus) (final dose), [pneumococcal conjugate](dod:pneumococcal-conjugate-vaccine) (3rd dose), and [inactivated polio](#dod:inactivated-polio-vaccine) (first dose). These are serious illnesses that can cause severe complications and even death, especially in young children. Vaccination is a critical tool for preventing these diseases and protecting public health."


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=False)
    # Drop out the non-global vaccinations
    tb = tb.drop(
        columns=[
            "coverage__antigen_yfv",
            "coverage__antigen_mena_c",
        ]
    )

    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart", "SlopeChart", "DiscreteBar"],
        "hasMapTab": True,
        "tab": "chart",
        "yAxis": {"min": 0},
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

    CONFIG_GROUP = {
        "title": {
            "coverage": "Share of children vaccinated, by vaccine",
            "vaccinated": "Number of children vaccinated, by vaccine",
            "unvaccinated": "Number of children unvaccinated, by vaccine",
        },
        "subtitle": {
            "coverage": "Share of one-year-olds who have been vaccinated against a disease or a pathogen.",
            "vaccinated": "Estimated number of one-year-olds who have received vaccinations for different diseases.",
            "unvaccinated": "Estimated number of one-year-olds who have not received vaccinations for different diseases.",
        },
        "description_key": {
            "coverage": [WHO_IMMUNIZATION, POPULATION_WEIGHT, ESTIMATE_SOURCES, ALL_DISEASES],
            "vaccinated": [WHO_VACCINATED, POPULATION_WEIGHT, ESTIMATE_SOURCES, ALL_DISEASES],
            "unvaccinated": [WHO_UNVACCINATED, POPULATION_WEIGHT, ESTIMATE_SOURCES, ALL_DISEASES],
        },
    }
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
                    "chartTypes": ["SlopeChart", "LineChart", "DiscreteBar"],
                    "selectedFacetStrategy": "entity",
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                    "note": "This includes [diphtheria](#dod:diphtheria), [pertussis](#dod:pertussis) and [tetanus](#dod:tetanus) (3rd dose), [measles](#dod:measles) (1st dose), [hepatitis B](#dod:hepatitis-virus) (3rd dose), [polio](#dod:polio) (3rd dose), Haemophilus influenzae b (3rd dose), [rubella](#dod:rubella) (1st dose), [rotavirus](#dod:rotavirus) (final dose), [pneumococcal conjugate](dod:pneumococcal-conjugate-vaccine) (3rd dose), and [inactivated polio](#dod:inactivated-polio-vaccine) (first dose).",
                },
                "view_metadata": {
                    "title": "{title}",
                    #"title_public": "{title_public}",
                    "description_short": "{subtitle}",
                    "description_key": "{description_key}",
                    "presentation": {
                        "title_public": "{title_public}",
                    },
                },
            }
        ],
        params={
            "title": lambda view: CONFIG_GROUP["title"][view.dimensions["metric"]],
            "title_public": lambda view: CONFIG_GROUP["title"][view.dimensions["metric"]],
            "subtitle": lambda view: CONFIG_GROUP["subtitle"][view.dimensions["metric"]],
            "description_key": lambda view: CONFIG_GROUP["description_key"][view.dimensions["metric"]],
        },
    )

    c.save()
