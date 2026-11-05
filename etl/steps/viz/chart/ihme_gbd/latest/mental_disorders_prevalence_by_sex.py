"""MDim of scatterplots comparing the prevalence of mental disorders in women and men (IHME GBD), by disorder."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Disorders, keyed by their slug in the source: name used in titles and glossary link.
DISORDERS = {
    "depressive_disorders": ("depression", "depressive_disorders"),
    "anxiety_disorders": ("anxiety disorders", "anxiety_disorders"),
    "bipolar_disorder": ("bipolar disorder", "bipolar_disorder"),
    "schizophrenia": ("schizophrenia", "schizophrenia"),
    "eating_disorders": ("an eating disorder", "eating_disorder"),
}
# Extra subtitle sentences, for disorders whose name alone doesn't say what's included.
DISORDER_NOTES = {
    "eating_disorders": "This only includes anorexia nervosa and bulimia nervosa.",
}


def run() -> None:
    config = paths.load_config()

    # Step 1: Build one scatter view per disorder: women on the x-axis, men on the y-axis.
    views = []
    for disorder, (name, dod) in DISORDERS.items():
        subtitle = (
            f"Estimated share of women and men who have [{name}](#dod:{dod}), whether or not they are diagnosed, "
            "based on representative surveys, medical data and statistical modelling."
        )
        if disorder in DISORDER_NOTES:
            subtitle += " " + DISORDER_NOTES[disorder]
        subtitle += (
            f" Prevalence is measured as the share experiencing {name} at a given point in time."
            " It does not measure lifetime prevalence."
        )
        views.append(
            {
                "dimensions": {"disorder": disorder},
                "indicators": {
                    "x": _share(disorder, "female"),
                    "y": [_share(disorder, "male")],
                    "color": "regions#owid_region",
                },
                "config": {
                    "title": f"Share of people with {name}, women vs. men",
                    "subtitle": subtitle,
                },
            }
        )
    config["views"] = views

    # Step 2: Create the chart.
    c = paths.create_chart(config=config)
    c.save()


def _share(disorder: str, sex: str) -> dict:
    """Age-standardized share of the population with the disorder, for one sex, with a short name for tooltips."""
    who = {"female": "women and girls", "male": "men and boys"}[sex]
    return {
        "catalogPath": f"gbd_mental_health#value__cause_{disorder}__metric_share__sex_{sex}__age_age_standardized",
        "display": {"name": f"Share of {who}"},
    }
