"""MDim of the prevalence of mental disorders by age group (IHME GBD), by disorder."""

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
# Age groups shown as bars, in order: slug in the source -> label.
AGES_YOUNG = {
    "5_14_years": "Ages 5–14",
    "15_19_years": "Ages 15–19",
    "20_24_years": "Ages 20–24",
    "25_29_years": "Ages 25–29",
    "30_34_years": "Ages 30–34",
    "35_39_years": "Ages 35–39",
    "40_44_years": "Ages 40–44",
    "45_49_years": "Ages 45–49",
}
AGES_OLD = {
    "50_54_years": "Ages 50–54",
    "55_59_years": "Ages 55–59",
    "60_64_years": "Ages 60–64",
    "65_69_years": "Ages 65–69",
    "70plus_years": "Ages 70+",
}
# The source has no five-year groups over 50 for eating disorders, only 50–69 and 70+.
AGES_OLD_EATING = {
    "50_69_years": "Ages 50–69",
    "70plus_years": "Ages 70+",
}


def run() -> None:
    config = paths.load_config()

    # Step 1: Build one view per disorder, with one indicator (bar) per age group.
    views = []
    for disorder, (name, dod) in DISORDERS.items():
        ages = AGES_YOUNG | (AGES_OLD_EATING if disorder == "eating_disorders" else AGES_OLD)
        subtitle = (
            f"Estimated share of people in each age group who have [{name}](#dod:{dod}), whether or not they are "
            "diagnosed, based on representative surveys, medical data and statistical modelling."
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
                    "y": [
                        {
                            "catalogPath": f"gbd_mental_health#value__cause_{disorder}__metric_share__sex_both__age_{age}",
                            "display": {"name": label},
                        }
                        for age, label in ages.items()
                    ]
                },
                "config": {
                    "title": f"Share of people with {name}, by age",
                    "subtitle": subtitle,
                },
            }
        )
    config["views"] = views

    # Step 2: Create the chart.
    c = paths.create_chart(config=config)
    c.save()
