"""MDim of the prevalence of mental disorders by age group (IHME GBD), by disorder."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Disorders, keyed by their slug in the source: name used in titles, how the subtitle describes it, and bar color.
DISORDERS = {
    "depressive_disorders": ("depression", "[depression](#dod:depressive_disorders)", "#286BBB"),
    "anxiety_disorders": ("anxiety disorders", "[anxiety disorders](#dod:anxiety_disorders)", "#6D3E91"),
    "bipolar_disorder": ("bipolar disorder", "[bipolar disorder](#dod:bipolar_disorder)", "#C05917"),
    "schizophrenia": ("schizophrenia", "[schizophrenia](#dod:schizophrenia)", "#58AC8C"),
    "eating_disorders": ("an eating disorder", "anorexia or bulimia nervosa", "#C15065"),
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

    # Step 1: Build one view per disorder, with one indicator (bar) per age group, all in the disorder's color.
    views = []
    for disorder, (name, subtitle_name, color) in DISORDERS.items():
        ages = AGES_YOUNG | (AGES_OLD_EATING if disorder == "eating_disorders" else AGES_OLD)
        subtitle = (
            f"Estimated share of people in each age group who have {subtitle_name}, whether or not they are "
            "diagnosed, based on representative surveys, medical data and statistical modelling."
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
                            "display": {"name": label, "color": color},
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
