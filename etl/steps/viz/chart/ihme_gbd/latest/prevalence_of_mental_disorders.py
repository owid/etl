"""MDim of the prevalence of mental disorders (IHME GBD), by disorder, sex, age and metric."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Disorders, keyed by their slug in the source: name used in titles, glossary link (if any), and map palette.
DISORDERS = {
    "mental_disorders": ("any mental disorder", None, "BuGn"),
    "depressive_disorders": ("depressive disorders", "depressive_disorders", "YlGnBu"),
    "anxiety_disorders": ("anxiety disorders", "anxiety_disorders", "PuBu"),
    "bipolar_disorder": ("bipolar disorder", "bipolar_disorder", "RdPu"),
    "schizophrenia": ("schizophrenia", "schizophrenia", "YlOrBr"),
    "eating_disorders": ("an eating disorder", "eating_disorder", "OrRd"),
}
# Extra subtitle sentences, for disorders whose name alone doesn't say what's included.
DISORDER_NOTES = {
    "mental_disorders": "This does not include alcohol and drug use disorders.",
    "eating_disorders": "This only includes anorexia nervosa and bulimia nervosa.",
}
# Sexes: slug in the MDim and the source, and who it refers to in titles, for children (under 15), adults and all ages.
SEXES = {
    "both": {"child": "people", "adult": "people", "all": "people"},
    "female": {"child": "girls", "adult": "women", "all": "women and girls"},
    "male": {"child": "boys", "adult": "men", "all": "men and boys"},
}
# Ages: slug in the MDim -> (slug in the source, phrase used in titles, age bracket for SEXES).
AGES = {
    "all_ages": ("all_ages", "", "all"),
    "under_5": ("_lt_5_years", " under 5", "child"),
    "age_5_14": ("5_14_years", " aged 5–14", "child"),
    "age_15_49": ("15_49_years", " aged 15–49", "adult"),
    "age_50_69": ("50_69_years", " aged 50–69", "adult"),
    "age_70_plus": ("70plus_years", " aged 70 and older", "adult"),
}
NOTE_AGE_STANDARDIZED = "To allow for comparisons between countries and over time, this metric is [age-standardized](#dod:age_standardized)."
NOTE_CRUDE = "This metric is not age-standardized, so differences between countries partly reflect differences in their age structure."
NOTE_NUMBER = "Figures are affected by the size of the population in a country."


def run() -> None:
    config = paths.load_config()
    disorder_names = {c["slug"]: c["name"] for c in config["dimensions"][0]["choices"]}

    # Step 1: Build one view per disorder, sex, age and metric. Age-standardized shares exist only for all ages.
    views = []
    for disorder, (name, dod, palette) in DISORDERS.items():
        disorder_text = f"[{name}](#dod:{dod})" if dod else name
        for sex, who_by_age in SEXES.items():
            for age, (source_age, age_text, bracket) in AGES.items():
                who = who_by_age[bracket]
                metrics = ["share_crude", "number"]
                if age == "all_ages":
                    metrics = ["share_age_standardized"] + metrics
                for metric in metrics:
                    views.append(
                        _view(
                            disorder,
                            sex,
                            age,
                            metric,
                            source_age,
                            title_end=f"{who}{age_text} with {name}",
                            subtitle_end=f"{who}{age_text} who have {disorder_text}",
                            display_name=disorder_names[disorder],
                            palette=palette,
                        )
                    )
    config["views"] = views

    # Step 2: Create the chart.
    c = paths.create_chart(config=config)
    c.save()


def _view(disorder, sex, age, metric, source_age, title_end, subtitle_end, display_name, palette) -> dict:
    """One view's indicator and chart text."""
    if metric == "share_age_standardized":
        source_metric, source_age = "share", "age_standardized"
    else:
        source_metric = "number" if metric == "number" else "share"
    catalog_path = f"gbd_mental_health#value__cause_{disorder}__metric_{source_metric}__sex_{sex}__age_{source_age}"

    measure = "number" if metric == "number" else "share"
    subtitle = (
        f"Estimated {measure} of {subtitle_end}, whether or not they are diagnosed, "
        "based on representative surveys, medical data and statistical modelling."
    )
    if disorder in DISORDER_NOTES:
        subtitle += " " + DISORDER_NOTES[disorder]
    note = {"share_age_standardized": NOTE_AGE_STANDARDIZED, "share_crude": NOTE_CRUDE, "number": NOTE_NUMBER}[metric]

    return {
        "dimensions": {"disorder": disorder, "sex": sex, "age": age, "metric": metric},
        "indicators": {"y": [{"catalogPath": catalog_path, "display": {"name": display_name}}]},
        "config": {
            "title": f"{measure.capitalize()} of {title_end}",
            "subtitle": subtitle,
            "note": note,
            "map": {"colorScale": {"baseColorScheme": palette}},
        },
    }
