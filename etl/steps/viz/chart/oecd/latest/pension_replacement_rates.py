"""Pension replacement rate MDim: the single views from the config, plus a comparison view for each selector."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# For each selector: the slug of its comparison choice, and the legend label of each choice it compares.
COMPARISONS = {
    "measure": ("gross_vs_net", {"gross": "Gross", "net": "Net"}),
    "earnings": (
        "all_earnings",
        {"half": "Half the average wage", "average": "Average wage", "twice": "Twice the average wage"},
    ),
    "schemes": (
        "mandatory_vs_voluntary",
        {"mandatory": "Mandatory", "mandatory_and_voluntary": "Mandatory and voluntary"},
    ),
    "sex": ("men_vs_women", {"men": "Men", "women": "Women"}),
}

# Two-way comparisons as dumbbells, sorted by the first choice, highest first. Neither choice comes before the other,
# so a plain line joins them instead of an arrow.
DUMBBELL_CONFIG = {
    "hasMapTab": False,
    "tab": "chart",
    "chartTypes": ["Dumbbell"],
    "dumbbell": {"connectorStyle": "line"},
    "sortBy": "startValue",
    "sortOrder": "desc",
}

# Three earnings levels don't fit a dumbbell, so they go side by side, one panel per country. The custom sort keeps
# the bars in the order of the indicators (half, average, twice), instead of sorting them by value in each panel.
EARNINGS_CONFIG = {
    "hasMapTab": False,
    "tab": "chart",
    "chartTypes": ["DiscreteBar"],
    "selectedFacetStrategy": "entity",
    "sortBy": "custom",
    "sortOrder": "asc",
}

# Wording of each choice in the titles and subtitles of the comparison views.
MEASURE = {"gross": "Gross", "net": "Net", "gross_vs_net": "Gross and net"}
TAXES = {
    "gross": "Both are measured before taxes.",
    "net": "Both are measured after taxes.",
    "gross_vs_net": "The gross rate measures both before taxes, and the net rate after taxes.",
}
EARNINGS = {
    "half": "half the average wage",
    "average": "the average wage",
    "twice": "twice the average wage",
    "all_earnings": "half the average wage, the average wage, or twice the average wage",
}
SCHEMES = {
    "mandatory": "mandatory pension schemes",
    "mandatory_and_voluntary": "mandatory and voluntary pension schemes",
    "mandatory_vs_voluntary": "mandatory pension schemes alone, and together with voluntary schemes",
}
SEX = {"men": "men", "women": "women", "men_vs_women": "men and women"}


def run() -> None:
    # Load the modeled-pensions note from the indicators, so the comparison views carry the same one.
    tb = paths.load_dataset("pensions_at_a_glance").read("replacement_rates", reset_index=False)
    note = tb[tb.columns[0]].metadata.presentation.grapher_config["note"]

    c = paths.create_chart(config=paths.load_config())

    # Add a comparison view for every combination of the other selectors.
    subtitle = "Pension that {sex} earning {earnings} can expect to receive from {schemes} when they retire, as a share of what they earned while working. {taxes}"
    titles = {
        "measure": "Gross and net pension replacement rates for {sex} earning {earnings}",
        "earnings": "{measure} pension replacement rates for {sex}, by earnings",
        "schemes": "Net pension replacement rates for {sex} earning {earnings}, with and without voluntary schemes",
        "sex": "{measure} pension replacement rates for men and women earning {earnings}",
    }
    c.group_views(
        groups=[
            {
                "dimension": dimension,
                "choices": list(labels),
                "choice_new_slug": slug,
                "view_config": {
                    **(EARNINGS_CONFIG if dimension == "earnings" else DUMBBELL_CONFIG),
                    "title": titles[dimension],
                    "subtitle": subtitle,
                    "note": note,
                },
                "view_metadata": {"description_short": subtitle},
            }
            for dimension, (slug, labels) in COMPARISONS.items()
        ],
        params={
            "measure": lambda view: MEASURE[view.dimensions["measure"]],
            "taxes": lambda view: TAXES[view.dimensions["measure"]],
            "earnings": lambda view: EARNINGS[view.dimensions["earnings"]],
            "schemes": lambda view: SCHEMES[view.dimensions["schemes"]],
            "sex": lambda view: SEX[view.dimensions["sex"]],
        },
    )

    # Drop comparisons with a missing side, and label each indicator in the legend by the choice it shows.
    c.views = [view for view in c.views if keep_view(view)]
    for view in c.views:
        for dimension, (slug, labels) in COMPARISONS.items():
            if view.dimensions[dimension] == slug:
                for indicator, label in zip(view.indicators.y, labels.values()):
                    indicator.update_display({"name": label})

    c.save()


def keep_view(view) -> bool:
    """Keep a comparison view only if it has an indicator for each choice it compares.

    Mandatory and voluntary schemes are only published for net rates for men, so the comparisons of gross and net, or
    of men and women, have only one side for those schemes, and so has the comparison of schemes for women or for
    gross rates.
    """
    for dimension, (slug, labels) in COMPARISONS.items():
        if view.dimensions[dimension] == slug:
            return len(view.indicators.y) == len(labels)
    return True
