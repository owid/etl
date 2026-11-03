"""Pension replacement rate MDim: the single views from the config, plus a comparison view for each selector."""

from etl.helpers import PathFinder
from etl.viz.chart.model.view import View

paths = PathFinder(__file__)

# Continent of each country, to color the Marimekkos.
CONTINENT_PATH = "regions#owid_region"

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
    "age": ("retirement_vs_80", {"at_retirement": "At retirement", "at_80": "At age 80"}),
}

# Two-way comparisons as dumbbells, sorted by the first choice, highest first. Mandatory vs. voluntary schemes and men
# vs. women have no order, so a plain line joins them instead of an arrow.
DUMBBELL_CONFIG = {
    "hasMapTab": False,
    "tab": "chart",
    "chartTypes": ["Dumbbell"],
    "dumbbell": {"connectorStyle": "line"},
    "sortBy": "startValue",
    "sortOrder": "desc",
}

# Gross and net are the rate before and after taxes, and the rates at retirement and at age 80 are one after the
# other, so an arrow points from the first to the second.
ARROW_CONFIG = {**DUMBBELL_CONFIG, "dumbbell": {"connectorStyle": "arrow"}}

# Gross vs. net is sorted by the net rate (the end of the arrow), since it's what pensioners actually receive.
GROSS_VS_NET_CONFIG = {**ARROW_CONFIG, "sortBy": "endValue"}

# Mandatory vs. voluntary schemes is sorted by the rate from mandatory and voluntary schemes together (the second choice).
SCHEMES_CONFIG = {**DUMBBELL_CONFIG, "sortBy": "endValue"}

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

# Chart settings of each comparison, other than the plain dumbbell.
CONFIGS = {
    "measure": GROSS_VS_NET_CONFIG,
    "schemes": SCHEMES_CONFIG,
    "earnings": EARNINGS_CONFIG,
    "age": ARROW_CONFIG,
}

# Wording of each choice in the titles and subtitles of the comparison views.
MEASURE = {"gross": "Gross", "net": "Net", "gross_vs_net": "Gross and net"}
TAXES = {
    "gross": "Both are measured before income taxes and social security contributions.",
    "net": "Both are measured after income taxes and social security contributions.",
    "gross_vs_net": "The gross rate measures both before income taxes and social security contributions, and the net rate after them.",
}
EARNINGS = {
    "half": "half the average wage",
    "average": "the average wage",
    "twice": "twice the average wage",
    "all_earnings": "half the average wage, the average wage, or twice the average wage",
}
SCHEMES = {
    "public": "public pension schemes",
    "public_and_quasi_mandatory": "public and quasi-mandatory private pension schemes",
    "mandatory": "mandatory pension schemes",
    "mandatory_by_scheme": "mandatory pension schemes",
    "mandatory_and_voluntary": "mandatory and voluntary pension schemes",
    "mandatory_vs_voluntary": "mandatory pension schemes alone, and together with voluntary schemes",
}
SEX = {"men": "men", "women": "women", "men_vs_women": "men and women"}

# Stacked views of the gross rate of men by type of mandatory scheme: for each choice, the types it stacks (pension
# schemes code in the indicator name, legend label and color), bottom first.
STACKED_SCHEMES = {
    "public_and_quasi_mandatory": [
        ("public", "Public", "#4C6A9C"),
        ("quasi_mandatory_private", "Quasi-mandatory private", "#E56E5A"),
    ],
    "mandatory_by_scheme": [
        ("public", "Public", "#4C6A9C"),
        ("quasi_mandatory_private", "Quasi-mandatory private", "#E56E5A"),
        ("other_mandatory_private", "Other mandatory private", "#A2B4DA"),
    ],
}
STACKED_TITLES = {
    "public_and_quasi_mandatory": "Gross pension replacement rates for men earning {earnings}, from public and quasi-mandatory private schemes",
    "mandatory_by_scheme": "Gross pension replacement rates for men earning {earnings}, by type of mandatory scheme",
}
STACKED_SUBTITLES = {
    "public_and_quasi_mandatory": "Pension that men earning {earnings} can expect to receive when they retire, as a share of what they earned while working, from public schemes and from quasi-mandatory private schemes, which cover nearly all workers without being required by law. Both are measured before income taxes and social security contributions.",
    "mandatory_by_scheme": "Pension that men earning {earnings} can expect to receive when they retire, as a share of what they earned while working, from public schemes, quasi-mandatory private schemes (which cover nearly all workers without being required by law), and other mandatory private schemes. Both are measured before income taxes and social security contributions.",
}
EARNINGS_CODES = {"half": "50pct_of_average_wage", "average": "average_wage", "twice": "200pct_of_average_wage"}
# The stacked views open on more countries than the rest, including those with each type of private scheme.
STACKED_SELECTION = [
    "OECD countries",
    "United Kingdom",
    "Netherlands",
    "Denmark",
    "Sweden",
    "Switzerland",
    "Australia",
    "Iceland",
    "Chile",
    "United States",
    "Germany",
    "France",
    "Japan",
]


def run() -> None:
    # Load the modeled-pensions note from the indicators, so the comparison views carry the same one.
    tb = paths.load_dataset("pensions_at_a_glance").read("replacement_rates", reset_index=False)
    note = tb[tb.columns[0]].metadata.presentation.grapher_config["note"]

    c = paths.create_chart(config=paths.load_config())

    # Add a comparison view for every combination of the other selectors.
    subtitle = "Pension that {sex} earning {earnings} can expect to receive from {schemes} when they retire, as a share of what they earned while working. {taxes}"
    subtitles = {
        "age": "Pension that {sex} earning {earnings} can expect to receive from {schemes} when they retire, as a share of what they earned while working, and at age 80, as a share of the average wage at that time. {taxes}"
    }
    titles = {
        "measure": "Gross and net pension replacement rates for {sex} earning {earnings}",
        "earnings": "{measure} pension replacement rates for {sex}, by earnings",
        "schemes": "{measure} pension replacement rates for {sex} earning {earnings}, with and without voluntary schemes",
        "sex": "{measure} pension replacement rates for men and women earning {earnings}",
        "age": "{measure} pension replacement rate for {sex} earning {earnings}, at retirement and at age 80",
    }
    c.group_views(
        groups=[
            {
                "dimension": dimension,
                "choices": list(labels),
                "choice_new_slug": slug,
                "view_config": {
                    **CONFIGS.get(dimension, DUMBBELL_CONFIG),
                    "title": titles[dimension],
                    "subtitle": subtitles.get(dimension, subtitle),
                    "note": note,
                },
                "view_metadata": {"description_short": subtitles.get(dimension, subtitle)},
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

    # Color the Marimekkos of the single views by continent. Added after grouping, since group_views only merges views
    # with y indicators alone.
    for view in c.views:
        if len(view.indicators.y) == 1:
            view.indicators.set_indicator(color=CONTINENT_PATH)

    # Add the stacked views by type of mandatory scheme (after grouping, since group_views can't merge them).
    dataset_uri = tb.m.dataset.uri
    c.views.extend(create_stacked_scheme_views(dataset_uri=dataset_uri, note=note))

    c.save()


def create_stacked_scheme_views(dataset_uri: str, note: str) -> list[View]:
    """Create the stacked bar views of the gross rate of men by type of mandatory scheme, one per earnings level."""
    views = []
    for schemes, types in STACKED_SCHEMES.items():
        for earnings, earnings_code in EARNINGS_CODES.items():
            indicators = [
                {
                    "catalogPath": f"{dataset_uri}/replacement_rates#gross_replacement_rate__sex_men__earnings_{earnings_code}__pension_schemes_{code}",
                    "display": {"name": label, "color": color},
                }
                for code, label, color in types
            ]
            views.append(
                View.from_dict(
                    {
                        "dimensions": {
                            "measure": "gross",
                            "earnings": earnings,
                            "schemes": schemes,
                            "sex": "men",
                            "age": "at_retirement",
                        },
                        "indicators": {"y": indicators},
                        "config": {
                            "hasMapTab": False,
                            "tab": "chart",
                            "chartTypes": ["StackedDiscreteBar"],
                            "stackMode": "absolute",
                            "selectedEntityNames": STACKED_SELECTION,
                            "title": STACKED_TITLES[schemes].format(earnings=EARNINGS[earnings]),
                            "subtitle": STACKED_SUBTITLES[schemes].format(earnings=EARNINGS[earnings]),
                            "note": note,
                        },
                    }
                )
            )
    return views


def keep_view(view) -> bool:
    """Keep a comparison view only if it has an indicator for each choice it compares.

    Mandatory and voluntary schemes are only published for men, so the comparison of men and women has only one side
    for those schemes, and so has the comparison of schemes for women. Public schemes are only published for gross
    rates for men. Likewise, rates at age 80 are only published for gross rates for men earning the average wage in
    mandatory schemes.
    """
    for dimension, (slug, labels) in COMPARISONS.items():
        if view.dimensions[dimension] == slug:
            return len(view.indicators.y) == len(labels)
    return True
