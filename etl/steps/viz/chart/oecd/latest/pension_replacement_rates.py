"""Pensions as a share of previous earnings MDim: the single views from the config, a comparison of mandatory schemes with and without voluntary ones, and stacked views by type of mandatory scheme."""

from etl.helpers import PathFinder
from etl.viz.chart.model.view import View

paths = PathFinder(__file__)

# Continent of each country, to color the Marimekkos.
CONTINENT_PATH = "regions#owid_region"

# The comparison choice of the schemes selector, and the legend label of each choice it compares.
COMPARISON_SLUG = "mandatory_vs_voluntary"
COMPARISON_LABELS = {"mandatory": "Mandatory", "mandatory_and_voluntary": "Mandatory and voluntary"}

# Mandatory vs. voluntary schemes as a dumbbell joined by a plain line (the two have no order), sorted by the rate
# with voluntary schemes (the second choice), highest first.
COMPARISON_CONFIG = {
    "hasMapTab": False,
    "tab": "chart",
    "chartTypes": ["Dumbbell"],
    "dumbbell": {"connectorStyle": "line"},
    "sortBy": "endValue",
    "sortOrder": "desc",
}

# Wording of each choice in the titles and subtitles of the views built here.
TITLE_START = "Pensions as a share of previous earnings"
TITLE_TAX = {"gross": "(before tax)", "net": "(after tax)"}
TAXES = {
    "gross": "Both are measured before income taxes and social security contributions.",
    "net": "Both are measured after income taxes and social security contributions.",
}
EARNINGS = {"average": "the average wage", "half": "half the average wage"}
EARNINGS_CODES = {"average": "average_wage", "half": "50pct_of_average_wage"}

# Stacked views of the gross rate by type of mandatory scheme: for each choice, the types it stacks (pension schemes
# code in the indicator name, legend label and color), bottom first.
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
STACKED_TITLE_ENDS = {
    "public_and_quasi_mandatory": ", from public and quasi-mandatory private schemes",
    "mandatory_by_scheme": ", by type of mandatory scheme",
}
STACKED_SCHEMES_PHRASES = {
    "public_and_quasi_mandatory": "public schemes and from quasi-mandatory private schemes, which cover nearly all workers without being required by law",
    "mandatory_by_scheme": "public schemes, quasi-mandatory private schemes (which cover nearly all workers without being required by law), and other mandatory private schemes",
}
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

SUBTITLE = "Pension that men earning {earnings} throughout their careers can expect to receive from {schemes} when they retire, as a share of their earnings while they worked. {taxes}"


def run() -> None:
    # Load the modeled-pensions note from the indicators, so the views built here carry the same one.
    tb = paths.load_dataset("pensions_at_a_glance").read("replacement_rates", reset_index=False)
    note = tb[tb.columns[0]].metadata.presentation.grapher_config["note"]

    c = paths.create_chart(config=paths.load_config())

    # Add the comparison of mandatory schemes with and without voluntary ones, for each measure and earnings level.
    subtitle = SUBTITLE.replace("{schemes}", "mandatory pension schemes alone, and together with voluntary schemes,")
    c.group_views(
        groups=[
            {
                "dimension": "schemes",
                "choices": list(COMPARISON_LABELS),
                "choice_new_slug": COMPARISON_SLUG,
                "view_config": {
                    **COMPARISON_CONFIG,
                    "title": TITLE_START
                    + " for men earning {earnings}, with and without voluntary schemes {title_tax}",
                    "subtitle": subtitle,
                    "note": note,
                },
                "view_metadata": {"description_short": subtitle},
            }
        ],
        params={
            "title_tax": lambda view: TITLE_TAX[view.dimensions["measure"]],
            "taxes": lambda view: TAXES[view.dimensions["measure"]],
            "earnings": lambda view: EARNINGS[view.dimensions["earnings"]],
        },
    )

    # Label each indicator of the comparison in the legend by the choice it shows.
    for view in c.views:
        if view.dimensions["schemes"] == COMPARISON_SLUG:
            for indicator, label in zip(view.indicators.y, COMPARISON_LABELS.values()):
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
    """Create the stacked bar views of the gross rate by type of mandatory scheme, one per earnings level."""
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
                        "dimensions": {"measure": "gross", "earnings": earnings, "schemes": schemes},
                        "indicators": {"y": indicators},
                        "config": {
                            "hasMapTab": False,
                            "tab": "chart",
                            "chartTypes": ["StackedDiscreteBar"],
                            "stackMode": "absolute",
                            "selectedEntityNames": STACKED_SELECTION,
                            "title": f"{TITLE_START} for men earning {EARNINGS[earnings]}{STACKED_TITLE_ENDS[schemes]} {TITLE_TAX['gross']}",
                            "subtitle": SUBTITLE.format(
                                earnings=EARNINGS[earnings],
                                schemes=STACKED_SCHEMES_PHRASES[schemes],
                                taxes=TAXES["gross"],
                            ),
                            "note": note,
                        },
                    }
                )
            )
    return views
