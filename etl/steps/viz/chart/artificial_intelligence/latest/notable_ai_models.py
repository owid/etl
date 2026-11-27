"""Notable AI models, by training computation, parameters, or training data, colored by one of several characteristics."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Plotted indicator and chart text for each metric.
METRICS = {
    "compute": {
        "indicator": "training_compute_flop",
        "title": "Computation used to train notable AI models",
        "subtitle": "Each dot is a notable AI model, placed by its publication date and the computation used to train it.",
        "y_label": "Training compute (FLOP)",
        "note": "One floating-point operation (FLOP) is a single calculation, such as adding or multiplying two decimal numbers. Where developers do not report it, Epoch AI estimates training compute from a model’s size, training data, hardware, or cost; the least-documented estimates may be off by a factor of about 30. About half of notable models have no compute figure and are not shown.",
    },
    "parameters": {
        "indicator": "parameters",
        "title": "Parameters in notable AI models",
        "subtitle": "Each dot is a notable AI model, placed by its publication date and its number of parameters: the values, also called weights, that a model adjusts during training to learn from its data.",
        "y_label": "Number of parameters",
        "note": "Developers usually report how many parameters their models have; where they don’t, Epoch AI estimates it from details of the model’s design. About a third of notable models have no parameter count and are not shown.",
    },
    "data": {
        "indicator": "training_dataset_size__total",
        "title": "Data used to train notable AI models",
        "subtitle": "Each dot is a notable AI model, placed by its publication date and the number of data points used to train it.",
        "y_label": "Training dataset size (data points)",
        "note": "What counts as a data point depends on the domain, such as words or tokens of text, images, or moves in a game, so compare models only within the same domain. About 40% of notable models have no dataset size and are not shown.",
    },
}

# Color indicator and chart text for each coloring. "Other" and missing categories are gray.
GRAY = "#6e7581"
# Lighter gray for "Other" when the named categories are the point of the chart, so that they stand out.
LIGHT_GRAY = "#b9bdc4"
COLORS = {
    "reporting": {
        "indicator": "training_compute_reporting",
        "sentence": "Colors show whether its training compute was directly reported by the developer or indirectly estimated by Epoch AI.",
        "by": "by whether training compute was reported or estimated",
        "color_scale": {
            "customCategoryColors": {"Reported by the developer": "#4C6A9C", "Estimated by Epoch AI": "#996D36"},
        },
        "metrics": ["compute"],
    },
    "domain": {
        "indicator": "domain",
        "sentence": "Colors show the area of machine learning that it is designed for.",
        "by": "by domain",
        "color_scale": {"baseColorScheme": "OwidCategoricalB", "customCategoryColors": {"Other": GRAY}},
    },
    "affiliation": {
        "indicator": "organization_categorization",
        "sentence": "Colors show the type of organization that its authors work for, such as companies or universities.",
        "by": "by researcher affiliation",
        "color_scale": {
            "customCategoryColors": {
                "Industry": "#ca011f",
                "Academia": "#1171b0",
                "Academia and industry collaboration": "#38aaba",
                "Other": GRAY,
                "Not specified": "#a2a6ad",
            },
        },
    },
    "weights": {
        "indicator": "weights_availability",
        "sentence": "Colors show whether its weights are openly available, whether it can only be used through an API or another hosted service, or whether it was not released.",
        "by": "by availability of weights",
        "color_scale": {
            "customCategoryColors": {
                "Open weights": "#00847E",
                "API or hosted access only": "#4C6A9C",
                "Not released": "#996D36",
                "No data": "#a2a6ad",
            },
            "customCategoryLabels": {"No data": "Not recorded"},
        },
    },
    "country": {
        "indicator": "country_group",
        "sentence": "Colors show the country of the organizations that developed it.",
        "by": "by country of developer",
        # The United States and China get their flags' blue and red. The UK and France, whose flags use the same
        # colors, get purple and teal so that the four stay easy to tell apart.
        "color_scale": {
            "customCategoryColors": {
                "United States": "#2F5DA8",
                "China": "#D52B1E",
                "United Kingdom": "#7B4BA8",
                "France": "#00847E",
                "Multiple countries": "#E0A000",
                "Other": LIGHT_GRAY,
                "No data": "#dcdee2",
            },
            "customCategoryLabels": {"No data": "Not recorded"},
        },
    },
    "developer": {
        "indicator": "developer_group",
        "sentence": "Colors show the company that developed it.",
        "by": "by developer",
        # Colors close to each company's brand, adjusted so that none of them clash (Google and Meta are both blue).
        "color_scale": {
            "customCategoryColors": {
                "OpenAI": "#10A37F",
                "Google": "#4285F4",
                "Anthropic": "#D97757",
                "xAI": "#1F1F1F",
                "Meta": "#8E44AD",
                "Other": LIGHT_GRAY,
            },
        },
    },
}


def view_metadata(tb, metric: dict, color: dict) -> dict:
    """Data-page text for a view, covering both the plotted indicator and the one that colors it."""
    y_meta = tb[metric["indicator"]].metadata
    color_meta = tb[color["indicator"]].metadata

    processing = [
        f"**{meta.title}:** {meta.description_processing}"
        for meta in (y_meta, color_meta)
        if meta.description_processing
    ]
    origins = []
    for origin in list(y_meta.origins) + list(color_meta.origins):
        if origin not in origins:
            origins.append(origin)

    metadata = {
        "presentation": {"title_public": f"{metric['title']}, {color['by']}"},
        "description_short": f"{y_meta.description_short} {color['sentence']}",
        "origins": [origin.to_dict() for origin in origins],
    }
    if processing:
        metadata["description_processing"] = "\n\n".join(processing)

    return metadata


def run() -> None:
    # Load the indicators the views plot, for their metadata.
    tb = paths.load_dataset("epoch").read("epoch", load_data=False)

    # Build one view per metric and coloring.
    config = paths.load_config()
    views = []
    for metric_slug, metric in METRICS.items():
        for color_slug, color in COLORS.items():
            if metric_slug not in color.get("metrics", METRICS):
                continue
            views.append(
                {
                    "dimensions": {"metric": metric_slug, "color": color_slug},
                    "indicators": {
                        "y": [{"catalogPath": f"epoch#{metric['indicator']}"}],
                        "color": {"catalogPath": f"epoch#{color['indicator']}"},
                    },
                    "config": {
                        "title": metric["title"],
                        "subtitle": f"{metric['subtitle']} {color['sentence']}",
                        "note": metric["note"],
                        "yAxis": {"label": metric["y_label"], "scaleType": "log", "canChangeScaleType": True},
                        "colorScale": color["color_scale"],
                    },
                    "metadata": view_metadata(tb, metric, color),
                }
            )
    config["views"] = views

    c = paths.create_chart(config=config)
    c.save()
