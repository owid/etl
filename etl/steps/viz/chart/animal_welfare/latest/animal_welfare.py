"""Create the animal-welfare multidim, shown as a data explorer.

Views on animal counts are built from the dimensions of the animals_used_for_food indicators; views on laws and hens
are defined in the adjacent config.
"""

import re

from owid.catalog.utils import underscore

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Estimate choice slug for each estimate in the data (only fish and crustaceans are estimated as a range).
ESTIMATES = {
    "midpoint": "midpoint",
    "lower limit": "lower_limit",
    "upper limit": "upper_limit",
}
ESTIMATE_NOT_APPLICABLE = "not_applicable"

# Animal choices used only by the views on laws (defined in the config). Other laws and hens views reuse an animal
# from the data (cattle for bullfighting, chickens for chick culling and caged hens).
EXTRA_ANIMALS = [
    {
        "slug": "fur_animals",
        "name": "Fur animals",
        "description": "Animals farmed for their fur, such as minks and foxes.",
    },
]

# Colors for each status of the chick culling laws.
CHICK_CULLING_STATUS_COLORS = {
    "Banned": "#759AC8",
    "Banned but not yet in effect": "#058580",
    "Not banned": "#AE2E3F",
    "Partially banned": "#A46F49",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # Load dataset of animals used for food, and read its main table.
    tb = paths.load_dataset("animals_used_for_food").read("animals_used_for_food")

    # Load dataset of chick culling laws, and read its main table.
    tb_chick = paths.load_dataset("chick_culling_laws").read("chick_culling_laws")

    #
    # Process data.
    #
    # Build one view per animal count indicator, and collect the animal choices.
    animals = {}
    views = []
    for column in tb.columns:
        if column in tb.metadata.primary_key:
            continue
        dims = tb[column].metadata.dimensions
        per_capita = str(dims["per_capita"]).lower() == "true"
        animal = dims["animal"]
        estimate = dims.get("estimate") or None
        animal_slug = underscore(animal)
        animals[animal_slug] = {"slug": animal_slug, "name": animal.capitalize()}
        views.append(
            {
                "dimensions": {
                    "metric": dims["metric"],
                    "animal": animal_slug,
                    "estimate": ESTIMATES[estimate] if estimate else ESTIMATE_NOT_APPLICABLE,
                    "per_capita": "per_capita" if per_capita else "total",
                },
                "indicators": {"y": [{"catalogPath": f"animals_used_for_food#{column}"}]},
            }
        )

    # Sanity check: the animals reused by the views on laws and hens exist in the data.
    for view in config["views"]:
        animal = view["dimensions"]["animal"]
        assert animal in animals or animal in {a["slug"] for a in EXTRA_ANIMALS}, f"Unknown animal: {animal}"

    # Sort animals alphabetically.
    animal_choices = sorted([*animals.values(), *EXTRA_ANIMALS], key=lambda choice: choice["name"].lower())
    for dimension in config["dimensions"]:
        if dimension["slug"] == "animal":
            dimension["choices"] = animal_choices

    # The chick culling status embeds each country's effective year (e.g. "Banned (2023)"). Color each value found in
    # the data by its plain status, and label it with that status, so the legend shows one entry per status.
    colors = {}
    labels = {}
    for value in sorted(set(tb_chick["status"])):
        plain = re.sub(r" \(\d{4}\)$", "", value)
        colors[value] = CHICK_CULLING_STATUS_COLORS[plain]
        if plain != value:
            labels[value] = plain
    for view in config["views"]:
        if view["dimensions"]["metric"] == "chick_culling_status":
            color_scale = view["config"]["map"]["colorScale"]
            color_scale["customCategoryColors"] = colors
            color_scale["customCategoryLabels"] = labels

    # Animal count views go first, followed by those on laws and hens.
    config["views"] = views + config["views"]

    #
    # Save outputs.
    #
    c = paths.create_chart(config=config)
    c.save()
