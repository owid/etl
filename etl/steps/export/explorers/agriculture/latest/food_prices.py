"""Build the food-prices explorer programmatically from food_prices_for_nutrition.

The explorer wraps 12 single-indicator views. Each view's chart text (title, subtitle,
note, map color scale, etc.) is authored upstream in the indicator's garden metadata
under `presentation.{title_public, grapher_config}`, so this step only needs to:

1. Tag each indicator column with its dimension tuple (diet, type, affordability_metric,
   cost_metric).
2. Hand the table to `paths.create_collection(tb=..., explorer=True)` for view expansion.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map each indicator column → its dimension tuple. Conditional widgets use the "na"
# slot when not applicable for that column (matching the legacy explorer's empty cells).
COLUMN_DIMENSIONS: dict[str, dict[str, str]] = {
    # Affordability — share that cannot afford
    "percent_of_the_population_who_cannot_afford_a_healthy_diet": {
        "diet": "healthy",
        "type": "affordability",
        "affordability_metric": "share",
        "cost_metric": "na",
    },
    "percent_of_the_population_who_cannot_afford_nutrient_adequacy": {
        "diet": "nutrient_adequate",
        "type": "affordability",
        "affordability_metric": "share",
        "cost_metric": "na",
    },
    "percent_of_the_population_who_cannot_afford_sufficient_calories": {
        "diet": "calorie_sufficient",
        "type": "affordability",
        "affordability_metric": "share",
        "cost_metric": "na",
    },
    # Affordability — number that cannot afford
    "people_who_cannot_afford_a_healthy_diet": {
        "diet": "healthy",
        "type": "affordability",
        "affordability_metric": "number",
        "cost_metric": "na",
    },
    "people_who_cannot_afford_nutrient_adequacy": {
        "diet": "nutrient_adequate",
        "type": "affordability",
        "affordability_metric": "number",
        "cost_metric": "na",
    },
    "people_who_cannot_afford_sufficient_calories": {
        "diet": "calorie_sufficient",
        "type": "affordability",
        "affordability_metric": "number",
        "cost_metric": "na",
    },
    # Cost — $ per day
    "cost_of_a_healthy_diet_in_ppp_dollars": {
        "diet": "healthy",
        "type": "cost",
        "affordability_metric": "na",
        "cost_metric": "dollars_per_day",
    },
    "cost_of_a_nutrient_adequate_diet_in_ppp_dollars": {
        "diet": "nutrient_adequate",
        "type": "cost",
        "affordability_metric": "na",
        "cost_metric": "dollars_per_day",
    },
    "cost_of_an_energy_sufficient_diet_in_ppp_dollars": {
        "diet": "calorie_sufficient",
        "type": "cost",
        "affordability_metric": "na",
        "cost_metric": "dollars_per_day",
    },
    # Cost — % of average food expenditure
    "affordability_of_a_healthy_diet__ratio_of_cost_to_food_expenditures": {
        "diet": "healthy",
        "type": "cost",
        "affordability_metric": "na",
        "cost_metric": "share_food_expenditure",
    },
    "affordability_of_a_nutrient_adequate_diet__ratio_of_cost_to_food_expenditures": {
        "diet": "nutrient_adequate",
        "type": "cost",
        "affordability_metric": "na",
        "cost_metric": "share_food_expenditure",
    },
    "affordability_of_an_energy_sufficient_diet__ratio_of_cost_to_food_expenditures": {
        "diet": "calorie_sufficient",
        "type": "cost",
        "affordability_metric": "na",
        "cost_metric": "share_food_expenditure",
    },
}


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("food_prices_for_nutrition")
    tb = ds.read("food_prices_for_nutrition", load_data=False)

    for column, dims in COLUMN_DIMENSIONS.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "food_prices"

    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["food_prices"],
        dimensions={
            "diet": ["healthy", "nutrient_adequate", "calorie_sufficient"],
            "type": ["affordability", "cost"],
            "affordability_metric": ["na", "share", "number"],
            "cost_metric": ["na", "dollars_per_day", "share_food_expenditure"],
        },
        short_name="food-prices",
        explorer=True,
    )

    c.save(tolerate_extra_indicators=True)
