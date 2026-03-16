"""Process MMLU benchmark data by country of origin for garden step."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Countries to merge into "Europe"
EUROPE_COUNTRIES = {
    "France",
    "United Kingdom of Great Britain and Northern Ireland",
    "Germany",
}

# Simplified country name mapping
COUNTRY_NAME_MAP = {
    "United States of America": "United States",
    "United Arab Emirates": "United Arab Emirates",
    "Canada": "Canada",
    "China": "China",
}


def assign_region(country_str: str) -> str:
    """Assign a country (or comma-separated list of countries) to a region.

    Multi-country models are assigned based on a priority: China > United States > Europe > other.
    France, the United Kingdom, and Germany are merged into 'Europe'.
    """
    countries = [c.strip() for c in country_str.split(",")]

    has_china = any("China" in c for c in countries)
    has_us = any("United States" in c for c in countries)
    has_europe = any(c in EUROPE_COUNTRIES for c in countries)
    has_uae = any("United Arab Emirates" in c for c in countries)
    has_canada = any("Canada" in c for c in countries)

    if has_china:
        return "China"
    if has_us:
        return "United States"
    if has_europe:
        return "Europe"
    if has_uae:
        return "United Arab Emirates"
    if has_canada:
        return "Canada"
    # Fallback: use the first listed country (simplified)
    first = countries[0]
    return COUNTRY_NAME_MAP.get(first, first)


def run() -> None:
    """Process MMLU benchmark data, keeping only models that set a new record per country."""
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("mmlu")
    tb = ds_meadow.read("mmlu").reset_index()

    #
    # Process data.
    #
    # Convert EM score to percentage.
    tb["em"] = (tb["em"] * 100).round(1)

    # Assign each model to a region.
    tb["country"] = tb["country"].apply(assign_region)

    # Ensure release_date is datetime.
    tb["release_date"] = tb["release_date"].astype("datetime64[ns]")

    # Drop rows with missing scores.
    tb = tb.dropna(subset=["em"])

    # For each (country, release_date), keep only the best-scoring model and its name.
    idx = tb.groupby(["country", "release_date"])["em"].idxmax()
    tb = tb.loc[idx].reset_index(drop=True)

    # For each country, keep only models that strictly improve on all previous scores
    # (i.e. the frontier/record-setting models over time).
    tb = tb.sort_values(["country", "release_date"]).reset_index(drop=True)
    tb["cummax"] = tb.groupby("country")["em"].cummax()
    tb["prev_cummax"] = tb.groupby("country")["cummax"].shift(1).fillna(0)
    tb = tb[tb["em"] > tb["prev_cummax"]].drop(columns=["cummax", "prev_cummax"])

    # Table 1: indexed by (release_date, model_name) — model name as the grapher entity.
    tb_models = tb.rename(columns={"country": "country_name", "name": "country"})
    tb_models = tb_models[["release_date", "country", "em", "country_name"]].reset_index(drop=True)
    tb_models = tb_models.format(["release_date", "country"], short_name="mmlu_by_model")

    # Table 2: indexed by (release_date, country) — region as the grapher entity, no model name.
    tb_country = tb[["release_date", "country", "em"]].reset_index(drop=True)
    tb_country = tb_country.format(["release_date", "country"], short_name="mmlu_by_country")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb_models, tb_country], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
