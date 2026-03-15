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
    """Process MMLU benchmark data, computing best score per country per year."""
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("mmlu")
    tb = ds_meadow.read("mmlu").reset_index()

    #
    # Process data.
    #
    # Convert EM score to percentage.
    tb["em"] = tb["em"] * 100

    # Assign each model to a region.
    tb["country"] = tb["country"].apply(assign_region)

    # Ensure release_date is datetime, then parse year.
    tb["release_date"] = tb["release_date"].astype("datetime64[ns]")

    # Parse year from release date.
    tb["year"] = tb["release_date"].dt.year

    # Keep best score per country per year (the frontier model for each region each year).
    tb = tb.groupby(["year", "country"], as_index=False)["em"].max()
    tb["em"] = tb["em"].round(1)

    tb = tb.format(["year", "country"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
