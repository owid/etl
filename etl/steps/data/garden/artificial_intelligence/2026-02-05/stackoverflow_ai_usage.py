"""Garden step for Stack Overflow Developer Survey – AI tool usage.

Normalises the response labels across years so that the "Yes" category is comparable:
- 2023 & 2024: a single "Yes" row already exists.
- 2025: "Yes" was split into daily / weekly / monthly or infrequently.
  These are collapsed back into a single "Yes" row (share_pct summed)
  while the granular frequency rows are kept as separate entries.

The table is then pivoted so that each response becomes its own column,
and a "country" dimension is added (set to "World" for all rows).
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rename raw response labels to clean column names immediately after loading.
RESPONSE_TO_COLUMN = {
    "Yes": "yes_overall",
    "No, but I plan to soon": "no_but_plan_to_soon",
    "No, and I don't plan to": "no_and_dont_plan_to",
    "Yes, I use AI tools daily": "yes_daily",
    "Yes, I use AI tools weekly": "yes_weekly",
    "Yes, I use AI tools monthly or infrequently": "yes_monthly_or_infrequently",
}

# 2025 granular frequency labels (already renamed).
YES_FREQUENCY_LABELS = {"yes_daily", "yes_weekly", "yes_monthly_or_infrequently"}


def run() -> None:
    """Create garden dataset."""
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("stackoverflow_ai_usage")
    tb = ds_meadow.read("stackoverflow_ai_usage")

    #
    # Process data.
    #

    # Rename raw response labels to clean column names right away.
    tb["response"] = tb["response"].map(RESPONSE_TO_COLUMN)

    # Build a collapsed "yes" row for 2025 by summing the frequency sub-categories.
    tb_2025_yes = tb[tb["response"].isin(YES_FREQUENCY_LABELS)].copy()
    yes_2025_pct = round(tb_2025_yes["share_pct"].sum(), 1)

    # Keep all original rows (including granular 2025 frequency rows).
    # Add a single collapsed "yes" row for 2025 for cross-year comparability.
    collapsed_row = tb_2025_yes.iloc[[0]].copy().reset_index(drop=True)
    collapsed_row.loc[0, "response"] = "yes_overall"
    collapsed_row.loc[0, "share_pct"] = yes_2025_pct
    tb = pr.concat([tb, collapsed_row])

    # Add 2020 rows with share_pct = 0 for all cross-year responses
    # (AI-tool usage question was not asked before 2023, so the share is 0).
    CROSS_YEAR_RESPONSES = ["yes_overall", "no_but_plan_to_soon", "no_and_dont_plan_to"]
    tb_2020 = Table(
        {
            "year": 2020,
            "response": CROSS_YEAR_RESPONSES,
            "share_pct": 0.0,
            "n_total_responses": 0,
        },
        like=tb,
    )
    tb = pr.concat([tb, tb_2020])

    # Compute response_range before dropping n_total_responses.
    response_range = f"{tb['n_total_responses'].max()} to {tb['n_total_responses'].min()}"
    tb = tb.drop(columns=["n_total_responses"])

    # Pivot so each response becomes its own column.
    tb = tb.pivot(columns="response", values="share_pct", index="year").assign(country="World").reset_index()

    tb = tb.format(["year", "country"])
    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb],
        yaml_params={"response_range": response_range},
    )
    ds_garden.save()
