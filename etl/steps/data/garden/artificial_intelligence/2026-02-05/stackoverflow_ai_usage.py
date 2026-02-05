"""Garden step for Stack Overflow Developer Survey – AI tool usage.

Normalises the response labels across years so that the "Yes" category is comparable:
- 2023 & 2024: a single "Yes" row already exists.
- 2025: "Yes" was split into daily / weekly / monthly or infrequently.
  These are collapsed back into a single "Yes" row (share_pct summed)
  while the granular frequency rows are kept as separate entries.
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping from 2025 granular labels to the collapsed "Yes" category.
YES_FREQUENCY_LABELS = {
    "Yes, I use AI tools daily",
    "Yes, I use AI tools weekly",
    "Yes, I use AI tools monthly or infrequently",
}


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
    # Build a collapsed "Yes" row for 2025 by summing the frequency sub-categories.
    tb_2025_yes = tb[tb["response"].isin(YES_FREQUENCY_LABELS)].copy()
    yes_2025_pct = round(tb_2025_yes["share_pct"].sum(), 1)

    # Keep all original rows (including granular 2025 frequency rows).
    # Add a single collapsed "Yes" row for 2025 for cross-year comparability.
    collapsed_row = tb_2025_yes.iloc[[0]].copy().reset_index(drop=True)
    collapsed_row.loc[0, "response"] = "Yes"
    collapsed_row.loc[0, "share_pct"] = yes_2025_pct
    tb = pr.concat([tb, collapsed_row])

    # Sort for readability: year, then Yes first, then alphabetical.
    response_order = {"Yes": 0, "No, but I plan to soon": 1, "No, and I don't plan to": 2}
    tb["_sort_key"] = tb["response"].map(response_order).fillna(3)
    tb = tb.sort_values(["year", "_sort_key", "response"]).drop(columns=["_sort_key"]).reset_index(drop=True)
    response_range = f"{tb['n_total_responses'].max()} to {tb['n_total_responses'].min()}"
    tb = tb.drop(columns=["n_total_responses"])

    tb = tb.format(["year", "response"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        yaml_params={"response_range": response_range},
    )
    ds_garden.save()
