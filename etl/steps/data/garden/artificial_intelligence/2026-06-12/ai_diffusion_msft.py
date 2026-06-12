"""Reshape Microsoft AI Diffusion data from wide to long format for time series."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map wide-format column names to end-of-period dates.
# End-of-period dates follow Microsoft's convention: their time-series charts
# plot each rolling-average value at the right edge of its window.
WAVE_TO_DATE = {
    "ai_diffusion_h1_2025": "2025-06-30",
    "ai_diffusion_h2_2025": "2025-12-31",
    "ai_diffusion_q1_2026": "2026-03-31",
}


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("ai_diffusion_msft")
    tb = ds_meadow.read("ai_diffusion_msft")

    #
    # Process data.
    #
    # Melt from wide to long: one row per (economy, wave).
    tb = tb.melt(id_vars=["economy"], var_name="wave", value_name="ai_user_share")

    # Map wave column to date and drop the wave label.
    tb["date"] = tb["wave"].map(WAVE_TO_DATE)
    tb = tb.drop(columns=["wave"])

    # Rename economy to country.
    tb = tb.rename(columns={"economy": "country"})

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Sanity checks.
    sanity_check_outputs(tb)

    # Format the table.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_outputs(tb):
    """Check output data quality."""
    # We expect 3 waves per country.
    counts = tb.groupby("country").size()
    assert (counts == 3).all(), f"Some countries don't have exactly 3 waves: {counts[counts != 3].to_dict()}"

    # Values should be in 0-100 range.
    assert tb["ai_user_share"].between(0, 100).all(), "Out-of-range values found"

    # Should have >100 countries.
    assert tb["country"].nunique() > 100, f"Expected >100 countries, got {tb['country'].nunique()}"
