"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The report covers H2 2025 (July–December 2025).
YEAR = 2025


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("ai_diffusion_msft")
    tb = ds_meadow.read("ai_diffusion_msft")

    #
    # Process data.
    #
    # Bring the economy index into a column and keep only H2 2025.
    tb = tb.reset_index()[["economy", "ai_diffusion_h2_2025"]].rename(
        columns={"economy": "country", "ai_diffusion_h2_2025": "ai_user_share"}
    )

    # Harmonize country names.
    tb = paths.regions.harmonize_names(
        tb=tb,
        country_col="country",
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Add year column.
    tb["year"] = YEAR

    # Format the table.
    tb = tb.format(["country", "year"], short_name="ai_diffusion_msft")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
