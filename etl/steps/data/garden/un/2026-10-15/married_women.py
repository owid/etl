"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Number of regional entities (regions, development groups and income groups) we keep from the source's "Regions" sheet.
# NOTE: Re-check this count if the producer adds or removes regional groupings.
N_SOURCE_REGIONS = 42


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("married_women")

    # Read table from meadow dataset.
    tb = ds_meadow.read("married_women")

    #
    # Process data.
    #
    # Step 1: Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Step 2: Convert the number of women from thousands (as published) to people.
    tb["number_married_union"] *= 1000

    # Step 3: Remove the number of women for regions.
    # The source's regional numbers are about twice the sum of their member countries (e.g. World is ~2.4 billion
    # in 2024, while countries add up to ~1.25 billion), so we keep only the shares for regions.
    countries = set(paths.regions.tb_regions.loc[paths.regions.tb_regions["region_type"] == "country", "name"])
    is_region = ~tb["country"].isin(countries)
    assert tb.loc[is_region, "country"].nunique() == N_SOURCE_REGIONS, (
        f"Expected {N_SOURCE_REGIONS} regional entities, found: {sorted(tb.loc[is_region, 'country'].unique())}"
    )
    tb.loc[is_region, "number_married_union"] = None

    sanity_check_outputs(tb)

    # dataprocess specifies whether the data is an estimate or a projection.
    tb = tb.format(["country", "year", "agegroup", "dataprocess"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_outputs(tb) -> None:
    # Shares are percentages.
    assert tb["percentage_married_union"].between(0, 100).all(), "Shares outside [0, 100]."
    # Numbers are non-negative and, after converting from thousands, in a plausible range (India has ~275 million).
    assert (tb["number_married_union"].dropna() >= 0).all(), "Negative number of women."
    assert 1e8 < tb["number_married_union"].max() < 5e8, (
        "Largest number of women is implausible: check the thousands conversion."
    )
    # Coverage didn't shrink (225 countries and areas in the 2024 edition, April 2025 revision).
    n_countries = tb["number_married_union"].notna().groupby(tb["country"], observed=True).any().sum()
    assert n_countries >= 225, f"Only {n_countries} countries with data."
    # The 15-49 number of each country equals the sum of its five-year age groups.
    tb_wide = tb.dropna(subset=["number_married_union"]).pivot_table(
        index=["country", "year"], columns="agegroup", values="number_married_union", observed=True
    )
    age_groups = [c for c in tb_wide.columns if c != "15-49"]
    rel_diff = (tb_wide[age_groups].sum(axis=1) / tb_wide["15-49"] - 1).abs()
    assert (rel_diff < 1e-3).all(), f"Age groups don't add up to 15-49 for: {rel_diff[rel_diff >= 1e-3].index[:5]}"
