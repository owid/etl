"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions we compute from the country data.
REGIONS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "World",
    "High-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
]

# Last year the UN labels regional values as estimates; we label our regional values the same way.
LAST_ESTIMATE_YEAR = 2024

# Countries and areas in the 2024 edition (April 2025 revision).
# NOTE: Re-check this count if the producer adds or removes countries.
N_COUNTRIES = 225

AGE_GROUPS = ["15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49"]


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
    # Step 1: Harmonize country names. All the UN's regions and groups are excluded here: their numbers of married
    # women double-count some regions, so we compute our own regions from the countries instead.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)
    sanity_check_countries(tb)

    # Step 2: Compute World, continents and income groups from the countries.
    tb_regions = add_regions(tb)

    # Step 3: Combine countries and regions, keeping only the shares.
    tb = pr.concat([tb.drop(columns=["number_married_union"]), tb_regions], ignore_index=True)

    # Step 4: Round shares to one decimal.
    tb["percentage_married_union"] = tb["percentage_married_union"].round(1)

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


def add_regions(tb_countries):
    """Compute regional shares as the share of all women in the region who are married or in a union.

    Each country is weighted by its number of women, derived from the source's number of married women and share:
    women = married / share * 100. Where the share is 0 (only in the 15-19 group), we derive the number of women as
    the 15-49 total minus the other age groups.
    """
    tb = tb_countries[["country", "year", "agegroup", "percentage_married_union", "number_married_union"]].copy()
    tb["agegroup"] = tb["agegroup"].astype(str)
    tb["married"] = tb["number_married_union"]
    tb["women"] = tb["married"] / tb["percentage_married_union"] * 100
    tb.loc[tb["percentage_married_union"] == 0, "women"] = None

    # Fill the number of women in rows with a zero share.
    tb_wide = tb.pivot(index=["country", "year"], columns="agegroup", values="women")
    missing = tb_wide["15-19"].isna()
    tb_wide.loc[missing, "15-19"] = tb_wide.loc[missing, "15-49"] - tb_wide.loc[missing, AGE_GROUPS[1:]].sum(axis=1)
    assert tb_wide.notna().all().all(), "Couldn't derive the number of women for some rows."
    assert (tb_wide > 0).all().all(), "Derived a non-positive number of women."
    tb = tb.drop(columns=["women"]).merge(
        tb_wide.stack().rename("women").reset_index(), on=["country", "year", "agegroup"], how="left"
    )

    # Sum married women and all women over each region's members, and compute the share.
    tb = paths.regions.add_aggregates(
        tb[["country", "year", "agegroup", "married", "women"]],
        regions=REGIONS,
        index_columns=["country", "year", "agegroup"],
        aggregations={"married": "sum", "women": "sum"},
    )
    tb = tb[tb["country"].isin(REGIONS)].reset_index(drop=True)
    tb["percentage_married_union"] = (tb["married"] / tb["women"] * 100).copy_metadata(
        tb_countries["percentage_married_union"]
    )

    # Label regional values as the UN does: estimates up to LAST_ESTIMATE_YEAR, projections afterwards.
    tb["dataprocess"] = "Estimate"
    tb.loc[tb["year"] > LAST_ESTIMATE_YEAR, "dataprocess"] = "Projection"

    return tb[["country", "year", "agegroup", "dataprocess", "percentage_married_union"]]


def sanity_check_countries(tb) -> None:
    # Expected number of countries, and no regions left among them.
    assert tb["country"].nunique() == N_COUNTRIES, f"Expected {N_COUNTRIES} countries, found {tb['country'].nunique()}."
    regions_left = set(tb["country"]) & set(REGIONS)
    assert not regions_left, f"Source regions still among the countries: {regions_left}"
    # One row (estimate or projection) per country, year and age group.
    assert not tb.duplicated(subset=["country", "year", "agegroup"]).any(), "Duplicate country-year-age rows."
    # The 15-49 number of each country equals the sum of its five-year age groups.
    tb_wide = tb.pivot_table(
        index=["country", "year"], columns="agegroup", values="number_married_union", observed=True
    )
    rel_diff = (tb_wide[AGE_GROUPS].sum(axis=1) / tb_wide["15-49"] - 1).abs()
    assert (rel_diff < 1e-3).all(), (
        f"Age groups don't add up to 15-49 for: {list(rel_diff[rel_diff >= 1e-3].index[:5])}"
    )


def sanity_check_outputs(tb) -> None:
    # Shares are percentages, with no gaps.
    assert tb["percentage_married_union"].notna().all(), "Missing shares."
    assert tb["percentage_married_union"].between(0, 100).all(), "Shares outside [0, 100]."
    # All regions are present.
    missing = set(REGIONS) - set(tb["country"])
    assert not missing, f"Missing regions: {missing}"
    # Regional shares lie between the lowest and highest country share (they are weighted averages).
    tb_countries = tb[~tb["country"].isin(REGIONS)]
    bounds = tb_countries.groupby(["year", "agegroup"], observed=True)["percentage_married_union"].agg(["min", "max"])
    tb_regions = tb[tb["country"].isin(REGIONS)].join(bounds, on=["year", "agegroup"])
    assert tb_regions["percentage_married_union"].between(tb_regions["min"], tb_regions["max"]).all(), (
        "A regional share is outside the range of country shares."
    )
