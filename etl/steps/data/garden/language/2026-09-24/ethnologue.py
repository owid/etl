"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]
YEAR_OF_UPDATE = 2026
# Coverage floor: number of countries in the 2026 edition's CountryCodes.tab (unchanged from 2025).
MIN_COUNTRIES = 242
# Plausible bounds on the global number of living languages (7,170 in the 2026 edition; "over 7,000" per SIL).
WORLD_LIVING_BOUNDS = (7000, 7500)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ethnologue")
    # Read table from meadow dataset.
    tb_country_codes = ds_meadow.read("country_codes")
    tb_language_codes = ds_meadow.read("language_codes")
    tb_language_index = ds_meadow.read("language_index")
    sanity_check_inputs(tb_country_codes, tb_language_codes, tb_language_index)
    # Store the origins to add back to indicators later
    origins = tb_country_codes["country"].metadata.origins
    #
    # Drop the area column as this lists continents according to SIL
    tb_country_codes = tb_country_codes.drop(columns="area")
    tb_country_codes = paths.regions.harmonize_names(tb_country_codes)
    # Add OWID continents
    tb_country_codes = add_region_to_table(tb_country_codes)

    # The number of living and extinct languages per entity
    tb_lang_by_status = extinct_and_living_languages_per_country(tb_language_index, tb_language_codes, tb_country_codes)
    tb_lang_by_status["year"] = YEAR_OF_UPDATE
    # The total number of languages
    tb_lang_by_status["total"] = tb_lang_by_status["living"] + tb_lang_by_status["extinct"]
    sanity_check_outputs(tb_lang_by_status)
    # Tidy up and add origins back in
    tb_lang_by_status = tb_lang_by_status.format(["country", "year"], short_name="languages_by_status")
    for col in tb_lang_by_status.columns:
        tb_lang_by_status[col].metadata.origins = origins
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_lang_by_status],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb_country_codes: Table, tb_language_codes: Table, tb_language_index: Table) -> None:
    assert set(tb_country_codes.columns) == {"countryid", "country", "area"}, (
        f"Unexpected CountryCodes columns: {list(tb_country_codes.columns)}"
    )
    assert set(tb_language_codes.columns) == {"langid", "countryid", "langstatus", "name"}, (
        f"Unexpected LanguageCodes columns: {list(tb_language_codes.columns)}"
    )
    assert set(tb_language_index.columns) == {"langid", "countryid", "nametype", "name"}, (
        f"Unexpected LanguageIndex columns: {list(tb_language_index.columns)}"
    )
    # Only living (L) and extinct (X) statuses are handled downstream; a new status would silently add a column.
    statuses = set(tb_language_codes["langstatus"].astype(str))
    assert statuses <= {"L", "X"}, f"Unexpected language statuses: {statuses - {'L', 'X'}}"
    assert not tb_language_codes["langid"].duplicated().any(), "Duplicate language codes in LanguageCodes."
    assert len(tb_country_codes) >= MIN_COUNTRIES, (
        f"CountryCodes has {len(tb_country_codes)} countries, fewer than the expected {MIN_COUNTRIES}."
    )
    unknown_countries = set(tb_language_index["countryid"].astype(str)) - set(tb_country_codes["countryid"].astype(str))
    assert not unknown_countries, (
        f"LanguageIndex references country codes missing from CountryCodes: {unknown_countries}"
    )


def sanity_check_outputs(tb: Table) -> None:
    assert not tb["country"].isna().any(), "Rows with a missing country (unmapped country code)."
    assert not tb["country"].duplicated().any(), "Duplicate entities in the output."
    assert set(REGIONS + ["World"]) <= set(tb["country"]), "Missing continent or World aggregates."
    cols = ["living", "extinct", "total"]
    assert not tb[cols].isna().any().any(), "Missing language counts in the output."
    assert (tb[cols] >= 0).all().all(), "Negative language counts in the output."
    assert (tb["total"] == tb["living"] + tb["extinct"]).all(), "Total does not equal living + extinct."
    world = tb[tb["country"] == "World"].iloc[0]
    assert WORLD_LIVING_BOUNDS[0] <= world["living"] <= WORLD_LIVING_BOUNDS[1], (
        f"World living languages ({world['living']}) outside plausible bounds {WORLD_LIVING_BOUNDS}."
    )
    # A language is counted once per entity, so no entity can exceed the global total.
    others = tb[tb["country"] != "World"]
    assert (others[cols].max() <= world[cols]).all(), "An entity has more languages than the World total."


def add_region_to_table(tb_country_codes: Table) -> Table:
    for region in REGIONS:
        countries_in_region = paths.regions.get_region(region)["members"]
        tb_country_codes.loc[tb_country_codes["country"].isin(countries_in_region), "region"] = region
    return tb_country_codes


def languages_per_region(tb_language_index: Table, tb_language_codes: Table, tb_country_codes: Table):
    tb_languages_per_region = (
        tb_language_index.merge(tb_language_codes, on=["langid"], how="outer", suffixes=("", "_lang"))
        .merge(tb_country_codes, on=["countryid"])
        .drop(columns=["countryid", "nametype", "name", "countryid_lang", "name_lang", "country"])
        .drop_duplicates()
        .groupby(["region", "langstatus"], observed=True)["langid"]
        .nunique()
        .unstack(fill_value=0)
        .rename(columns={"L": "living", "X": "extinct"})
        .reset_index()
        .rename(columns={"region": "country"})
    )
    return tb_languages_per_region


def extinct_and_living_languages_per_country(
    tb_language_index: Table, tb_language_codes: Table, tb_country_codes: Table
) -> Table:
    """
    This function calculates the number of both extinct and living languages in each country, region and globally.
    Regions must be calculated separately from countries to ensure a language isn't counted multiple times for each country it exists in.
    """
    tb_extinct_living_languages = (
        tb_language_index.merge(tb_language_codes, on="langid", how="outer", suffixes=("", "_lang"))
        .drop(columns=["nametype", "name", "name_lang"])
        .drop_duplicates()
        .groupby(["countryid", "langstatus"], observed=True)["langid"]
        .nunique()
        .unstack(fill_value=0)
        .rename(columns={"L": "living", "X": "extinct"})
        .reset_index()
        .merge(tb_country_codes, on="countryid", how="left")
        .drop(columns=["countryid", "region"])
    )
    # Calculating the number of extinct and living languages per region
    tb_region = languages_per_region(tb_language_index, tb_language_codes, tb_country_codes)

    # Calculate the number of living and extinct languages globally
    tb_global = tb_language_codes.groupby(["langstatus"], observed=True)["langid"].nunique().reset_index()
    tb_global["country"] = "World"
    tb_global = (
        tb_global.pivot(index="country", columns=["langstatus"], values="langid")
        .reset_index()
        .rename(columns={"L": "living", "X": "extinct"})
    )

    tb_combined = pr.concat([tb_extinct_living_languages, tb_region, tb_global], ignore_index=True)
    return tb_combined
