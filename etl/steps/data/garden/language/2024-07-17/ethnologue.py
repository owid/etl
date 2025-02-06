"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]
YEAR_OF_UPDATE = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ethnologue")
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb_country_codes = ds_meadow["country_codes"].reset_index()
    tb_language_codes = ds_meadow["language_codes"].reset_index()
    tb_language_index = ds_meadow["language_index"].reset_index()
    # Store the origins to add back to indicators later
    origins = tb_country_codes["country"].metadata.origins
    #
    # Drop the area column as this lists continents according to SIL
    tb_country_codes = tb_country_codes.drop(columns="area")
    tb_country_codes = geo.harmonize_countries(
        df=tb_country_codes,
        countries_file=paths.country_mapping_path,
    )
    # Add OWID continents
    tb_country_codes = add_region_to_table(ds_regions, tb_country_codes)

    # The number of living and extinct languages per entity
    tb_lang_by_status = extinct_and_living_languages_per_country(tb_language_index, tb_language_codes, tb_country_codes)
    tb_lang_by_status["year"] = YEAR_OF_UPDATE
    # The total number of languages
    tb_lang_by_status["total"] = tb_lang_by_status["living"] + tb_lang_by_status["extinct"]
    # Tidy up and add origins back in
    tb_lang_by_status = tb_lang_by_status.format(["country", "year"], short_name="languages_by_status")
    tb_lang_by_status = tb_lang_by_status.drop(columns=["population"])
    for col in tb_lang_by_status.columns:
        tb_lang_by_status[col].metadata.origins = origins
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_lang_by_status],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_region_to_table(ds_regions: Dataset, tb_country_codes: Table):
    for region in REGIONS:
        countries_in_region = geo.list_members_of_region(ds_regions=ds_regions, region=region)
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
