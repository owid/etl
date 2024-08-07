"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ethnologue")

    # Read table from meadow dataset.
    tb_country_codes = ds_meadow["country_codes"].reset_index()
    # tb_language_codes = ds_meadow["language_codes"].reset_index()
    tb_language_index = ds_meadow["language_index"].reset_index()

    origins = tb_country_codes["country"].metadata.origins
    #
    tb_country_codes = tb_country_codes.drop(columns="area")
    # Process data.
    #
    tb_country_codes = geo.harmonize_countries(
        df=tb_country_codes,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Calculate the total number of languages per country
    tb_languages_per_country = languages_per_country(tb_language_index, tb_country_codes)
    tb_languages_per_country["year"] = 2024
    tb_languages_per_country = tb_languages_per_country.drop(columns="countryid")

    # tb_lang_by_status = extinct_and_living_languages_per_country(tb_language_index, tb_language_codes, tb_country_codes)
    tb_languages_per_country = tb_languages_per_country.format(["country", "year"], short_name="languages_per_country")
    tb_languages_per_country["n"].metadata.origins = origins
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_languages_per_country], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def countries_per_language(tb_language_index: Table, tb_country_codes: Table, tb_language_codes: Table) -> Table:
    tb_countries_per_language = (
        tb_country_codes.merge(tb_language_index, on="countryid", how="inner")
        .loc[:, ["countryid", "langid"]]
        .drop_duplicates()
        .groupby("langid")
        .size()
        .reset_index(name="number_of_countries")
        .sort_values(by="number_of_countries", ascending=False)
        .merge(tb_language_codes, on="langid", how="inner")
    )

    tb_countries_per_language = tb_countries_per_language[["name", "number_of_countries"]]
    return tb_countries_per_language


def languages_per_country(tb_language_index: Table, tb_country_codes: Table) -> Table:
    tb_languages_per_country = (
        tb_language_index.groupby("countryid", observed=True)["langid"].nunique().reset_index(name="n")
    ).reset_index(drop=True)
    assert tb_languages_per_country["n"].isnull().sum() == 0
    tb_languages_per_country = tb_languages_per_country.merge(tb_country_codes, on="countryid", how="left")

    return tb_languages_per_country


def extinct_and_living_languages_per_country(
    tb_language_index: Table, tb_language_codes: Table, tb_country_codes: Table
) -> Table:
    tb_extinct_living_languages = (
        tb_language_index.merge(tb_language_codes, on=["langid", "countryid"], how="outer", suffixes=("", "_lang"))
        .groupby(["countryid", "langstatus"], observed=True)
        .agg({"langid": "nunique"})
        .reset_index()
        .rename(columns={"langid": "number_of_languages"})
        .replace({"langstatus": {"L": "living", "X": "extinct"}})
        .merge(tb_country_codes, on="countryid", how="left")
        .pivot(index="country", columns="langstatus", values="number_of_languages")
        .fillna(0)
        .reset_index()
    )
    return tb_extinct_living_languages
