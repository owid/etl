"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ['Africa','Asia', 'Europe', 'North America', 'Oceania', 'South America', 'World']

def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ethnologue")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb_country_codes = ds_meadow["country_codes"].reset_index()
    tb_language_codes = ds_meadow["language_codes"].reset_index()
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

    # countries by status living and extinct
    tb_lang_by_status = extinct_and_living_languages_per_country(tb_language_index, tb_language_codes, tb_country_codes)
    tb_lang_by_status["year"] = 2024
    tb_lang_by_status["total"] = tb_lang_by_status["living"] + tb_lang_by_status["extinct"]
    tb_lang_by_status = geo.add_population_to_table(tb_lang_by_status, ds_population)
    cols = ["total", "living", "extinct"]
    for col in cols:
        tb_lang_by_status[f"{col}_per_million"] = tb_lang_by_status[col] / tb_lang_by_status["population"] * 1_000_000
    tb_lang_by_status = tb_lang_by_status.format(["country", "year"], short_name="languages_by_status")
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

def countries_per_region()
    return


def extinct_and_living_languages_per_country(
    tb_language_index: Table, tb_language_codes: Table, tb_country_codes: Table
) -> Table:
    tb_extinct_living_languages = (
        tb_language_index.merge(tb_language_codes, on=["langid"], how="outer", suffixes=("", "_lang"))
        .drop(columns=["nametype", "name", "name_lang"])
        .drop_duplicates()
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
