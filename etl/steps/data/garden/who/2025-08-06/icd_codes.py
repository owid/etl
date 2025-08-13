"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("icd_codes")

    # Read table from meadow dataset.
    tb = ds_meadow["icd_codes"].reset_index()
    origins = tb["icd"].metadata.origins

    icd_codes_clean = {"Icd10": "ICD-10", "Icd9": "ICD-9", "Icd8": "ICD-8", "Icd7": "ICD-7"}
    tb["icd"] = tb["icd"].cat.rename_categories(lambda x: icd_codes_clean.get(x, x))
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Calculate sum of number of countries using each ICD code type each year
    tb_sum = tb.groupby(["year", "icd"], observed=True).count().reset_index()
    tb_sum = tb_sum.rename(columns={"icd": "country", "country": "countries_using_icd_code"})
    # Copy metadata from the original table
    # Format the datasets
    tb = tb.format(["country", "year"], short_name="icd_country_year")
    tb_sum = tb_sum.format(["country", "year"], short_name="icd_totals")

    # Getting those origins back in
    tb["icd"].metadata.origins = origins
    tb_sum["countries_using_icd_code"].metadata.origins = origins
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_sum], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
