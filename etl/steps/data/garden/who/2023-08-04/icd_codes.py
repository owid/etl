"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("icd_codes"))

    # Read table from meadow dataset.
    tb = ds_meadow["icd_codes"]
    tb = tb.reset_index()
    icd_codes_sum = {"Icd10": "ICD-10", "Icd9": "ICD-9", "Icd8": "ICD-8", "Icd7": "ICD-7"}
    icd_codes_map = {"Icd10": "04. ICD-10", "Icd9": "03. ICD-9", "Icd8": "02. ICD-8", "Icd7": "01. ICD-7"}

    # Calculate sum of number of countries using each ICD code type each year
    tb_sum = tb.groupby(["year", "icd"]).count().reset_index()
    tb_sum = tb_sum.rename(columns={"icd": "country", "country": "countries_using_icd_code"})
    tb_sum["country"] = tb_sum["country"].replace(icd_codes_sum)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb["icd"] = tb["icd"].replace(icd_codes_map)

    # Combine the datasets
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    tb.metadata.short_name = "icd_country_year"
    tb_sum = tb_sum.set_index(["country", "year"], verify_integrity=True)
    tb_sum.metadata.short_name = "icd_totals"
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_sum], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
