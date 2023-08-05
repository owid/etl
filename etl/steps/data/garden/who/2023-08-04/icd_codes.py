"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import owid.catalog.processing as pr
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
    icd_codes_map = {"Icd10": "ICD-10", "Icd9": "ICD-9", "Icd8": "ICD-8", "Icd7": "ICD-7"}
    tb["icd"] = tb["icd"].replace(icd_codes_map)

    # Calculate sum of number of countries using each ICD code type each year
    tb_sum = tb.groupby(["year", "icd"]).count().reset_index()
    tb_sum = tb_sum.rename(columns={"icd": "country", "country": "countries_using_icd_code"})

    # More of a traditional grapher dataset. For each country-year, which ICD code is being used.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Combine the datasets
    tb_combined = pr.concat([tb_sum, tb])
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True)
    tb_combined.metadata.short_name = "icd_codes"
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
