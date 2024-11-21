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
    ds_meadow = paths.load_dataset("mortality_database_cancer")
    tb = ds_meadow.read("mortality_database_cancer", safe_types=False)

    #
    # Process data.
    #
    tb = tidy_sex_dimension(tb)
    tb = tidy_age_dimension(tb)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year", "sex", "age_group", "cause", "icd10_codes"])
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=False, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def tidy_sex_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}
    tb["sex"] = tb["sex"].cat.rename_categories(lambda x: sex_dict.get(x, x))
    return tb


def tidy_age_dimension(tb: Table) -> Table:
    age_dict = {
        "[Unknown]": "Unknown age",
        "[85+]": "over 85 years",
        "[80-84]": "80-84 years",
        "[75-79]": "75-79 years",
        "[70-74]": "70-74 years",
        "[65-69]": "65-69 years",
        "[60-64]": "60-64 years",
        "[55-59]": "55-59 years",
        "[50-54]": "50-54 years",
        "[45-49]": "45-49 years",
        "[40-44]": "40-44 years",
        "[35-39]": "35-39 years",
        "[30-34]": "30-34 years",
        "[25-29]": "25-29 years",
        "[20-24]": "20-24 years",
        "[15-19]": "15-19 years",
        "[10-14]": "10-14 years",
        "[5-9]": "5-9 years",
        "[1-4]": "1-4 years",
        "[0]": "less than 1 year",
        "[All]": "all ages",
    }

    tb["age_group"] = tb["age_group"].cat.rename_categories(lambda x: age_dict.get(x, x))
    return tb
