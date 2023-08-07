"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table, Variable, VariableMeta
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("mortality_database"))

    # Read table from meadow dataset.
    tb = ds_meadow["mortality_database"]
    tb = tb.reset_index()
    #
    # Process data.
    #
    tb = tidy_sex_dimension(tb)
    tb = tidy_age_dimension(tb)
    tb = tidy_causes_dimension(tb)
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    ds_garden = add_metadata(dest_dir=dest_dir, ds_meadow=ds_meadow, tb=tb)

    # Save changes in the new garden dataset.
    ds_garden.save()


def tidy_causes_dimension(tb: Table) -> Table:
    """
    To clarify blood disorders are also included in this group.
    """
    cause_dict = {"Diabetes mellitus and endocrine disorders": "Diabetes mellitus, blood and endocrine disorders"}
    tb["cause"] = tb["cause"].replace(cause_dict, regex=False)
    return tb


def tidy_sex_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}
    tb["sex"] = tb["sex"].replace(sex_dict, regex=False)
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

    tb["age_group"] = tb["age_group"].replace(age_dict, regex=False)

    return tb


def add_metadata(dest_dir: str, ds_meadow: Dataset, tb: Table) -> Dataset:
    """
    Adding metadata at the variable level. The first step is to group by the dims, which are : age, sex and cause, icd10_codes and broad_cause_group.
    Age, sex and cause are used to name the tables, icd10_codes and broad_cause_group are somewhat extraneous but could be useful in the variable description.
    Then for each variable (the different metrics) we add in the metadata.

    Parameters
    ----------
    dest_dir : str
        Compulsory etl destination directory.
    ds_meadow : Dataset
        The meadow Dataset, the metadata of this dataset is used in the garden Dataset.
    tb: Table
        The Table with the full dataset.

    Returns
    -------
    ds_garden: Dataset:
        Dataset containing tables with causes of mortality for each group ("cause", "sex", "age_group"), with variable level metadata.
    """
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)
    dims = ["cause", "sex", "age_group", "icd10_codes", "broad_cause_group"]
    tb_group = tb.groupby(dims)
    for group_id, group in tb_group:
        # Grab out the IDs of each of the grouping factors, e.g. the age-group, sex and cause
        group = Table(group)
        dims_id = dict(zip(dims, group_id))
        # Create the unique table short name as a combination of cause, sex and age
        keys_to_extract = ["cause", "sex", "age_group"]
        # Extract values for the specified keys
        name_list = [dims_id[key] for key in keys_to_extract]
        group.metadata.short_name = underscore(" - ".join(name_list))[0:240]
        variables = group.columns.drop(dims + ["country", "year"])
        for variable_name in variables:
            group[variable_name] = Variable(group[variable_name])
            # Create all the necessary metadata
            cleaned_variable = create_variable_metadata(variable=group[variable_name], **dims_id)
            group[cleaned_variable.name] = cleaned_variable
            # Drop the original variable name as the same data with new variable name now it exists
            group = group.drop(columns=variable_name)
            # dropping columns that are totally empty - not all combinations of variables exist
        group = group.dropna(axis=1, how="all")
        # Dropping dims as table name contains them
        group = group.drop(columns=dims)
        group = group.set_index(["country", "year"], verify_integrity=True)
        ds_garden.add(group)
    return ds_garden


def create_variable_metadata(
    variable: Variable, cause: str, age_group: str, sex: str, icd10_codes: str, broad_cause_group: str
) -> Variable:
    """
    Creates the metadata for the four metrics in each table. Each table is a group for every cause-sex-age combination and there is additional information included on the ICD10 codes and the broad cause group (e.g. Injuries). The broad cause group is not currently used.
    To add additional information the desired column should be added to the `dims` argument set in add_metadata().

    Parameters
    ----------
    variable : Variable
        Original variable with no assigned metadata.
    cause : str
        The cause of death dimension.
    age_group: str
        The age-group dimension.
    sex: str
        The sex dimension.
    broad_cause_group: str
        The broad cause of death group e.g. Injuries. Not currently used.

    Returns
    -------
    new_variable: Variable
        Variable with tailored metadata according to the required metric and cause, age and sex dimensions.
    """
    var_name_dict = {
        "number": {
            "title": f"Total deaths that are from {cause.lower()}" + f", in {sex.lower()} aged {age_group.lower()}",
            "description": f"{cause} has the following ICD 10 codes: {icd10_codes}.",
            "unit": "deaths",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "percentage_of_cause_specific_deaths_out_of_total_deaths": {
            "title": f"Share of total deaths in {sex.lower()} aged {age_group.lower()} years that are from {cause.lower()}",
            "description": f"{cause} has the following ICD 10 codes: {icd10_codes}.",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "age_standardized_death_rate_per_100_000_standard_population": {
            "title": f"Age-standardized deaths that are from {cause.lower()}"
            + f" per 100,000 people, in {sex.lower()} aged {age_group.lower()}",
            "description": f"{cause} has the following ICD 10 codes: {icd10_codes}. The data is standardized using the WHO standard population.",
            "unit": "deaths per 100,000 people",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "death_rate_per_100_000_population": {
            "title": f"Deaths from {cause.lower()}" + f" per 100,000 people in, {sex.lower()} aged {age_group.lower()}",
            "description": f"{cause} has the following ICD 10 codes: {icd10_codes}.",
            "unit": "deaths per 100,000 people",
            "short_unit": "",
            "num_decimal_places": 1,
        },
    }
    new_variable = variable.copy()
    new_variable.name = underscore(var_name_dict[variable.name]["title"])
    new_variable.metadata = VariableMeta(
        title=var_name_dict[variable.name]["title"],
        description=var_name_dict[variable.name]["description"],
        unit=var_name_dict[variable.name]["unit"],
        short_unit=var_name_dict[variable.name]["short_unit"],
    )
    new_variable.metadata.display = {
        "name": var_name_dict[variable.name]["title"],
        "numDecimalPlaces": var_name_dict[variable.name]["num_decimal_places"],
    }

    return new_variable
