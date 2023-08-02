"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table, Variable

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

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
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_variable_metadata(variable: Variable, cause: str, age: str, sex: str, rei: str = "None"):
    var_name_dict = {
        "Deaths - Share of the population": {
            "title": f"Share of total deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei is not None else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "DALYs (Disability-Adjusted Life Years) - Share of the population": {
            "title": f"Share of total DALYs that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "Deaths - Rate": {
            "title": f"Deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f" per 100,000 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "deaths per 100,000 people",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "DALYs (Disability-Adjusted Life Years) - Rate": {
            "title": f"DALYs from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f" per 100,000 people in, {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "DALYs per 100,000 people",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Deaths - Percent": {
            "title": f"Share of total deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "DALYs (Disability-Adjusted Life Years) - Percent": {
            "title": f"Share of total DALYs that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "Deaths - Number": {
            "title": f"Deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "deaths",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "DALYs (Disability-Adjusted Life Years) - Number": {
            "title": f"DALYs that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "DALYs",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Incidence - Number": {
            "title": f"Number of new cases of {cause.lower()}, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "Prevalence - Number": {
            "title": f"Current number of cases of {cause.lower()}, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "Incidence - Rate": {
            "title": f"Number of new cases of {cause.lower()} per 100,000 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Prevalence - Rate": {
            "title": f"Current number of cases of {cause.lower()} per 100,000 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Incidence - Share of the population": {
            "title": f"Number of new cases of {cause.lower()} per 100 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "Prevalence - Share of the population": {
            "title": f"Current number of cases of {cause.lower()} per 100 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
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


def add_metadata(dest_dir: str, ds_meadow: Dataset, df: pd.DataFrame, dims: List[str]) -> Dataset:
    """
    Adding metadata at the variable level
    First step is to group by the dims, which are normally: age, sex and cause.
    Then for each variable (the different metrics) we add in the metadata.
    """
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    df = df.reset_index()
    df_group = df.groupby(dims)
    for group_id, group in df_group:
        # Grab out the IDs of each of the grouping factors, e.g. the age-group, sex and cause
        dims_id = dict(zip(dims, group_id))
        tb_group = Table(group)
        # Create the unique table short name
        dims_values = list(dims_id.values())
        tb_group.metadata.short_name = underscore(" - ".join(dims_values))[0:240]
        variables = tb_group.columns.drop(dims + ["country", "year"])
        for variable_name in variables:
            tb_group[variable_name] = Variable(tb_group[variable_name])
            # Create all the necessary metadata
            cleaned_variable = create_variable_metadata(variable=tb_group[variable_name], **dims_id)
            tb_group[cleaned_variable.name] = cleaned_variable
            tb_group = tb_group.drop(columns=variable_name)
            # dropping columns that are totally empty - not all combinations of variables exist or have been downloaded
        tb_group = tb_group.dropna(axis=1, how="all")
        # Dropping dims as table name contains them
        tb_group = tb_group.drop(columns=dims)
        tb_group = tb_group.set_index(["country", "year"], verify_integrity=True)
        ds_garden.add(tb_group)
    return ds_garden
