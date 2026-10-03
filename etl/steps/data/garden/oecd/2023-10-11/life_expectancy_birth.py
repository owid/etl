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
    ds_meadow = paths.load_dataset("life_expectancy_birth")
    # Read table from meadow dataset.
    tb = ds_meadow["life_expectancy_birth"].reset_index()

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "location": "country",
            "time": "year",
            "subject": "sex",
            "value": "life_expectancy_birth",
        }
    )

    # Check values in 'sex', and rename them
    sexes_exp = {"MEN", "WOMEN", "TOT"}
    assert set(tb["sex"]) == sexes_exp, f"Other sexes than {sexes_exp} found!"
    tb["sex"] = tb["sex"].replace(
        {
            "MEN": "male",
            "WOMEN": "female",
            "TOT": "all",
        }
    )

    # Preserve only relevant columns
    tb = tb[["country", "year", "sex", "life_expectancy_birth"]]

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set index
    tb = tb.set_index(["country", "year", "sex"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
