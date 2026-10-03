"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("bmj_2022")

    # Read table from meadow dataset.
    tb = ds_meadow["bmj_2022"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = pr.pivot(tb, index=["country", "year"], columns="indicator", values="value")
    tb = tb.underscore().sort_index()

    # Drop mortatlity rate and health expenditure per capita (already on grapher)
    tb = tb.drop(["ihd_mortality_rate_in_2019", "health_expenditure_per_capita_in_2018"], axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
