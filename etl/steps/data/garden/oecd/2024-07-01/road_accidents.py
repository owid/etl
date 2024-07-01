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
    ds_meadow = paths.load_dataset("road_accidents", channel="meadow", version="2024-07-01")
    ds_old = paths.load_dataset("road_accidents", version="2023-08-11")

    # Read table from meadow dataset.
    tb = ds_meadow["road_accidents"].reset_index()
    tb_old = ds_old["road_accidents"].reset_index()

    tb = tb.pivot_table(index=["country", "year"], columns=["measure"], values="obs_value").reset_index()

    # harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)
    tb_old = geo.harmonize_countries(
        df=tb_old, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    tb = tb.merge(tb_old, how="outer", on=["country", "year"], suffixes=("", "_old"))

    tb = tb.fillna(-1)
    # if one column is -1 use other column, otherwise use new data (in columns Fatalities, Injured, Injury crashes)
    tb["accident_deaths"] = tb.apply(lambda x: x["Fatalities"] if x["Fatalities"] != -1 else x["deaths"], axis=1)
    tb["accident_injuries"] = tb.apply(lambda x: x["Injured"] if x["Injured"] != -1 else x["injuries"], axis=1)
    tb["accidents_with_injuries"] = tb.apply(
        lambda x: x["Injury crashes"] if x["Injury crashes"] != -1 else x["accidents_involving_casualties"], axis=1
    )

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
