"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# In the latest IRENA renewable power generation costs, IRENA included new data for 2024, in constant 2024$.
# Unfortunately, they also dropped data for years prior to 2010, for various indicators, that were given in the previous release (in constant 2023$).
# Therefore, in this step, I combine the old data with the latest release (prioritizing the latest release, where there is overlap).
# To do that, I convert the old data (in constant 2023$) to constant 2024$.
# IRENA costs are given in the latest year's USD, so we convert other costs to the same currency.
LATEST_YEAR = 2024
PREVIOUS_YEAR = LATEST_YEAR - 1
# Convert costs to constant {LATEST_YEAR} USD, using
# https://www.usinflationcalculator.com/
# "If in {PREVIOUS_YEAR} I purchased an item for  $ 1.00 then in {LATEST_YEAR} that same item would cost:"
USDPREVIOUS_TO_USDLATEST = 1.03


def run() -> None:
    #
    # Load inputs.
    #
    # Find out versions available among the dependencies of the current step.
    versions = sorted(
        [step.split("/")[-2] for step in paths.dependencies if step.endswith("renewable_power_generation_costs")]
    )

    # Load latest meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("renewable_power_generation_costs", version=versions[1])
    tb = ds_meadow.read("renewable_power_generation_costs", safe_types=False)
    tb_solar_pv = ds_meadow.read("solar_photovoltaic_module_prices", reset_index=False, safe_types=False)

    # Load previous meadow dataset and read its tables.
    ds_meadow_old = paths.load_dataset("renewable_power_generation_costs", version=versions[0])
    tb_old = ds_meadow_old.read("renewable_power_generation_costs", safe_types=False)
    # NOTE: Solar pv prices in the new release are not missing data with respect to the previous release.

    #
    # Process data.
    #
    # Column by column in the old table, convert to constant USD$ of the latest year.
    for column in tb_old.drop(columns=["country", "year"]).columns:
        # Sanity check, given that this step requires a manual input, (the conversion above).
        assert tb[column].metadata.unit == f"constant {LATEST_YEAR} US$ per kilowatt-hour"
        tb_old[column] *= USDPREVIOUS_TO_USDLATEST
        tb_old[column].metadata.unit = tb[column].metadata.unit

    # Combine old and new tables, prioritizing the new where there is overlap.
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_old, index_columns=["country", "year"])

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table formatting.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_solar_pv], default_metadata=ds_meadow.metadata)
    ds_garden.save()
