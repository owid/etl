"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# In the latest IRENA renewable power generation costs, IRENA included new data for 2024, in constant 2024$.
# Unfortunately, they also dropped data for years prior to 2010, for various indicators, that were given in the previous release (in constant 2023$).
# Therefore, in this step, I combine the old data with the latest release (prioritizing the latest release, where there is overlap).
# To do that, I convert the old data (in constant 2023$) to constant 2024$.
# IRENA costs are given in the latest year's USD, so we convert other costs to the same currency.
LATEST_YEAR = 2024
PREVIOUS_YEAR = LATEST_YEAR - 1


def adjust_old_data_for_inflation(tb_old, tb_deflator, tb):
    # Use the GDP deflator (linked series) to convert from constant USD of PREVIOUS_YEAR to constant USD of LATEST_YEAR.
    tb_deflator = (
        tb_deflator[tb_deflator["year"].isin([PREVIOUS_YEAR, LATEST_YEAR])][["country", "year", "gdp_deflator_linked"]]
        .pivot(index=["country"], columns=["year"], values="gdp_deflator_linked")
        .reset_index()
    )
    tb_deflator = tb_deflator.dropna(how="all", subset=[PREVIOUS_YEAR, LATEST_YEAR]).reset_index(drop=True)
    # Check that we have deflator data for PREVIOUS_YEAR for all countries.
    assert set(tb_old["country"]) - set(tb_deflator[(tb_deflator[PREVIOUS_YEAR].notnull())]["country"]) == {"World"}
    # Only one country will not be able to be adjusted, namely South Korea.
    assert set(tb_old["country"]) - set(tb_deflator[(tb_deflator[LATEST_YEAR].notnull())]["country"]) == {
        "World",
        "South Korea",
    }
    # Calculate the adjustment factor.
    tb_deflator["adjustment"] = tb_deflator[2024] / tb_deflator[2023]

    # Add adjustment column to old table.
    tb_old = tb_old.merge(tb_deflator[["country", "adjustment"]], on=["country"], how="left")

    # Assign US adjustment to the World (for which we don't have an alternative).
    tb_old.loc[tb_old["country"] == "World", "adjustment"] = tb_deflator[(tb_deflator["country"] == "United States")][
        "adjustment"
    ].item()

    # Column by column in the old table, convert to constant USD of the latest year.
    for column in tb_old.drop(columns=["country", "year", "adjustment"]).columns:
        # Sanity check.
        error = f"Unexpected units for column {column}."
        assert tb[column].metadata.unit == f"constant {LATEST_YEAR} US$ per kilowatt-hour", error
        # Fill adjustment factor with 1 for countries for which we can't do the adjustment (namely South Korea).
        tb_old[column] *= tb_old["adjustment"].fillna(1)
        tb_old[column].metadata.unit = tb[column].metadata.unit

    tb_old = tb_old.drop(columns=["adjustment"], errors="raise")

    return tb_old


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

    # Load OWID deflator dataset, and read its main table.
    ds_deflator = paths.load_dataset("owid_deflator")
    tb_deflator = ds_deflator.read("owid_deflator")

    #
    # Process data.
    #
    # Harmonize country names of the old table.
    tb_old = geo.harmonize_countries(df=tb_old, countries_file=paths.country_mapping_path)

    # Harmonize country names of the new table.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)

    # Adjust prices in the old table to be in constant USD of LATEST_YEAR.
    tb_old = adjust_old_data_for_inflation(tb_old=tb_old, tb_deflator=tb_deflator, tb=tb)

    # Combine old and new tables, prioritizing the new where there is overlap.
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_old, index_columns=["country", "year"])

    # Improve table formatting.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_solar_pv], default_metadata=ds_meadow.metadata, yaml_params={"LATEST_YEAR": LATEST_YEAR}
    )
    ds_garden.save()
