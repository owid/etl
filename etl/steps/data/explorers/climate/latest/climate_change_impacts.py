"""Load a garden dataset and create an explorers dataset.

The output csv file will feed our Climate Change Impacts data explorer:
https://ourworldindata.org/explorers/climate-change
"""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load GISS dataset surface temperature analysis, and read monthly data.
    ds_giss = paths.load_dataset("surface_temperature_analysis")
    tb_giss = ds_giss["surface_temperature_analysis"].reset_index()

    # Load NSIDC dataset of sea ice index.
    ds_nsidc = paths.load_dataset("sea_ice_index")
    tb_nsidc = ds_nsidc["sea_ice_index"].reset_index()

    # Load Met Office dataset on sea surface temperature.
    ds_met_office = paths.load_dataset("sea_surface_temperature")
    tb_met_office = ds_met_office["sea_surface_temperature"].reset_index()

    # Load NOAA/NCIE dataset on ocean heat content.
    ds_ocean_heat = paths.load_dataset("ocean_heat_content", namespace="climate")
    tb_ocean_heat_monthly = ds_ocean_heat["ocean_heat_content_monthly"].reset_index()
    tb_ocean_heat_annual = ds_ocean_heat["ocean_heat_content_annual"].reset_index()

    # Load EPA's compilation of data on ocean heat content.
    ds_epa = paths.load_dataset("ocean_heat_content", namespace="epa")
    tb_ocean_heat_annual_epa = ds_epa["ocean_heat_content"].reset_index()

    #
    # Process data.
    #
    # Gather monthly data from different tables.
    tb_monthly = tb_giss.astype({"date": str}).copy()
    for table in [tb_nsidc, tb_met_office, tb_ocean_heat_monthly]:
        tb_monthly = tb_monthly.merge(
            table.astype({"date": str}),
            how="outer",
            on=["location", "date"],
            validate="one_to_one",
            short_name="climate_change_impacts_monthly",
        )

    # Combine NOAA's annual data on ocean heat content (which is more up-to-date) with the analogous EPA's data based on
    # NOAA (which, for some reason, spans a longer time range for 2000m). Prioritize NOAA's data on common years.
    tb_ocean_heat_annual = combine_two_overlapping_dataframes(
        tb_ocean_heat_annual.rename(
            columns={
                "ocean_heat_content_700m": "ocean_heat_content_noaa_700m",
                "ocean_heat_content_2000m": "ocean_heat_content_noaa_2000m",
            },
            errors="raise",
        ),
        tb_ocean_heat_annual_epa,
        index_columns=["location", "year"],
    )

    # Gather annual data from different tables.
    tb_annual = tb_ocean_heat_annual.copy()
    tb_annual.metadata.short_name = "climate_change_impacts_annual"

    # Set an appropriate index to monthly and annual tables, and sort conveniently.
    tb_monthly = tb_monthly.set_index(["location", "date"], verify_integrity=True).sort_index()
    tb_annual = tb_annual.set_index(["location", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create explorer dataset with combined table in csv format.
    ds_explorer = create_dataset(dest_dir, tables=[tb_annual, tb_monthly], formats=["csv"])
    ds_explorer.save()
