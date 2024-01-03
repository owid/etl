"""Load a garden dataset and create an explorers dataset.

The output csv file will feed our Climate Change Impacts data explorer:
https://ourworldindata.org/explorers/climate-change
"""


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
    ds_ocean_heat = paths.load_dataset("ocean_heat_content")
    tb_ocean_heat_monthly = ds_ocean_heat["ocean_heat_content_monthly"].reset_index()
    tb_ocean_heat_annual = ds_ocean_heat["ocean_heat_content_annual"].reset_index()

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
