"""Create a garden dataset with all climate change impacts data."""

from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_sea_ice_extent(tb_nsidc: Table) -> Table:
    tb_nsidc = tb_nsidc.copy()
    # Create a table with the minimum and maximum Arctic sea ice extent.
    # Assume minimum and maximum occur in September and February every year.
    tb_nsidc["month"] = tb_nsidc["date"].astype(str).str[5:7]
    tb_nsidc["year"] = tb_nsidc["date"].astype(str).str[0:4].astype(int)
    arctic_sea_ice_extent = (
        tb_nsidc[(tb_nsidc["location"] == "Northern Hemisphere") & (tb_nsidc["month"].isin(["02", "09"]))]
        .pivot(index=["location", "year"], columns=["month"], values="sea_ice_extent", join_column_levels_with=" ")
        .rename(columns={"02": "arctic_sea_ice_extent_max", "09": "arctic_sea_ice_extent_min"}, errors="raise")
    )
    # Instead of calling the location a generic "Northern Hemisphere", call it "Arctic Ocean".
    arctic_sea_ice_extent["location"] = "Arctic Ocean"

    # Idem for the Antarctic sea ice extent.
    # Assume maximum and minimum occur in September and February every year.
    antarctic_sea_ice_extent = (
        tb_nsidc[(tb_nsidc["location"] == "Southern Hemisphere") & (tb_nsidc["month"].isin(["02", "09"]))]
        .pivot(index=["location", "year"], columns=["month"], values="sea_ice_extent", join_column_levels_with=" ")
        .rename(columns={"02": "antarctic_sea_ice_extent_min", "09": "antarctic_sea_ice_extent_max"}, errors="raise")
    )
    # Instead of calling the location a generic "Southern Hemisphere", call it "Antarctica".
    antarctic_sea_ice_extent["location"] = "Antarctica"

    return arctic_sea_ice_extent, antarctic_sea_ice_extent


def prepare_ocean_heat_content(tb_ocean_heat_annual: Table, tb_ocean_heat_annual_epa: Table) -> Table:
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
    # Recover the original indicator titles (they are empty because of combining two columns with different titles).
    tb_ocean_heat_annual["ocean_heat_content_noaa_700m"].metadata.title = tb_ocean_heat_annual_epa[
        "ocean_heat_content_noaa_700m"
    ].metadata.title
    tb_ocean_heat_annual["ocean_heat_content_noaa_2000m"].metadata.title = tb_ocean_heat_annual_epa[
        "ocean_heat_content_noaa_2000m"
    ].metadata.title

    return tb_ocean_heat_annual


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load NSIDC dataset of sea ice index.
    ds_nsidc = paths.load_dataset("sea_ice_index")
    tb_nsidc = ds_nsidc.read("sea_ice_index")

    # Load Met Office dataset on sea surface temperature.
    ds_met_office = paths.load_dataset("sea_surface_temperature")
    tb_met_office = ds_met_office.read("sea_surface_temperature")
    tb_met_office_annual = ds_met_office.read("sea_surface_temperature_annual")

    # Load NOAA/NCIE dataset on ocean heat content.
    ds_ocean_heat = paths.load_dataset("ocean_heat_content", namespace="climate")
    tb_ocean_heat_monthly = ds_ocean_heat.read("ocean_heat_content_monthly")
    tb_ocean_heat_annual = ds_ocean_heat.read("ocean_heat_content_annual")

    # Load EPA's compilation of data on ocean heat content.
    ds_epa = paths.load_dataset("ocean_heat_content", namespace="epa")
    tb_ocean_heat_annual_epa = ds_epa.read("ocean_heat_content")

    # Load ocean pH data from the School of Ocean and Earth Science and Technology.
    ds_ocean_ph = paths.load_dataset("ocean_ph_levels")
    tb_ocean_ph = ds_ocean_ph.read("ocean_ph_levels")

    # Load snow cover extent from Rutgers University Global Snow Lab.
    ds_snow = paths.load_dataset("snow_cover_extent")
    tb_snow = ds_snow.read("snow_cover_extent")

    # Load ice sheet mass balance data from EPA.
    ds_ice_sheet = paths.load_dataset("ice_sheet_mass_balance")
    tb_ice_sheet = ds_ice_sheet.read("ice_sheet_mass_balance")

    # Load annual data on mass balance of US glaciers from EPA.
    ds_us_glaciers = paths.load_dataset("mass_balance_us_glaciers")
    tb_us_glaciers = ds_us_glaciers.read("mass_balance_us_glaciers")

    # Load monthly greenhouse gas concentration data from NOAA/GML.
    ds_gml = paths.load_dataset("ghg_concentration")
    tb_gml = ds_gml.read("ghg_concentration")

    # Load long-run yearly greenhouse gas concentration data.
    ds_ghg = paths.load_dataset("long_run_ghg_concentration")
    tb_ghg = ds_ghg.read("long_run_ghg_concentration")

    # Load global sea level.
    ds_sea_level = paths.load_dataset("global_sea_level")
    tb_sea_level = ds_sea_level.read("global_sea_level")

    #
    # Process data.
    #
    # Prepare sea ice extent data.
    arctic_sea_ice_extent, antarctic_sea_ice_extent = prepare_sea_ice_extent(tb_nsidc=tb_nsidc)

    # Prepare ocean heat content data.
    tb_ocean_heat_annual = prepare_ocean_heat_content(
        tb_ocean_heat_annual=tb_ocean_heat_annual, tb_ocean_heat_annual_epa=tb_ocean_heat_annual_epa
    )

    # Gather monthly data from different tables.
    tb_monthly = tb_nsidc.astype({"date": str}).copy()
    # NOTE: The values in tb_ocean_ph are monthly, but the dates are not consistently on the middle of the month.
    #  Instead, they are on different days of the month. When merging with other tables, this will create many nans.
    #  We could reindex linearly, but it's not a big deal.
    for table in [
        tb_met_office,
        tb_ocean_heat_monthly,
        tb_ocean_ph,
        tb_snow,
        tb_ice_sheet,
        tb_gml,
        tb_sea_level,
    ]:
        tb_monthly = tb_monthly.merge(
            table.astype({"date": str}),
            how="outer",
            on=["location", "date"],
            validate="one_to_one",
            short_name="climate_change_impacts_monthly",
        )

    # Gather annual data from different tables.
    tb_annual = tb_ocean_heat_annual.copy()
    for table in [
        tb_met_office_annual,
        arctic_sea_ice_extent,
        antarctic_sea_ice_extent,
        tb_ghg,
        tb_us_glaciers.astype({"year": int}),
    ]:
        tb_annual = tb_annual.merge(
            table,
            how="outer",
            on=["location", "year"],
            validate="one_to_one",
            short_name="climate_change_impacts_annual",
        )
    tb_annual.metadata.short_name = "climate_change_impacts_annual"

    # Set an appropriate index to monthly and annual tables, and sort conveniently.
    tb_monthly = tb_monthly.format(["location", "date"])
    tb_annual = tb_annual.format(["location", "year"])

    #
    # Save outputs.
    #
    # Create explorer dataset with combined table in csv format.
    ds_explorer = create_dataset(dest_dir, tables=[tb_annual, tb_monthly])
    ds_explorer.save()
