# List of identifiers of steps that should be considered as non-updateable.
# NOTE: The identifier is the step name without the version (and without the "data://").
NON_UPDATEABLE_IDENTIFIERS = [
    # All population-related datasets.
    "garden/demography/population",
    "garden/gapminder/population",
    "garden/hyde/baseline",
    "garden/un/un_wpp",
    "meadow/gapminder/population",
    "meadow/hyde/baseline",
    "meadow/hyde/general_files",
    "meadow/un/un_wpp",
    "open_numbers/open_numbers/gapminder__systema_globalis",
    "open-numbers/ddf--gapminder--systema_globalis",
    "snapshot/hyde/general_files.zip",
    "snapshot/hyde/baseline.zip",
    "snapshot/gapminder/population.xlsx",
    "snapshot/un/un_wpp.zip",
    # Regions dataset.
    "garden/regions/regions",
    # Old WB income groups.
    "garden/wb/wb_income",
    "meadow/wb/wb_income",
    "walden/wb/wb_income",
    # New WB income groups.
    "garden/wb/income_groups",
    "meadow/wb/income_groups",
    "snapshot/wb/income_groups.xlsx",
    # World Bank country shapes.
    "snapshot/countries/world_bank.zip",
    # World Bank WDI.
    "snapshot/worldbank_wdi/wdi.zip",
    "meadow/worldbank_wdi/wdi",
    "garden/worldbank_wdi/wdi",
    # Other steps we don't want to update (because the underlying data does not get updated).
    # TODO: We need a better way to achieve this, for example adding update_period_days to all steps and snapshots.
    #  A simpler alternative would be to move these steps to a separate file in a meaningful place.
    #  Another option is to have "playlists", e.g. "climate_change_explorer" with the identifiers of steps to update.
    "meadow/epa/ocean_heat_content",
    "snapshot/epa/ocean_heat_content_annual_world_700m.csv",
    "snapshot/epa/ocean_heat_content_annual_world_2000m.csv",
    "garden/epa/ocean_heat_content",
    "meadow/epa/ocean_heat_content",
    "meadow/epa/ice_sheet_mass_balance",
    "snapshot/epa/ice_sheet_mass_balance.csv",
    "garden/epa/ice_sheet_mass_balance",
    "meadow/epa/ice_sheet_mass_balance",
    "meadow/epa/ghg_concentration",
    "snapshot/epa/co2_concentration.csv",
    "snapshot/epa/ch4_concentration.csv",
    "snapshot/epa/n2o_concentration.csv",
    "garden/epa/ghg_concentration",
    "meadow/epa/ghg_concentration",
    "meadow/epa/mass_balance_us_glaciers",
    "snapshot/epa/mass_balance_us_glaciers.csv",
    "garden/epa/mass_balance_us_glaciers",
    "meadow/epa/mass_balance_us_glaciers",
    "meadow/climate/antarctic_ice_core_co2_concentration",
    "snapshot/climate/antarctic_ice_core_co2_concentration.xls",
    "garden/climate/antarctic_ice_core_co2_concentration",
    "meadow/climate/antarctic_ice_core_co2_concentration",
    "meadow/climate/global_sea_level",
    "snapshot/climate/global_sea_level.csv",
    "garden/climate/global_sea_level",
    "meadow/climate/global_sea_level",
]
