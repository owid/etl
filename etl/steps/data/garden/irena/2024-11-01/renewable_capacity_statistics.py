"""Create a dataset of renewable electricity capacity using IRENA's Renewable Electricity Capacity and Generation.

"""
from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select and rename columns.
# NOTE: IRENA includes non-renewable technologies and heat indicators, but for now we will only consider renewable electricity.
COLUMNS = {
    "country": "country",
    "year": "year",
    "group_technology": "group_technology",
    "technology": "technology",
    "sub_technology": "sub_technology",
    "producer_type": "producer_type",
    "electricity_installed_capacity__mw": "capacity",
}

# Regions for which aggregates will be created.
REGIONS = [
    "North America",
    "South America",
    "Europe",
    # European Union (27) is already included in the original data.
    # "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


# Technology mapping.
# The following mappin decides how to group technologies, and which ones to consider.
# The structure is as follows:
#   Producer type (on or off-grid) - group technology - technology - subtechnology: simplified technology name
# To ignore a certain group, set it to None.
# All new groupings will be summed together (but Producer type will still be a separate field).
# NOTE: IRENA includes non-renewable technologies, but we will only consider renewable ones.
TECHNOLOGY_MAPPING = {
    "Off-grid - Bioenergy - Biogas - Biogas n.e.s.": "Biogas",
    "Off-grid - Bioenergy - Biogas - Biogases from thermal processes": "Biogas",
    "Off-grid - Bioenergy - Biogas - Landfill gas": "Biogas",
    "Off-grid - Bioenergy - Biogas - Other biogases from anaerobic fermentation": "Biogas",
    "On-grid - Bioenergy - Biogas - Biogas n.e.s.": "Biogas",
    "On-grid - Bioenergy - Biogas - Biogases from thermal processes": "Biogas",
    "On-grid - Bioenergy - Biogas - Landfill gas": "Biogas",
    "On-grid - Bioenergy - Biogas - Other biogases from anaerobic fermentation": "Biogas",
    "On-grid - Bioenergy - Biogas - Sewage sludge gas": "Biogas",
    "Off-grid - Bioenergy - Liquid biofuels - Other liquid biofuels": "Liquid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Animal waste": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Bagasse": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Black liquor": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Energy crops": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Other primary solid biofuels n.e.s.": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Other vegetal and agricultural waste": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Rice husks": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Wood fuel": "Solid biofuels",
    "Off-grid - Bioenergy - Solid biofuels - Wood waste": "Solid biofuels",
    "Off-grid - Fossil fuels - Coal and peat - Coal and peat": "Coal and peat",
    "Off-grid - Fossil fuels - Fossil fuels n.e.s. - Fossil fuels n.e.s.": "Other fossil fuels",
    "Off-grid - Fossil fuels - Natural gas - Natural gas": "Natural gas",
    "Off-grid - Fossil fuels - Oil - Oil": "Oil",
    "Off-grid - Geothermal energy - Geothermal energy - Geothermal energy": "Geothermal",
    "Off-grid - Hydropower (excl. Pumped Storage) - Renewable hydropower - Renewable hydropower": "Hydropower",
    "Off-grid - Solar energy - Solar photovoltaic - Off-grid Solar photovoltaic": "Solar photovoltaic",
    "Off-grid - Wind energy - Onshore wind energy - Onshore wind energy": "Onshore wind",
    "On-grid - Bioenergy - Liquid biofuels - Advanced biodiesel": "Liquid biofuels",
    "On-grid - Bioenergy - Liquid biofuels - Advanced biogasoline": "Liquid biofuels",
    "On-grid - Bioenergy - Liquid biofuels - Conventional biodiesel": "Liquid biofuels",
    "On-grid - Bioenergy - Liquid biofuels - Other liquid biofuels": "Liquid biofuels",
    "On-grid - Bioenergy - Renewable municipal waste - Renewable municipal waste": "Renewable municipal waste",
    "On-grid - Bioenergy - Solid biofuels - Animal waste": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Bagasse": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Biomass pellets and briquettes": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Black liquor": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Energy crops": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Other primary solid biofuels n.e.s.": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Other vegetal and agricultural waste": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Renewable industrial waste": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Rice husks": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Straw": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Wood fuel": "Solid biofuels",
    "On-grid - Bioenergy - Solid biofuels - Wood waste": "Solid biofuels",
    "On-grid - Fossil fuels - Coal and peat - Coal and peat": "Coal and peat",
    "On-grid - Fossil fuels - Fossil fuels n.e.s. - Fossil fuels n.e.s.": "Other fossil fuels",
    "On-grid - Fossil fuels - Natural gas - Natural gas": "Natural gas",
    "On-grid - Fossil fuels - Oil - Oil": "Oil",
    "On-grid - Geothermal energy - Geothermal energy - Geothermal energy": "Geothermal",
    "On-grid - Hydropower (excl. Pumped Storage) - Mixed Hydro Plants - Mixed Hydro Plants": "Mixed hydro plants",
    "On-grid - Hydropower (excl. Pumped Storage) - Renewable hydropower - Renewable hydropower": "Hydropower",
    "On-grid - Marine energy - Marine energy - Marine energy": "Marine",
    "On-grid - Nuclear - Nuclear - Nuclear": "Nuclear",
    "On-grid - Other non-renewable energy - Other non-renewable energy - Other non-renewable energy": "Other non-renewable",
    "On-grid - Pumped storage - Pumped storage - Pumped storage": "Pumped storage",
    "On-grid - Solar energy - Solar photovoltaic - On-grid Solar photovoltaic": "Solar photovoltaic",
    "On-grid - Solar energy - Solar thermal energy - Concentrated solar power": "Solar thermal",
    "On-grid - Wind energy - Offshore wind energy - Offshore wind energy": "Offshore wind",
    "On-grid - Wind energy - Onshore wind energy - Onshore wind energy": "Onshore wind",
}
# _technologies = ["RE or Non-RE", "Group Technology", "Technology", "Producer Type", "Sub-Technology"]
# {" - ".join(row): row.iloc[-2] for _, row in df_country.sort_values(_technologies)[_technologies].drop_duplicates().iterrows()}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow and read its main table.
    ds_meadow = paths.load_dataset("renewable_capacity_statistics")
    tb = ds_meadow.read_table("renewable_capacity_statistics")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Drop empty rows.
    tb = tb.dropna(subset="capacity").reset_index(drop=True)

    # TODO: It's unclear if global data is the combination of off and on-grid data.
    #  Check, e.g. for solar.

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # TODO: Select, map and simplify technologies.

    # Reshape dataframe to have each technology as a separate column
    tb = tb.pivot(index=["country", "year"], columns=["technology"], values="capacity", join_column_levels_with="")

    # For convenience, remove parentheses from column names.
    tb = tb.rename(columns={column: column.replace("(", "").replace(")", "") for column in tb.columns}, errors="raise")

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups, min_num_values_per_year=1
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
