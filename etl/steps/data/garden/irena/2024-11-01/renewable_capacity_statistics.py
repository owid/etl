"""Create a dataset of renewable electricity capacity using IRENA's Renewable Electricity Capacity and Generation.

We will map the input data as follows (to generate the following mapping, uncomment the DEBUGGING section below):

[Old categories] -> [New categories]
Renewable or not | Producer type | Group technology    | Technology            | Sub-technology           -> Producer type | Technology

No |Off-grid|Fossil fuels        |Coal and peat       |Coal and peat             -> Off-grid|Coal and peat
No |Off-grid|Fossil fuels        |Other fossil fuels  |Fossil fuels n.e.s.       -> Off-grid|Other fossil fuels
No |Off-grid|Fossil fuels        |Natural gas         |Natural gas               -> Off-grid|Natural gas
No |Off-grid|Fossil fuels        |Oil                 |Oil                       -> Off-grid|Oil
No |On-grid |Fossil fuels        |Coal and peat       |Coal and peat             -> On-grid |Coal and peat
No |On-grid |Fossil fuels        |Other fossil fuels  |Fossil fuels n.e.s.       -> On-grid |Other fossil fuels
No |On-grid |Fossil fuels        |Natural gas         |Natural gas               -> On-grid |Natural gas
No |On-grid |Fossil fuels        |Oil                 |Oil                       -> On-grid |Oil
No |On-grid |Nuclear             |Nuclear             |Nuclear                   -> On-grid |Nuclear
No |On-grid |Other non-renewable |Other non-renewable |Other non-renewable energ -> On-grid |Other non-renewable
No |On-grid |Pumped storage      |Pumped storage      |Pumped storage            -> On-grid |Pumped storage
Yes|Off-grid|Bioenergy           |Biogas              |Biogas n.e.s.             -> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Biogas              |Biogases from thermal pro -> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Biogas              |Landfill gas              -> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Biogas              |Other biogases from anaer -> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Liquid biofuels     |Other liquid biofuels     -> Off-grid|Liquid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Animal waste              -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Bagasse                   -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Black liquor              -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Energy crops              -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Other primary solid biofu -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Other vegetal and agricul -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Rice husks                -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Wood fuel                 -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Wood waste                -> Off-grid|Solid biofuels
Yes|Off-grid|Geothermal          |Geothermal          |Geothermal energy         -> Off-grid|Geothermal
Yes|Off-grid|Hydropower          |Hydropower          |Renewable hydropower      -> Off-grid|Hydropower
Yes|Off-grid|Solar               |Solar photovoltaic  |Off-grid Solar photovolta -> Off-grid|Solar photovoltaic
Yes|Off-grid|Wind                |Onshore wind        |Onshore wind energy       -> Off-grid|Onshore wind
Yes|On-grid |Bioenergy           |Biogas              |Biogas n.e.s.             -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Biogas              |Biogases from thermal pro -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Biogas              |Landfill gas              -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Biogas              |Other biogases from anaer -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Biogas              |Sewage sludge gas         -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Liquid biofuels     |Advanced biodiesel        -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Liquid biofuels     |Advanced biogasoline      -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Liquid biofuels     |Conventional biodiesel    -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Liquid biofuels     |Other liquid biofuels     -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Renewable municipal waste|Renewable municipal waste -> On-grid |Renewable municipal waste
Yes|On-grid |Bioenergy           |Solid biofuels      |Animal waste              -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Bagasse                   -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Biomass pellets and briqu -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Black liquor              -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Energy crops              -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Other primary solid biofu -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Other vegetal and agricul -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Renewable industrial wast -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Rice husks                -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Straw                     -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Wood fuel                 -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Wood waste                -> On-grid |Solid biofuels
Yes|On-grid |Geothermal          |Geothermal          |Geothermal energy         -> On-grid |Geothermal
Yes|On-grid |Hydropower          |Mixed hydro plants  |Mixed Hydro Plants        -> On-grid |Mixed hydro plants
Yes|On-grid |Hydropower          |Hydropower          |Renewable hydropower      -> On-grid |Hydropower
Yes|On-grid |Marine              |Marine              |Marine energy             -> On-grid |Marine
Yes|On-grid |Solar               |Solar photovoltaic  |On-grid Solar photovoltai -> On-grid |Solar photovoltaic
Yes|On-grid |Solar               |Solar thermal       |Concentrated solar power  -> On-grid |Concentrated solar power
Yes|On-grid |Wind                |Offshore wind       |Offshore wind energy      -> On-grid |Offshore wind
Yes|On-grid |Wind                |Onshore wind        |Onshore wind energy       -> On-grid |Onshore wind

"""
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select and rename columns.
# NOTE: IRENA includes non-renewable technologies and heat indicators, but for now we will only consider renewable electricity.
COLUMNS = {
    "country": "country",
    "year": "year",
    "re_or_non_re": "is_renewable",
    "group_technology": "group_technology",
    "technology": "technology",
    "sub_technology": "sub_technology",
    "producer_type": "producer_type",
    "electricity_installed_capacity__mw": "capacity",
}

# Mapping of different categories.
CATEGORY_MAPPING = {
    "is_renewable": {
        "Total Non-Renewable": "No",
        "Total Renewable": "Yes",
    },
    "producer_type": {
        "Off-grid electricity": "Off-grid",
        "On-grid electricity": "On-grid",
    },
    "group_technology": {
        "Fossil fuels": "Fossil fuels",
        "Nuclear": "Nuclear",
        "Other non-renewable energy": "Other non-renewable",
        "Pumped storage": "Pumped storage",
        "Bioenergy": "Bioenergy",
        "Geothermal energy": "Geothermal",
        "Hydropower (excl. Pumped Storage)": "Hydropower",
        "Solar energy": "Solar",
        "Wind energy": "Wind",
        "Marine energy": "Marine",
    },
    "technology": {
        "Coal and peat": "Coal and peat",
        "Fossil fuels n.e.s.": "Other fossil fuels",
        "Natural gas": "Natural gas",
        "Oil": "Oil",
        "Nuclear": "Nuclear",
        "Other non-renewable energy": "Other non-renewable",
        "Pumped storage": "Pumped storage",
        "Biogas": "Biogas",
        "Liquid biofuels": "Liquid biofuels",
        "Solid biofuels": "Solid biofuels",
        "Geothermal energy": "Geothermal",
        "Renewable hydropower": "Hydropower",
        "Solar photovoltaic": "Solar photovoltaic",
        "Onshore wind energy": "Onshore wind",
        "Renewable municipal waste": "Renewable municipal waste",
        "Mixed Hydro Plants": "Mixed hydro plants",
        "Marine energy": "Marine",
        "Solar thermal energy": "Concentrated solar power",
        "Offshore wind energy": "Offshore wind",
    },
    # NOTE: Sub-technologies will not be stored (we will keep data aggregated at the technology level).
    #  However, we keep this mapping just to be warned in case the data changes in a future update.
    "sub_technology": {
        "Onshore wind energy": "Onshore wind energy",
        "Straw": "Straw",
        "Pumped storage": "Pumped storage",
        "Advanced biodiesel": "Advanced biodiesel",
        "Oil": "Oil",
        "Energy crops": "Energy crops",
        "Rice husks": "Rice husks",
        "Renewable industrial waste": "Renewable industrial waste",
        "Coal and peat": "Coal and peat",
        "Renewable hydropower": "Renewable hydropower",
        "Advanced biogasoline": "Advanced biogasoline",
        "Natural gas": "Natural gas",
        "On-grid Solar photovoltaic": "On-grid Solar photovoltaic",
        "Biogas n.e.s.": "Biogas n.e.s.",
        "Sewage sludge gas": "Sewage sludge gas",
        "Bagasse": "Bagasse",
        "Offshore wind energy": "Offshore wind energy",
        "Biogases from thermal processes": "Biogases from thermal processes",
        "Other biogases from anaerobic fermentation": "Other biogases from anaerobic fermentation",
        "Renewable municipal waste": "Renewable municipal waste",
        "Biomass pellets and briquettes": "Biomass pellets and briquettes",
        "Marine energy": "Marine energy",
        "Nuclear": "Nuclear",
        "Geothermal energy": "Geothermal energy",
        "Black liquor": "Black liquor",
        "Fossil fuels n.e.s.": "Fossil fuels n.e.s.",
        "Other liquid biofuels": "Other liquid biofuels",
        "Conventional biodiesel": "Conventional biodiesel",
        "Off-grid Solar photovoltaic": "Off-grid Solar photovoltaic",
        "Other vegetal and agricultural waste": "Other vegetal and agricultural waste",
        "Animal waste": "Animal waste",
        "Concentrated solar power": "Concentrated solar power",
        "Mixed Hydro Plants": "Mixed Hydro Plants",
        "Other primary solid biofuels n.e.s.": "Other primary solid biofuels n.e.s.",
        "Landfill gas": "Landfill gas",
        "Wood waste": "Wood waste",
        "Other non-renewable energy": "Other non-renewable energy",
        "Wood fuel": "Wood fuel",
    },
}
# Create new groups for total capacity of each technology.
# NOTE: The following groups will include both on-grid and off-grid. The new producer type will be "Both".
NEW_GROUPS = {
    "Fossil fuels (total)": ["Coal and peat", "Other fossil fuels", "Natural gas", "Oil"],
    "Bioenergy (total)": ["Biogas", "Liquid biofuels", "Solid biofuels", "Renewable municipal waste"],
    # In IRENA's Renewable Capacity Statistics's PDF, they show:
    #   * "Renewable hydropower (including mixed plants)" which includes Hydropower + Mixed hydro plants.
    #   * "Hydropower" which includes Hydropower + Mixed hydro plants + Pumped storage.
    #   * "Total renewable energy" which includes all renewables, but excludes Pumped storage.
    # So, for consistency with them, we will create a hydropower total group, which includes pumped storage, and another that doesn't.
    # And, when constructing the total of renewables, pumped storage will not be included.
    # Also note that other totals seem to include off-grid capacity.
    # For example, "Solar" in the PDF is the sum of on- and off-grid "Solar photovoltaic" and "Concentrated solar power".
    "Hydropower (total)": ["Hydropower", "Mixed hydro plants", "Pumped storage"],
    "Hydropower (total, excl. pumped storage)": ["Hydropower", "Mixed hydro plants"],
    "Solar (total)": ["Solar photovoltaic", "Concentrated solar power"],
    "Wind (total)": ["Onshore wind", "Offshore wind"],
    "Renewables (total)": [
        "Bioenergy (total)",
        "Geothermal",
        "Hydropower (total, excl. pumped storage)",
        "Solar (total)",
        "Wind (total)",
        "Marine",
    ],
    "Geothermal (total)": ["Geothermal"],
    # Other groups that could be created, but, since they have only one item (for one producer type), they are unnecessary, and create redundancy.
    # "Nuclear": ["Nuclear"],
    # "Other non-renewable": ["Other non-renewable"],
    # "Pumped storage": ["Pumped storage"],
    # "Marine": ["Marine"],
}

# We will keep only technologies that appear explicitly in the Renewable Capacity Statistics 2024 document.
# So we will exclude the rest.
# NOTE: We do this after creating all aggregates, in case in the future we decide to include them.
EXCLUDE_TECHNOLOGIES = [
    "Fossil fuels (total)",
    "Coal and peat",
    "Other fossil fuels",
    "Natural gas",
    "Oil",
    "Nuclear",
    "Other non-renewable",
]

# Regions for which aggregates will be created.
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def remove_original_regional_and_global_data(tb: Table, tb_global: Table) -> Table:
    # The spreadsheet doesn't explicitly say whether global data corresponds to off-grid, on-grid, or both.
    # After inspection, it seems to be only on-grid.
    # Check that adding up the capacity of all on-grid technologies, sub-technologies and countries reproduces global data
    # (within a certain percentage error).
    aggregates = ["World"] + [region for region in set(tb["country"]) if "(IRENA)" in region]
    _tb_global = (
        tb[(tb["producer_type"] == "On-grid electricity") & (~tb["country"].isin(aggregates))]
        .groupby(["group_technology", "year"], observed=True, as_index=False)
        .agg({"capacity": "sum"})
    )
    check = tb_global.merge(_tb_global, on=["group_technology", "year"], suffixes=("", "_sum"), validate="1:1")
    error = "Adding up on-grid capacity for all countries does not add up to global data."
    assert check[(100 * abs(check["capacity_sum"] - check["capacity"]) / check["capacity"]) > 6].empty, error

    # Drop global and regional data (they will be recalculated afterwards consistently).
    tb = tb.loc[~tb["country"].isin(aggregates)].reset_index(drop=True)

    # Check that the only index columns strictly required are producer type and subtechnology.
    error = "Expected columns producer type and subtechnology (together with country-year) to be a unique index."
    assert len(
        tb[["is_renewable", "group_technology", "technology", "sub_technology", "producer_type"]].drop_duplicates()
    ) == len(tb[["producer_type", "sub_technology"]].drop_duplicates()), error

    return tb


def remap_categories(tb: Table) -> Table:
    # Store the number of unique categories and unique combinations (up to the technology level) before mapping.
    n_categories = {
        category: len(set(tb[category]))
        for category in ["is_renewable", "group_technology", "technology", "sub_technology", "producer_type"]
    }
    n_combinations = len(set(tb[["is_renewable", "group_technology", "technology", "producer_type"]].drop_duplicates()))
    # Rename categories conveniently.
    for category in CATEGORY_MAPPING:
        tb[category] = map_series(
            tb[category],
            mapping=CATEGORY_MAPPING[category],
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=True,
            show_full_warning=True,
        )
    # Check that the number of unique categories and unique combinations (up to the technology level) are the same as before mapping.
    error = "Unexpected number of unique categories after mapping."
    assert {
        category: len(set(tb[category]))
        for category in ["is_renewable", "group_technology", "technology", "sub_technology", "producer_type"]
    } == n_categories, error
    assert (
        len(set(tb[["is_renewable", "group_technology", "technology", "producer_type"]].drop_duplicates()))
        == n_combinations
    ), error

    # We will group at the technology level.
    # DEBUGGING: Print the final mapping.
    # _technologies = ["is_renewable", "producer_type", "group_technology", "technology", "sub_technology"]
    # for _, row in tb.sort_values(_technologies)[_technologies].drop_duplicates().iterrows():
    #     print(f"{row['is_renewable']:<3}|{row['producer_type']:<8}|{row['group_technology']:<20}|{row['technology']:<20}|{row['sub_technology'][:25]:<25} -> {row['producer_type']:<8}|{row['technology']:<20}")

    # Group by producer type and technology (therefore dropping subtechnology level).
    tb = tb.groupby(["country", "year", "producer_type", "technology"], observed=True, as_index=False).agg(
        {"capacity": "sum"}
    )

    return tb


def sanity_check_outputs(tb: Table, tb_global: Table) -> None:
    # Just for peace of mind, check again that the resulting global data (for on-grid technologies) matches (within a small error) with the original global data.
    _tb_global = (
        tb[(tb["producer_type"] == "On-grid") & (tb["country"] == "World")]
        .groupby(["year"], observed=True, as_index=False)
        .agg({"capacity": "sum"})
    )
    check = (
        tb_global.groupby("year", observed=True, as_index=False)
        .agg({"capacity": "sum"})
        .merge(_tb_global, on="year", suffixes=("", "_sum"), validate="1:1")
    )
    error = "Adding up on-grid capacity for all countries does not add up to global data."
    assert check[(100 * abs(check["capacity_sum"] - check["capacity"]) / check["capacity"]) > 1].empty, error

    # Check that there are no missing values or negative values.
    error = "Unexpected missing values."
    assert tb.notnull().all().all(), error
    error = "Unexpected negative values."
    assert (tb["capacity"] >= 0).all(), error


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow and read its main table.
    ds_meadow = paths.load_dataset("renewable_capacity_statistics")
    tb = ds_meadow.read("renewable_capacity_statistics")

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

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Get original global data (used for sanity checks).
    tb_global = tb[(tb["country"] == "World")][["group_technology", "year", "capacity"]].reset_index(drop=True)

    # Remove original regional and global data, and perform some sanity checks.
    tb = remove_original_regional_and_global_data(tb=tb, tb_global=tb_global)  # type: ignore

    # Remap categories.
    tb = remap_categories(tb=tb)

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb,
        index_columns=["country", "year", "producer_type", "technology"],
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Add groups with total capacity (e.g. "Solar (total)").
    for group_name, group_members in NEW_GROUPS.items():
        _tb = (
            tb[(tb["technology"].isin(group_members))]
            .groupby(["country", "year"], observed=True, as_index=False)
            .agg({"capacity": "sum"})
            .assign(**{"technology": group_name, "producer_type": "Both"})
        )
        tb = pr.concat([tb, _tb], ignore_index=True)

    # Sanity check outputs.
    sanity_check_outputs(tb=tb, tb_global=tb_global)  # type: ignore

    # Exclude technologies that are not explicitly mentioned in the IRENA's Renewable Capacity Statistics 2024 document.
    tb = tb[~tb["technology"].isin(EXCLUDE_TECHNOLOGIES)].reset_index(drop=True)

    # Change from long to wide format.
    off_grid_filter = tb["producer_type"] == "Off-grid"
    tb["technology"] = tb["technology"].astype(str)
    tb.loc[off_grid_filter, "technology"] = tb[off_grid_filter]["technology"] + " (off-grid)"
    tb = tb.drop(columns="producer_type").pivot(
        index=["country", "year"], columns="technology", values="capacity", join_column_levels_with="_"
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(keys=["country", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
