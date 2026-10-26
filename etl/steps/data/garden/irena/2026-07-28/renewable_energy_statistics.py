"""Create a dataset of renewable electricity capacity using IRENA's Renewable Energy Statistics.

NOTE: The 2026 release replaced IRENA's technology taxonomy (the file's "Tech Mapping" sheet relates the new
product names to the legacy ones). Two things changed for us:

* The granularity of the columns shifted by one level: what the producer now calls "Technology" is the finest
  level (the previous release's "Sub-Technology"), and the new "Sub-Technology" sits where the previous
  "Technology" did. We therefore map the finest level onto the technologies we publish.
* The finest level is now split further for some technologies (e.g. on-grid solar photovoltaic is broken down by
  plant size, and hydropower into reservoir and run-of-river). For a given country-year the producer reports
  either the aggregate or its breakdown, never both, so summing over the finest level is safe. The
  sanity checks below assert this.

We map the input data as follows (to regenerate this mapping, uncomment the DEBUGGING section below):

[Renewable or not | Producer type | Group technology | Sub-technology | Technology] -> [Producer type | Technology]

No |Off-grid|Fossil fuels        |Coal                |Coal                      -> Off-grid|Coal and peat
No |Off-grid|Fossil fuels        |Natural gas         |Natural gas               -> Off-grid|Natural gas
No |Off-grid|Fossil fuels        |Oil                 |Oil                       -> Off-grid|Oil
No |Off-grid|Fossil fuels        |Other fossil fuels  |Other fossil fuels n.e.s. -> Off-grid|Other fossil fuels
No |On-grid |Energy storage      |Mechanical storage  |Pumped hydro              -> On-grid |Pumped storage
No |On-grid |Fossil fuels        |Coal                |Coal                      -> On-grid |Coal and peat
No |On-grid |Fossil fuels        |Natural gas         |Natural gas               -> On-grid |Natural gas
No |On-grid |Fossil fuels        |Oil                 |Oil                       -> On-grid |Oil
No |On-grid |Fossil fuels        |Other fossil fuels  |Other fossil fuels n.e.s. -> On-grid |Other fossil fuels
No |On-grid |Non-renewable energy|Non-renewable waste |Non-renewable industrial w-> On-grid |Other non-renewable
No |On-grid |Non-renewable energy|Non-renewable waste |Non-renewable municipal wa-> On-grid |Other non-renewable
No |On-grid |Non-renewable energy|Nuclear energy      |Nuclear energy            -> On-grid |Nuclear
No |On-grid |Non-renewable energy|Other non-renewable |Other non-renewable energ -> On-grid |Other non-renewable
Yes|Off-grid|Bioenergy           |Gas biofuels        |Biogas from thermal proces-> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Gas biofuels        |Landfill biogas           -> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Gas biofuels        |Other biogas from anaerobi-> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Gas biofuels        |Other gas biofuels product-> Off-grid|Biogas
Yes|Off-grid|Bioenergy           |Liquid biofuels     |Other biodiesels          -> Off-grid|Liquid biofuels
Yes|Off-grid|Bioenergy           |Liquid biofuels     |Other liquid biofuels     -> Off-grid|Liquid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Animal residues           -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Bagasse                   -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Black liquor              -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Energy crops              -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Other biomass processing r-> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Other solid biofuels      -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Plant husks               -> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Solid wood processing resi-> Off-grid|Solid biofuels
Yes|Off-grid|Bioenergy           |Solid biofuels      |Wood fuel                 -> Off-grid|Solid biofuels
Yes|Off-grid|Geothermal          |Geothermal energy   |Geothermal energy         -> Off-grid|Geothermal
Yes|Off-grid|Hydropower          |Renewable hydropower|Pure hydropower           -> Off-grid|Hydropower
Yes|Off-grid|Solar               |Solar photovoltaic  |Off-grid Solar photovoltai-> Off-grid|Solar photovoltaic
Yes|Off-grid|Wind                |Onshore wind energy |Onshore wind energy       -> Off-grid|Onshore wind
Yes|On-grid |Bioenergy           |Gas biofuels        |Biogas from thermal proces-> On-grid |Biogas
Yes|On-grid |Bioenergy           |Gas biofuels        |Landfill biogas           -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Gas biofuels        |Other biogas from anaerobi-> On-grid |Biogas
Yes|On-grid |Bioenergy           |Gas biofuels        |Other gas biofuels product-> On-grid |Biogas
Yes|On-grid |Bioenergy           |Gas biofuels        |Wastewater sludge biogas  -> On-grid |Biogas
Yes|On-grid |Bioenergy           |Liquid biofuels     |Other biodiesels          -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Liquid biofuels     |Other biogasolines        -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Liquid biofuels     |Other liquid biofuels     -> On-grid |Liquid biofuels
Yes|On-grid |Bioenergy           |Renewable waste     |Renewable industrial waste-> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Renewable waste     |Renewable municipal waste -> On-grid |Renewable municipal waste
Yes|On-grid |Bioenergy           |Solid biofuels      |Animal residues           -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Bagasse                   -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Biomass pellets and brique-> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Black liquor              -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Energy crops              -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Other biomass processing r-> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Other solid biofuels      -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Plant husks               -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Solid wood processing resi-> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Straw                     -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Wood fuel                 -> On-grid |Solid biofuels
Yes|On-grid |Bioenergy           |Solid biofuels      |Wood pellets and briquette-> On-grid |Solid biofuels
Yes|On-grid |Geothermal          |Geothermal energy   |Geothermal energy         -> On-grid |Geothermal
Yes|On-grid |Marine              |Marine energy       |Marine energy             -> On-grid |Marine
Yes|On-grid |Marine              |Marine energy       |Thermal gradient          -> On-grid |Marine
Yes|On-grid |Hydropower          |Mixed hydropower    |Mixed hydropower          -> On-grid |Mixed hydro plants
Yes|On-grid |Hydropower          |Pure hydropower     |Reservoir hydropower      -> On-grid |Hydropower
Yes|On-grid |Hydropower          |Pure hydropower     |Run-of-river hydropower   -> On-grid |Hydropower
Yes|On-grid |Hydropower          |Renewable hydropower|Pure hydropower           -> On-grid |Hydropower
Yes|On-grid |Solar               |Solar photovoltaic  |On-grid distributed photov-> On-grid |Solar photovoltaic
Yes|On-grid |Solar               |Solar photovoltaic  |On-grid utility-scale phot-> On-grid |Solar photovoltaic
Yes|On-grid |Solar               |Solar photovoltaic  |Other on-grid solar photov-> On-grid |Solar photovoltaic
Yes|On-grid |Solar               |Solar thermal energy|Concentrated solar power  -> On-grid |Concentrated solar power
Yes|On-grid |Wind                |Offshore wind energy|Offshore wind energy      -> On-grid |Offshore wind
Yes|On-grid |Wind                |Onshore wind energy |Onshore wind energy       -> On-grid |Onshore wind

"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

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
    # NOTE: Group technologies are used only for the sanity checks that compare country-level data with the
    #  producer's own regional and global aggregates, which are given at this level.
    "group_technology": {
        "Fossil fuels": "Fossil fuels",
        "Non-renewable energy": "Non-renewable energy",
        "Energy storage": "Energy storage",
        "Bioenergy": "Bioenergy",
        "Geothermal energy": "Geothermal",
        "Renewable hydropower": "Hydropower",
        "Solar energy": "Solar",
        "Wind energy": "Wind",
        "Marine energy": "Marine",
    },
    # The producer's finest level, which is what we aggregate our published technologies from.
    "technology": {
        "Coal": "Coal and peat",
        "Natural gas": "Natural gas",
        "Oil": "Oil",
        "Other fossil fuels n.e.s.": "Other fossil fuels",
        "Nuclear energy": "Nuclear",
        "Non-renewable industrial waste": "Other non-renewable",
        "Non-renewable municipal waste": "Other non-renewable",
        "Other non-renewable energy": "Other non-renewable",
        "Other non-renewable energy n.e.s.": "Other non-renewable",
        "Pumped hydro": "Pumped storage",
        "Biogas from thermal processes": "Biogas",
        "Landfill biogas": "Biogas",
        "Other biogas from anaerobic digestion": "Biogas",
        "Other gas biofuels products": "Biogas",
        "Wastewater sludge biogas": "Biogas",
        "Other biodiesels": "Liquid biofuels",
        "Other biogasolines": "Liquid biofuels",
        "Other liquid biofuels": "Liquid biofuels",
        "Animal residues": "Solid biofuels",
        "Bagasse": "Solid biofuels",
        "Biomass pellets and briquettes": "Solid biofuels",
        "Black liquor": "Solid biofuels",
        "Energy crops": "Solid biofuels",
        "Other biomass processing residues": "Solid biofuels",
        "Other solid biofuels": "Solid biofuels",
        "Plant husks": "Solid biofuels",
        "Solid wood processing residues": "Solid biofuels",
        "Straw": "Solid biofuels",
        "Wood fuel": "Solid biofuels",
        "Wood pellets and briquettes": "Solid biofuels",
        # NOTE: In the legacy taxonomy, renewable industrial waste was classified under primary solid biofuels
        #  (whereas the new taxonomy groups it with renewable municipal waste, under renewable waste). We keep it
        #  with solid biofuels, so that our published series stays consistent with previous versions.
        "Renewable industrial waste": "Solid biofuels",
        "Renewable municipal waste": "Renewable municipal waste",
        "Geothermal energy": "Geothermal",
        # NOTE: "Pure hydropower" is the new name of the legacy "Renewable hydropower" product. Reservoir and
        #  run-of-river hydropower are new breakdowns of it, reported instead of it (never alongside it).
        "Pure hydropower": "Hydropower",
        "Reservoir hydropower": "Hydropower",
        "Run-of-river hydropower": "Hydropower",
        "Mixed hydropower": "Mixed hydro plants",
        "Marine energy": "Marine",
        "Thermal gradient": "Marine",
        "Off-grid Solar photovoltaic": "Solar photovoltaic",
        "On-grid distributed photovoltaic (30-1 000 kW)": "Solar photovoltaic",
        "On-grid distributed photovoltaic (<30 kW)": "Solar photovoltaic",
        "On-grid utility-scale photovoltaic (>1 000 kW)": "Solar photovoltaic",
        "Other on-grid solar photovoltaic n.e.s.": "Solar photovoltaic",
        "Concentrated solar power": "Concentrated solar power",
        "Onshore wind energy": "Onshore wind",
        "Offshore wind energy": "Offshore wind",
    },
    # NOTE: Sub-technologies will not be stored (we keep data aggregated at the technology level).
    #  However, we keep this mapping just to be warned in case the data changes in a future update.
    "sub_technology": {
        "Coal": "Coal",
        "Gas biofuels": "Gas biofuels",
        "Geothermal energy": "Geothermal energy",
        "Liquid biofuels": "Liquid biofuels",
        "Marine energy": "Marine energy",
        "Mechanical storage": "Mechanical storage",
        "Mixed hydropower": "Mixed hydropower",
        "Natural gas": "Natural gas",
        "Non-renewable waste": "Non-renewable waste",
        "Nuclear energy": "Nuclear energy",
        "Offshore wind energy": "Offshore wind energy",
        "Oil": "Oil",
        "Onshore wind energy": "Onshore wind energy",
        "Other fossil fuels": "Other fossil fuels",
        "Other non-renewable energy": "Other non-renewable energy",
        "Pure hydropower": "Pure hydropower",
        "Renewable hydropower": "Renewable hydropower",
        "Renewable waste": "Renewable waste",
        "Solar photovoltaic": "Solar photovoltaic",
        "Solar thermal energy": "Solar thermal energy",
        "Solid biofuels": "Solid biofuels",
    },
}

# Technologies expected after mapping the producer's finest level.
# NOTE: This is the set of technologies we publish (before adding the aggregates defined in NEW_GROUPS, and before
#  excluding the ones listed in EXCLUDE_TECHNOLOGIES). A change here means the producer added or removed a
#  technology, which needs to be looked at before it silently changes our indicators.
EXPECTED_TECHNOLOGIES = {
    "Biogas",
    "Coal and peat",
    "Concentrated solar power",
    "Geothermal",
    "Hydropower",
    "Liquid biofuels",
    "Marine",
    "Mixed hydro plants",
    "Natural gas",
    "Nuclear",
    "Offshore wind",
    "Oil",
    "Onshore wind",
    "Other fossil fuels",
    "Other non-renewable",
    "Pumped storage",
    "Renewable municipal waste",
    "Solar photovoltaic",
    "Solid biofuels",
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


def sanity_check_inputs(tb: Table) -> None:
    # The producer reports either an aggregate product or its breakdown for a given country-year, never both.
    # If that stopped being true, summing over the finest level would double count.
    # NOTE: The hierarchy is given by the product codes in the file's "Tech Mapping" sheet, e.g. reservoir
    #  hydropower (211100) and run-of-river hydropower (211200) are breakdowns of pure hydropower (211000).
    hierarchy = {
        "Pure hydropower": ["Reservoir hydropower", "Run-of-river hydropower"],
        "Marine energy": ["Thermal gradient"],
        "Biomass pellets and briquettes": ["Wood pellets and briquettes"],
    }
    for parent, children in hierarchy.items():
        overlap = tb[tb["technology"].isin([parent] + children)].pivot(
            index=["country", "year", "producer_type"], columns="technology", values="capacity"
        )
        present_children = [child for child in children if child in overlap.columns]
        if (parent not in overlap.columns) or (not present_children):
            continue
        both = overlap[parent].notna() & overlap[present_children].notna().any(axis=1)
        error = f"Product '{parent}' is now reported alongside its breakdown ({present_children}), which would double count it."
        assert not both.any(), error


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

    # Check that the only index columns strictly required are producer type and technology (the finest level).
    error = "Expected columns producer type and technology (together with country-year) to be a unique index."
    assert len(
        tb[["is_renewable", "group_technology", "technology", "sub_technology", "producer_type"]].drop_duplicates()
    ) == len(tb[["producer_type", "technology"]].drop_duplicates()), error

    return tb


def remap_categories(tb: Table) -> Table:
    # Rename categories conveniently.
    for category in CATEGORY_MAPPING:
        tb[category] = map_series(
            tb[category],
            mapping=CATEGORY_MAPPING[category],
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=True,
            show_full_warning=True,
        )

    # The producer's finest level is mapped onto a smaller set of technologies (e.g. all solid biofuel products
    # become "Solid biofuels"), so check that we end up with exactly the technologies we expect to publish.
    error = "Unexpected technologies after mapping."
    assert set(tb["technology"]) == EXPECTED_TECHNOLOGIES, error

    # DEBUGGING: Print the final mapping.
    # _technologies = ["is_renewable", "producer_type", "group_technology", "sub_technology", "technology"]
    # for _, row in tb.sort_values(_technologies)[_technologies].drop_duplicates().iterrows():
    #     print(f"{row['is_renewable']:<3}|{row['producer_type']:<8}|{row['group_technology']:<20}|{row['sub_technology']:<20}|{row['technology'][:26]:<26} -> {row['producer_type']:<8}|{CATEGORY_MAPPING['technology'][row['technology']]:<20}")

    # Group by producer type and technology (therefore dropping the sub-technology level).
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


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow and read its main table.
    ds_meadow = paths.load_dataset("renewable_energy_statistics")
    tb = ds_meadow.read("renewable_energy_statistics")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Drop empty rows.
    tb = tb.dropna(subset="capacity").reset_index(drop=True)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Sanity check inputs.
    sanity_check_inputs(tb=tb)

    # Get original global data (used for sanity checks).
    tb_global = tb[(tb["country"] == "World")][["group_technology", "year", "capacity"]].reset_index(drop=True)

    # Remove original regional and global data, and perform some sanity checks.
    tb = remove_original_regional_and_global_data(tb=tb, tb_global=tb_global)  # ty: ignore

    # Remap categories.
    tb = remap_categories(tb=tb)

    # Add region aggregates.
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        index_columns=["country", "year", "producer_type", "technology"],
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
    sanity_check_outputs(tb=tb, tb_global=tb_global)  # ty: ignore

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
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
