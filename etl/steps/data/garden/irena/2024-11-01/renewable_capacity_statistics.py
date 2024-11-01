"""Create a dataset of renewable electricity capacity using IRENA's Renewable Electricity Capacity and Generation.

"""
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
IS_RENEWABLE_MAPPING = {
    "Total Non-Renewable": "No",
    "Total Renewable": "Yes",
}
PRODUCER_TYPE_MAPPING = {
    "Off-grid electricity": "Off-grid",
    "On-grid electricity": "On-grid",
}
GROUP_TECHNOLOGY_MAPPING = {
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
}
TECHNOLOGY_MAPPING = {
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
    "Solar thermal energy": "Solar thermal energy",
    "Offshore wind energy": "Offshore wind",
}

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

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

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
    tb_global = tb[(tb["country"] == "World")][["group_technology", "year", "capacity"]].reset_index(drop=True)
    check = tb_global.merge(_tb_global, on=["group_technology", "year"], suffixes=("", "_sum"), validate="1:1")
    error = "Adding up on-grid capacity for all countries does not add up to global data."
    assert check[(100 * abs(check["capacity_sum"] - check["capacity"]) / check["capacity"]) > 6].empty, error
    # Drop global and regional data (they will be recalculated afterwards consistently).
    tb = tb[~tb["country"].isin(aggregates)].reset_index(drop=True)

    # Check that the only index columns strictly required are producer type and subtechnology.
    error = "Expected columns producer type and subtechnology (together with country-year) to be a unique index."
    assert len(
        tb[["is_renewable", "group_technology", "technology", "sub_technology", "producer_type"]].drop_duplicates()
    ) == len(tb[["producer_type", "sub_technology"]].drop_duplicates()), error

    # Store the number of unique categories and unique combinations (up to the technology level) before mapping.
    n_categories = {
        category: len(set(tb[category]))
        for category in ["is_renewable", "group_technology", "technology", "sub_technology", "producer_type"]
    }
    n_combinations = len(set(tb[["is_renewable", "group_technology", "technology", "producer_type"]].drop_duplicates()))
    # Rename categories conveniently.
    tb["is_renewable"] = map_series(
        tb["is_renewable"], mapping=IS_RENEWABLE_MAPPING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )
    tb["producer_type"] = map_series(
        tb["producer_type"], mapping=PRODUCER_TYPE_MAPPING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )
    tb["group_technology"] = map_series(
        tb["group_technology"],
        mapping=GROUP_TECHNOLOGY_MAPPING,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb["technology"] = map_series(
        tb["technology"], mapping=TECHNOLOGY_MAPPING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
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
    # FOR DEBUGGING: Print the final mapping.
    # _technologies = ["is_renewable", "producer_type", "group_technology", "technology", "sub_technology"]
    # for _, row in tb.sort_values(_technologies)[_technologies].drop_duplicates().iterrows():
    #     print(f"{row['is_renewable']:<3}|{row['producer_type']:<8}|{row['group_technology']:<20}|{row['technology']:<20}|{row['sub_technology'][:25]:<25} -> {row['producer_type']:<8}|{row['technology']:<20}")

    # Group by producer type and technology (therefore dropping subtechnology level).
    tb = tb.groupby(["country", "year", "producer_type", "technology"], observed=True, as_index=False).agg(
        {"capacity": "sum"}
    )

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb,
        index_columns=["country", "year", "producer_type", "technology"],
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

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

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
