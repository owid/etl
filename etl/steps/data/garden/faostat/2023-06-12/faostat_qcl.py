"""FAOSTAT garden step for faostat_qcl dataset."""

from pathlib import Path

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes
from shared import (
    ADDED_TITLE_TO_WIDE_TABLE,
    CURRENT_DIR,
    FLAG_MULTIPLE_FLAGS,
    NAMESPACE,
    REGIONS_TO_ADD,
    add_per_capita_variables,
    add_regions,
    clean_data,
    handle_anomalies,
    harmonize_elements,
    harmonize_items,
    log,
    parse_amendments_table,
    prepare_long_table,
    prepare_wide_table,
)

from etl.helpers import PathFinder, create_dataset

# Item and item code for 'Meat, poultry'.
ITEM_POULTRY = "Meat, poultry"
ITEM_CODE_MEAT_POULTRY = "00001808"
# Item code for 'Meat, chicken'.
ITEM_CODE_MEAT_CHICKEN = "00001058"
# List item codes to sum as part of "Meat, total" (avoiding double-counting items).
MEAT_TOTAL_ITEM_CODES = [
    "00000977",  # 'Meat, lamb and mutton' (previously 'Meat, lamb and mutton')
    "00001035",  # 'Meat of pig with the bone, fresh or chilled' (previously 'Meat, pig')
    "00001097",  # 'Horse meat, fresh or chilled' (previously 'Meat, horse')
    "00001108",  # 'Meat of asses, fresh or chilled' (previously 'Meat, ass')
    "00001111",  # 'Meat of mules, fresh or chilled' (previously 'Meat, mule')
    "00001127",  # 'Meat of camels, fresh or chilled' (previously 'Meat, camel')
    "00001141",  # 'Meat of rabbits and hares, fresh or chilled' (previously 'Meat, rabbit')
    "00001806",  # 'Meat, beef and buffalo' (previously 'Meat, beef and buffalo')
    "00001807",  # 'Meat, sheep and goat' (previously 'Meat, sheep and goat')
    ITEM_CODE_MEAT_POULTRY,  # 'Meat, poultry' (previously 'Meat, poultry')
]

# List of element codes for "Producing or slaughtered animals" (they have different items assigned).
SLAUGHTERED_ANIMALS_ELEMENT_CODES = ["005320", "005321"]
# For the resulting dataframe, we arbitrarily assign the first of those codes.
SLAUGHTERED_ANIMALS_ELEMENT_CODE = SLAUGHTERED_ANIMALS_ELEMENT_CODES[0]
# Item code for 'Meat, total'.
TOTAL_MEAT_ITEM_CODE = "00001765"
# OWID item name for total meat.
TOTAL_MEAT_ITEM = "Meat, total"
# OWID element name, unit name, and unit short name for number of slaughtered animals.
SLAUGHTERED_ANIMALS_ELEMENT = "Producing or slaughtered animals"
SLAUGHTERED_ANIMALS_UNIT = "animals"
SLAUGHTERED_ANIMALS_UNIT_SHORT_NAME = "animals"
# Text to be added to the dataset description (after the description of anomalies).
SLAUGHTERED_ANIMALS_ADDITIONAL_DESCRIPTION = (
    "\n\nFAO does not provide data for the total number of slaughtered animals "
    "to produce meat. We calculate this metric by adding up the number of slaughtered animals of all meat groups. "
    "However, when data for slaughtered poultry (which usually outnumbers other meat groups) is not provided, we do "
    "not calculate the total (to avoid spurious dips in the data)."
)


def fill_slaughtered_poultry_with_slaughtered_chicken(data: pd.DataFrame) -> pd.DataFrame:
    """Fill missing data on slaughtered poultry with slaughtered chicken.

    Most of poultry meat comes from chicken. However, sometimes chicken is informed, but the rest of poultry isn't,
    which causes poultry data to be empty (e.g. Spain in 2018).
    Therefore, we fill missing data for poultry with chicken data.
    """
    data = data.copy()

    # Prepare a slice of the data to extract additional data fields.
    additional_fields = (
        data[(data["item_code"] == ITEM_CODE_MEAT_POULTRY) & (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)][
            ["fao_item", "item_description", "fao_unit_short_name"]
        ]
        .drop_duplicates()
        .iloc[0]
    )

    # Select data for the number of slaughtered chicken.
    chickens_slaughtered = data[
        (data["item_code"] == ITEM_CODE_MEAT_CHICKEN)
        & (data["element"] == SLAUGHTERED_ANIMALS_ELEMENT)
        & (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)
    ]

    # Select data for the number of slaughtered poultry.
    poultry_slaughtered = data[
        (data["item_code"] == ITEM_CODE_MEAT_POULTRY)
        & (data["element"] == SLAUGHTERED_ANIMALS_ELEMENT)
        & (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)
    ][["country", "year", "value"]]

    # Combine poultry and chicken data.
    compared = pd.merge(
        chickens_slaughtered,
        poultry_slaughtered,
        on=["country", "year"],
        how="outer",
        indicator=True,
        suffixes=("_chicken", "_poultry"),
    )

    error = "There are cases where slaughtered poultry is informed, but slaughered chicken is not."
    assert compared[compared["_merge"] == "right_only"].empty, error

    error = "There are rows where there is more slaughtered poultry than slaughtered chicken."
    assert compared[compared["value_poultry"] < compared["value_chicken"]].empty, error

    # Prepare a replacement dataframe for missing data on slaughtered poultry.
    poultry_slaughtered_missing_data = (
        compared[compared["_merge"] == "left_only"]
        .assign(
            **{
                "item_code": ITEM_CODE_MEAT_POULTRY,
                "item": ITEM_POULTRY,
                "fao_item": additional_fields["fao_item"],
                "fao_unit_short_name": additional_fields["fao_unit_short_name"],
                "item_description": additional_fields["item_description"],
            }
        )
        .drop(columns=["_merge", "value_poultry"])
        .rename(columns={"value_chicken": "value"})
    )

    log.info(
        f"Filling {len(poultry_slaughtered_missing_data)} rows of missing data for slaughtered poultry with "
        "slaughtered chicken."
    )
    # Add chicken data to the full dataframe.
    data = pd.concat([data, poultry_slaughtered_missing_data], ignore_index=True)

    return data


def add_slaughtered_animals_to_meat_total(data: pd.DataFrame) -> pd.DataFrame:
    """Add number of slaughtered animals to meat total.

    There is no FAOSTAT data on slaughtered animals for total meat. We construct this data by aggregating that element
    for the items specified in items_to_aggregate (which corresponds to all meat items after removing redundancies).

    If the number of slaughtered poultry is not informed, we remove the number of total animals slaughtered
    (since poultry are by far the most commonly slaughtered animals).

    Parameters
    ----------
    data : pd.DataFrame
        Processed data where meat total does not have number of slaughtered animals.

    Returns
    -------
    combined_data : pd.DataFrame
        Data after adding the new variable.

    """
    data = data.copy()

    error = f"Some items required to get the aggregate '{TOTAL_MEAT_ITEM}' are missing in data."
    assert set(MEAT_TOTAL_ITEM_CODES) < set(data["item_code"]), error
    assert SLAUGHTERED_ANIMALS_ELEMENT in data["element"].unique()
    assert SLAUGHTERED_ANIMALS_UNIT in data["unit"].unique()

    # Check that, indeed, the number of slaughtered animals for total meat is not given in the original data.
    assert data[
        (data["item"] == TOTAL_MEAT_ITEM)
        & (data["element"] == SLAUGHTERED_ANIMALS_ELEMENT)
        & (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)
    ].empty

    # There are two element codes for the same element (they have different items assigned).
    error = "Element codes for 'Producing or slaughtered animals' may have changed."
    assert (
        data[(data["element"] == SLAUGHTERED_ANIMALS_ELEMENT) & ~(data["element_code"].str.contains("pc"))][
            "element_code"
        ]
        .unique()
        .tolist()
        == SLAUGHTERED_ANIMALS_ELEMENT_CODES
    ), error

    # Check that the items assigned to each the two element codes do not overlap.
    error = "Element codes for 'Producing or slaughtered animals' have overlapping items."
    items_for_different_elements = (
        data[(data["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES))]
        .groupby("element_code", observed=True)
        .agg({"item_code": lambda x: list(x.unique())})
        .to_dict()["item_code"]
    )
    assert set.intersection(*[set(x) for x in items_for_different_elements.values()]) == set(), error

    # Confirm the item code for total meat.
    error = f"Item code for '{TOTAL_MEAT_ITEM}' may have changed."
    assert list(data[data["item"] == TOTAL_MEAT_ITEM]["item_code"].unique()) == [TOTAL_MEAT_ITEM_CODE], error

    # Select the subset of data to aggregate.
    data_to_aggregate = (
        data[
            (data["element"] == SLAUGHTERED_ANIMALS_ELEMENT)
            & (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)
            & (data["item_code"].isin(MEAT_TOTAL_ITEM_CODES))
        ]
        .dropna(subset="value")
        .reset_index(drop=True)
    )

    # Create a dataframe with the total number of animals used for meat.
    animals = dataframes.groupby_agg(
        data_to_aggregate,
        groupby_columns=[
            "area_code",
            "fao_country",
            "fao_element",
            "country",
            "year",
            "population_with_data",
        ],
        aggregations={
            "value": "sum",
            "flag": lambda x: x if len(x) == 1 else FLAG_MULTIPLE_FLAGS,
        },
    ).reset_index()

    # Get element description for selected element code (so far it's always been an empty string).
    _slaughtered_animals_element_description = data[data["element_code"].isin(SLAUGHTERED_ANIMALS_ELEMENT_CODES)][
        "element_description"
    ].unique()
    assert len(_slaughtered_animals_element_description) == 1
    slaughtered_animals_element_description = _slaughtered_animals_element_description[0]

    # Get item description for selected item code.
    _total_meat_item_description = data[data["item_code"] == TOTAL_MEAT_ITEM_CODE]["item_description"].unique()
    assert len(_total_meat_item_description) == 1
    total_meat_item_description = _total_meat_item_description[0]

    # Get FAO item name for selected item code.
    _total_meat_fao_item = data[data["item_code"] == TOTAL_MEAT_ITEM_CODE]["fao_item"].unique()
    assert len(_total_meat_fao_item) == 1
    total_meat_fao_item = _total_meat_fao_item[0]

    # Get FAO unit for selected item code.
    _total_meat_fao_unit = data[data["item_code"] == TOTAL_MEAT_ITEM_CODE]["fao_unit_short_name"].unique()
    assert len(_total_meat_fao_unit) == 1
    total_meat_fao_unit = _total_meat_fao_unit[0]

    # Manually include the rest of columns.
    animals["element"] = SLAUGHTERED_ANIMALS_ELEMENT
    animals["element_description"] = slaughtered_animals_element_description
    animals["unit"] = SLAUGHTERED_ANIMALS_UNIT
    animals["unit_short_name"] = SLAUGHTERED_ANIMALS_UNIT_SHORT_NAME
    # We arbitrarily assign the first element code (out of the two available) to the resulting variables.
    animals["element_code"] = SLAUGHTERED_ANIMALS_ELEMENT_CODE
    animals["item_code"] = TOTAL_MEAT_ITEM_CODE
    animals["item"] = TOTAL_MEAT_ITEM
    animals["item_description"] = total_meat_item_description
    animals["fao_item"] = total_meat_fao_item
    animals["fao_unit_short_name"] = total_meat_fao_unit

    log.info(f"Adding {len(animals)} rows with the total number of slaughtered animals for meat.")

    # For each year, we are adding up the number of animals slaughtered to compute the total, regardless of how many
    # of those animals have data.
    # However, some years do not have data for a particular animal; this is acceptable except if the animal is poultry,
    # which is the most commonly slaughtered animal. Therefore, if data is missing for poultry, the total will show a
    # significant (and spurious) decrease (this happens, e.g. in Estonia in 2019).
    # Therefore, we remove data points for which poultry is not informed.

    # Find country-years for which we have the number of poultry slaughtered.
    country_years_with_poultry_data = (
        data[
            (data["item_code"] == ITEM_CODE_MEAT_POULTRY)
            & (data["element"] == SLAUGHTERED_ANIMALS_ELEMENT)
            & (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)
        ]
        .dropna(subset="value")[["country", "year"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Add a column to inform of all those rows for which we don't have poultry data.
    compared = pd.merge(animals, country_years_with_poultry_data, how="outer", indicator=True)

    assert compared[compared["_merge"] == "right_only"].empty, "Expected 'left_only' or 'both', not 'right_only'."

    log.info(
        f"Removed {len(compared[compared['_merge'] == 'left_only'])} rows for which we don't have the number of "
        "poultry slaughtered."
    )

    animals_corrected = compared[compared["_merge"] == "both"].reset_index(drop=True).drop(columns=["_merge"])

    # Check that we are not missing any column.
    assert set(data.columns) == set(animals_corrected.columns)

    # Add animals data to the original dataframe.
    combined_data = (
        pd.concat([data, animals_corrected], ignore_index=True)
        .reset_index(drop=True)
        .astype(
            {
                "element_code": "category",
                "item_code": "category",
                "fao_item": "category",
                "fao_unit_short_name": "category",
                "flag": "category",
                "item": "category",
                "item_description": "category",
                "element": "category",
                "unit": "category",
                "element_description": "category",
                "unit_short_name": "category",
            }
        )
    )

    return combined_data


def add_yield_to_aggregate_regions(data: pd.DataFrame) -> pd.DataFrame:
    """Add yield (production / area harvested) to data for aggregate regions (i.e. continents and income groups).

    This data is not included in aggregate regions because it cannot be aggregated by simply summing the contribution of
    the individual countries. Instead, we need to aggregate production, then aggregate area harvested, and then divide
    one by the other.

    Note: Here, we divide production (the sum of the production from a list of countries in a region) by area (the sum
    of the area from a list of countries in a region) to obtain yield. But the list of countries that contributed to
    production may not be the same as the list of countries that contributed to area. We could impose that they must be
    the same, but this causes the resulting series to have gaps. Additionally, it seems that FAO also constructs yield
    in the same way. This was checked by comparing the resulting yield curves for 'Almonds' for all aggregate regions
    with their corresponding *(FAO) regions; they were identical.

    Parameters
    ----------
    data : pd.DataFrame
        Data that does not contain yield for aggregate regions.

    Returns
    -------
    combined_data : pd.DataFrame
        Data after adding yield.

    """
    # Element code of production, area harvested, and yield.
    production_element_code = "005510"
    area_element_code = "005312"
    yield_element_code = "005419"

    # Check that indeed regions do not contain any data for yield.
    assert data[(data["country"].isin(REGIONS_TO_ADD)) & (data["element_code"] == yield_element_code)].empty

    # Gather all fields that should stay the same.
    additional_fields = data[data["element_code"] == yield_element_code][
        [
            "element",
            "element_description",
            "fao_element",
            "fao_unit_short_name",
            "unit",
            "unit_short_name",
        ]
    ].drop_duplicates()
    assert len(additional_fields) == 1

    # Create a dataframe of production of regions.
    data_production = data[(data["country"].isin(REGIONS_TO_ADD)) & (data["element_code"] == production_element_code)]

    # Create a dataframe of area of regions.
    data_area = data[(data["country"].isin(REGIONS_TO_ADD)) & (data["element_code"] == area_element_code)]

    # Merge the two dataframes and create the new yield variable.
    merge_cols = [
        "area_code",
        "year",
        "item_code",
        "fao_country",
        "fao_item",
        "item",
        "item_description",
        "country",
    ]
    combined = pd.merge(
        data_production,
        data_area[merge_cols + ["flag", "value"]],
        on=merge_cols,
        how="inner",
        suffixes=("_production", "_area"),
    )

    combined["value"] = combined["value_production"] / combined["value_area"]

    # Replace infinities (caused by dividing by zero) by nan.
    combined["value"] = combined["value"].replace(np.inf, np.nan)

    # If both fields have the same flag, use that, otherwise use the flag of multiple flags.
    combined["flag"] = [
        flag_production if flag_production == flag_area else FLAG_MULTIPLE_FLAGS
        for flag_production, flag_area in zip(combined["flag_production"], combined["flag_area"])
    ]

    # Drop rows of nan and unnecessary columns.
    combined = combined.drop(columns=["flag_production", "flag_area", "value_production", "value_area"])
    combined = combined.dropna(subset="value").reset_index(drop=True)

    # Replace fields appropriately.
    combined["element_code"] = yield_element_code
    # Replace all other fields from the corresponding fields in yield (tonnes per hectare) variable.
    for field in additional_fields.columns:
        combined[field] = additional_fields[field].item()
    assert set(data.columns) == set(combined.columns)
    combined_data = (
        pd.concat([data, combined], ignore_index=True)
        .reset_index(drop=True)
        .astype(
            {
                "element_code": "category",
                "fao_element": "category",
                "fao_unit_short_name": "category",
                "flag": "category",
                "element": "category",
                "unit": "category",
                "element_description": "category",
                "unit_short_name": "category",
            }
        )
    )

    return combined_data


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load latest meadow dataset and keep its metadata.
    ds_meadow: catalog.Dataset = paths.load_dependency(dataset_short_name)
    # Load main table from dataset.
    tb_meadow = ds_meadow[dataset_short_name]
    data = pd.DataFrame(tb_meadow).reset_index()

    # Load dataset of FAOSTAT metadata.
    metadata: catalog.Dataset = paths.load_dependency(f"{NAMESPACE}_metadata")

    # Load dataset, items, element-units, and countries metadata.
    dataset_metadata = pd.DataFrame(metadata["datasets"]).loc[dataset_short_name].to_dict()
    items_metadata = pd.DataFrame(metadata["items"]).reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    elements_metadata = pd.DataFrame(metadata["elements"]).reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    countries_metadata = pd.DataFrame(metadata["countries"]).reset_index()
    amendments = parse_amendments_table(amendments=metadata["amendments"], dataset_short_name=dataset_short_name)

    #
    # Process data.
    #
    # Harmonize items and elements, and clean data.
    data = harmonize_items(df=data, dataset_short_name=dataset_short_name)
    data = harmonize_elements(df=data)

    # Prepare data.
    data = clean_data(
        data=data,
        items_metadata=items_metadata,
        elements_metadata=elements_metadata,
        countries_metadata=countries_metadata,
        amendments=amendments,
    )

    # Fill missing data for slaughtered poultry with slaughtered chicken.
    data = fill_slaughtered_poultry_with_slaughtered_chicken(data=data)

    # Include number of slaughtered animals in total meat (which is missing).
    data = add_slaughtered_animals_to_meat_total(data=data)

    # Add data for aggregate regions.
    data = add_regions(data=data, elements_metadata=elements_metadata)

    # Add per-capita variables.
    data = add_per_capita_variables(data=data, elements_metadata=elements_metadata)

    # Add yield (production per area) to aggregate regions.
    data = add_yield_to_aggregate_regions(data)

    # Handle detected anomalies in the data.
    data, anomaly_descriptions = handle_anomalies(dataset_short_name=dataset_short_name, data=data)

    # Create a long table (with item code and element code as part of the index).
    data_table_long = prepare_long_table(data=data)

    # Create a wide table (with only country and year as index).
    data_table_wide = prepare_wide_table(data=data)

    #
    # Save outputs.
    #
    # Update tables metadata.
    data_table_long.metadata.short_name = dataset_short_name
    data_table_long.metadata.title = dataset_metadata["owid_dataset_title"]
    data_table_wide.metadata.short_name = f"{dataset_short_name}_flat"
    data_table_wide.metadata.title = dataset_metadata["owid_dataset_title"] + ADDED_TITLE_TO_WIDE_TABLE

    # Initialise new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[data_table_long, data_table_wide], default_metadata=ds_meadow.metadata
    )

    # Update dataset metadata and add description of anomalies (if any) to the dataset description.
    ds_garden.metadata.description = (
        dataset_metadata["owid_dataset_description"] + anomaly_descriptions + SLAUGHTERED_ANIMALS_ADDITIONAL_DESCRIPTION
    )
    ds_garden.metadata.title = dataset_metadata["owid_dataset_title"]

    # Update the main source's metadata description (which will be shown in charts).
    ds_garden.metadata.sources[0].description = ds_garden.metadata.description

    # Create garden dataset.
    ds_garden.save()
