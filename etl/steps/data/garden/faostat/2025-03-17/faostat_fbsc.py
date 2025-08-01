"""FAOSTAT garden step for faostat_fbsc dataset (Food Balances Combined).

Combine the old and new food balances datasets:
* `faostat_fbsh`: Old (historical) dataset.
* `faostat_fbs`: Current dataset.

A new (combined) dataset will be generated: "faostat_fbsc".

This is because a new version of the Food Balances dataset was launched in 2014 with a slightly new methodology:
https://fenixservices.fao.org/faostat/static/documents/FBS/New%20FBS%20methodology.pdf

NOTE: It seems that FAOSTAT is possibly extending the coverage of the new methodology. So the year of intersection of
both datasets will be earlier and earlier. The global variable `FBS_FIRST_YEAR` may have to be redefined in a future
update.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from shared import (
    ADDED_TITLE_TO_WIDE_TABLE,
    CURRENT_DIR,
    ELEMENTS_IN_FBSH_MISSING_IN_FBS,
    add_modified_variables,
    add_per_capita_variables,
    add_regions,
    clean_data,
    handle_anomalies,
    harmonize_elements,
    harmonize_items,
    improve_metadata,
    log,
    parse_amendments_table,
    prepare_long_table,
    prepare_wide_table,
    sanity_check_custom_units,
)

from etl.helpers import PathFinder

# First year for which we have data in fbs dataset (it defines the first year when new methodology is used).
FBS_FIRST_YEAR = 2010
DATASET_TITLE = f"Food Balances (old methodology before {FBS_FIRST_YEAR}, and new from {FBS_FIRST_YEAR} onwards)"
# Last year for which we have data in fbsh dataset (used for sanity checks).
FBSH_LAST_YEAR = 2013


def combine_fbsh_and_fbs_datasets(
    ds_fbsh: Dataset,
    ds_fbs: Dataset,
) -> Table:
    """Combine `faostat_fbsh` and `faostat_fbs` meadow datasets.

    Parameters
    ----------
    ds_fbsh : Dataset
        Meadow `faostat_fbsh` dataset.
    ds_fbs : Dataset
        Meadow `faostat_fbs` dataset.

    Returns
    -------
    tb_fbsc : Table
        Combination of the tables of the two input datasets (as a table, not a dataset).

    """
    # Sanity checks.
    error = "Description of fbs and fbsh datasets is different."
    assert ds_fbsh.metadata.description == ds_fbs.metadata.description, error
    error = "Licenses of fbsh and fbs are different."
    assert ds_fbsh.metadata.licenses == ds_fbs.metadata.licenses, error

    # Load tables for fbs and fbsh datasets.
    tb_fbsh = ds_fbsh["faostat_fbsh"].reset_index()
    tb_fbs = ds_fbs["faostat_fbs"].reset_index()

    # Harmonize items and elements in both datasets.
    tb_fbsh = harmonize_items(tb=tb_fbsh, dataset_short_name="faostat_fbsh")
    tb_fbsh = harmonize_elements(tb=tb_fbsh, dataset_short_name="faostat_fbsh")
    tb_fbs = harmonize_items(tb=tb_fbs, dataset_short_name="faostat_fbs")
    tb_fbs = harmonize_elements(tb=tb_fbs, dataset_short_name="faostat_fbs")

    # FBSH and FBS overlap for years 2010, 2011, 2012, and 2013.
    # We need to remove overlapping data, prioritising FBS.
    assert tb_fbs["year"].min() == FBS_FIRST_YEAR, f"First year of fbs dataset is not {FBS_FIRST_YEAR}"
    assert tb_fbsh["year"].max() == FBSH_LAST_YEAR, f"Last year of fbsh dataset is not {FBSH_LAST_YEAR}"
    # We could simply do:
    # tb_fbsh = tb_fbsh.loc[(tb_fbsh["year"] < tb_fbs["year"].min())].reset_index(drop=True)
    # However, we have to handle the special case of Sudan:
    # In FBSH, there is data for "Sudan (former)", for all years from 1961 to 2011, as well as "Sudan" (which corresponds to North Sudan), for just 2012 and 2013.
    # However, FBS only has data for "Sudan" (from 2012, corresponding to North Sudan), and "South Sudan" (only from 2019).
    # This causes that, when combining FBS (from 2010) with FBSH (up to 2009) we lose data for "Sudan (former)" for years 2010 and 2011. This causes a dip in various African indicators for those two years.
    # The solution is to keep the data for "Sudan (former)" for those two years from FBSH, and append it to FBS after the combination of FBS and FBSH.
    error = "Data for Sudan (former, north and south) has changed in FBSH or FBS."
    assert tb_fbsh[tb_fbsh["area"] == "Sudan (former)"]["year"].max() == 2011
    assert set(tb_fbsh[tb_fbsh["area"] == "Sudan"]["year"]) == {2012, 2013}
    assert tb_fbs[tb_fbs["area"] == "Sudan (former)"].empty, error
    assert tb_fbs[tb_fbs["area"] == "Sudan"]["year"].min() == 2012, error
    assert tb_fbs[tb_fbs["area"] == "South Sudan"]["year"].min() == 2019, error
    # Remove years from FBSH for which we have data in FBS, but keep all years for Sudan (former).
    tb_fbsh = tb_fbsh.loc[(tb_fbsh["year"] < tb_fbs["year"].min()) | (tb_fbsh["area"] == "Sudan (former)")].reset_index(
        drop=True
    )

    # Sanity checks.
    # Ensure the elements that are in fbsh but not in fbs are covered by ITEMS_MAPPING.
    error = "Mismatch between items in fbsh and fbs. Redefine shared.ITEM_AMENDMENTS."
    assert set(tb_fbsh["item"]) == set(tb_fbs["item"]), error
    assert set(tb_fbsh["item_code"]) == set(tb_fbs["item_code"]), error
    # Some elements are found in fbs but not in fbsh. This is understandable, since fbs is
    # more recent and may have additional elements. However, ensure that there are no
    # elements in fbsh that are not in fbs.
    error = "There are elements in fbsh that are not in fbs."
    assert set(tb_fbsh["element"]) < set(tb_fbs["element"]), error
    assert set(tb_fbsh["element_code"]) - set(tb_fbs["element_code"]) == ELEMENTS_IN_FBSH_MISSING_IN_FBS, error

    # Remove elements from fbsh that are not in fbs (since they have different meanings and hence should not be
    # combined as if they were the same element).
    tb_fbsh = tb_fbsh[~tb_fbsh["element_code"].isin(ELEMENTS_IN_FBSH_MISSING_IN_FBS)].reset_index(drop=True)

    # Concatenate old and new tables.
    # tb_fbsc = dataframes.concatenate([tb_fbsh, tb_fbs]).sort_values(["area", "year"]).reset_index(drop=True)
    tb_fbsc = pr.concat([tb_fbsh, tb_fbs]).sort_values(["area", "year"]).reset_index(drop=True)

    # Sanity check.
    error = "Found duplicated rows after combining FBSH and FBS."
    assert tb_fbsc[tb_fbsc.duplicated(subset=["area", "year", "item_code", "element_code"])].empty, error

    # Ensure that each element has only one unit and one description.
    error = "Some elements in the combined dataset have more than one unit. Manually check them and consider adding them to ELEMENT_AMENDMENTS."
    units_per_element = tb_fbsc.groupby("element", as_index=False, observed=True)["unit"].nunique()
    elements_with_ambiguous_units = units_per_element[units_per_element["unit"] > 1]["element"].tolist()
    # tb_fbsc[tb_fbsc["element"].isin(elements_with_ambiguous_units)].drop_duplicates(subset=["element", "unit"])
    assert len(elements_with_ambiguous_units) == 0, error

    return tb_fbsc


def _assert_tb_size(tb: Table, size_mb: float) -> None:
    """Check that table is smaller than given size to prevent OOM errors."""
    real_size_mb = tb.memory_usage(deep=True).sum() / 1e6
    assert real_size_mb <= size_mb, f"Table size is too big: {real_size_mb} MB > {size_mb} MB"


def run() -> None:
    #
    # Load data.
    #
    # Define the dataset short name.
    dataset_short_name = "faostat_fbsc"

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load fbsh and fbs.
    log.info("faostat_fbsc.loading_datasets")
    ds_fbsh = paths.load_dataset("faostat_fbsh")
    ds_fbs = paths.load_dataset("faostat_fbs")

    # Load dataset of FAOSTAT metadata.
    metadata = paths.load_dataset("faostat_metadata")

    # Load dataset, items, element-units, and countries metadata.
    dataset_metadata = metadata["datasets"].loc[dataset_short_name].to_dict()
    items_metadata = metadata["items"].reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    elements_metadata = metadata["elements"].reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    countries_metadata = metadata["countries"].reset_index()
    amendments = parse_amendments_table(amendments=metadata["amendments"], dataset_short_name=dataset_short_name)

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Lod population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Combine fbsh and fbs datasets.
    log.info(
        "faostat_fbsc.combine_fbsh_and_fbs_datasets",
        fbsh_shape=ds_fbsh["faostat_fbsh"].shape,
        fbs_shape=ds_fbs["faostat_fbs"].shape,
    )
    tb = combine_fbsh_and_fbs_datasets(ds_fbsh=ds_fbsh, ds_fbs=ds_fbs)

    # _assert_tb_size(tb, 2000)

    # Prepare data.
    tb = clean_data(
        tb=tb,
        ds_population=ds_population,
        items_metadata=items_metadata,
        elements_metadata=elements_metadata,
        countries_metadata=countries_metadata,
        amendments=amendments,
    )

    # Add data for aggregate regions.
    tb = add_regions(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        ds_population=ds_population,
        elements_metadata=elements_metadata,
    )

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, elements_metadata=elements_metadata)

    # Handle detected anomalies in the data.
    tb, anomaly_descriptions = handle_anomalies(dataset_short_name=dataset_short_name, tb=tb)

    # For convenience, create additional indicators in different units.
    tb = add_modified_variables(tb=tb, dataset_short_name=dataset_short_name)

    # Avoid objects as they would explode memory, use categoricals instead.
    # for col in tb.columns:
    # assert tb[col].dtype != object, f"Column {col} should not have object type"

    # _assert_tb_size(tb, 2000)

    # Create a long table (with item code and element code as part of the index).
    log.info("faostat_fbsc.prepare_long_table", shape=tb.shape)
    tb_long = prepare_long_table(tb=tb)

    # _assert_tb_size(tb_long, 2000)

    # Create a wide table (with only country and year as index).
    log.info("faostat_fbsc.prepare_wide_table", shape=tb.shape)
    tb_wide = prepare_wide_table(tb=tb)

    # Improve metadata (of wide table).
    improve_metadata(tb_wide=tb_wide, dataset_short_name=dataset_short_name)

    # Check that column "value" has two origins (other columns are not as important and may not have origins).
    error = f"Column 'value' of the long table of {dataset_short_name} must have two origins."
    assert len(tb_long["value"].metadata.origins) == 2, error
    error = f"All value columns of the wide table of {dataset_short_name} must have two origins."
    assert all(
        [len(tb_wide[column].metadata.origins) == 2 for column in tb_wide.columns if column not in ["area_code"]]
    ), error

    #
    # Save outputs.
    #
    # Update tables metadata.
    tb_long.metadata.short_name = dataset_short_name
    tb_long.metadata.title = dataset_metadata["owid_dataset_title"]
    tb_wide.metadata.short_name = f"{dataset_short_name}_flat"
    tb_wide.metadata.title = dataset_metadata["owid_dataset_title"] + ADDED_TITLE_TO_WIDE_TABLE

    # Initialise new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_long, tb_wide], default_metadata=ds_fbs.metadata, check_variables_metadata=False
    )

    # Sanity check custom units.
    sanity_check_custom_units(tb_wide=tb_wide, ds_garden=ds_garden)

    # Check that the title assigned here coincides with the one in custom_datasets.csv (for consistency).
    error = "Dataset title given to fbsc is different to the one in custom_datasets.csv. Update the latter file."
    assert DATASET_TITLE == dataset_metadata["owid_dataset_title"], error

    # Update dataset metadata.
    ds_garden.metadata.title = dataset_metadata["owid_dataset_title"]
    # The following description is not publicly shown in charts; it is only visible when accessing the catalog.
    ds_garden.metadata.description = dataset_metadata["owid_dataset_description"] + anomaly_descriptions

    # Create garden dataset.
    ds_garden.save()
