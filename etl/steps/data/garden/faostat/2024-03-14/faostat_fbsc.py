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

from pathlib import Path

from owid.catalog import Dataset, Table
from owid.datautils import dataframes
from shared import (
    ADDED_TITLE_TO_WIDE_TABLE,
    CURRENT_DIR,
    ELEMENTS_IN_FBSH_MISSING_IN_FBS,
    NAMESPACE,
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

# First year for which we have data in fbs dataset (it defines the first year when new methodology is used).
FBS_FIRST_YEAR = 2010
DATASET_TITLE = f"Food Balances (old methodology before {FBS_FIRST_YEAR}, and new from {FBS_FIRST_YEAR} onwards)"


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
        Combination of the tables of the two input datasets (as a dataframe, not a dataset).

    """
    # Sanity checks.
    error = "Description of fbs and fbsh datasets is different."
    assert ds_fbsh.metadata.description == ds_fbs.metadata.description, error
    error = "Licenses of fbsh and fbs are different."
    assert ds_fbsh.metadata.licenses == ds_fbs.metadata.licenses, error

    # Load dataframes for fbs and fbsh datasets.
    tb_fbsh = ds_fbsh["faostat_fbsh"].reset_index()
    tb_fbs = ds_fbs["faostat_fbs"].reset_index()

    # Harmonize items and elements in both datasets.
    tb_fbsh = harmonize_items(tb=tb_fbsh, dataset_short_name="faostat_fbsh")
    tb_fbsh = harmonize_elements(tb=tb_fbsh, dataset_short_name="faostat_fbsh")
    tb_fbs = harmonize_items(tb=tb_fbs, dataset_short_name="faostat_fbs")
    tb_fbs = harmonize_elements(tb=tb_fbs, dataset_short_name="faostat_fbs")

    # Ensure there is no overlap in data between the two datasets, and that there is no gap between them.
    assert tb_fbs["year"].min() == FBS_FIRST_YEAR, f"First year of fbs dataset is not {FBS_FIRST_YEAR}"
    if tb_fbsh["year"].max() >= tb_fbs["year"].min():
        # There is overlapping data between fbsh and fbs datasets. Prioritising fbs over fbsh."
        tb_fbsh = tb_fbsh.loc[tb_fbsh["year"] < tb_fbs["year"].min()].reset_index(drop=True)
    if (tb_fbsh["year"].max() + 1) < tb_fbs["year"].min():
        log.warning("Data is missing for one or more years between fbsh and fbs datasets.")

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

    # Concatenate old and new dataframes using function that keeps categoricals.
    tb_fbsc = dataframes.concatenate([tb_fbsh, tb_fbs]).sort_values(["area", "year"]).reset_index(drop=True)

    # Ensure that each element has only one unit and one description.
    error = "Some elements in the combined dataset have more than one unit. Manually check them and consider adding them to ELEMENT_AMENDMENTS."
    units_per_element = tb_fbsc.groupby("element", as_index=False, observed=True)["unit"].nunique()
    elements_with_ambiguous_units = units_per_element[units_per_element["unit"] > 1]["element"].tolist()
    tb_fbsc[tb_fbsc["element"].isin(elements_with_ambiguous_units)].drop_duplicates(subset=["element", "unit"])
    assert len(elements_with_ambiguous_units) == 0, error

    return tb_fbsc


def _assert_tb_size(tb: Table, size_mb: float) -> None:
    """Check that dataframe is smaller than given size to prevent OOM errors."""
    real_size_mb = tb.memory_usage(deep=True).sum() / 1e6
    assert real_size_mb <= size_mb, f"DataFrame size is too big: {real_size_mb} MB > {size_mb} MB"


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

    # Load fbsh and fbs.
    log.info("faostat_fbsc.loading_datasets")
    fbsh_dataset = paths.load_dataset(f"{NAMESPACE}_fbsh")
    fbs_dataset = paths.load_dataset(f"{NAMESPACE}_fbs")

    # Load dataset of FAOSTAT metadata.
    metadata = paths.load_dataset(f"{NAMESPACE}_metadata")

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
        fbsh_shape=fbsh_dataset["faostat_fbsh"].shape,
        fbs_shape=fbs_dataset["faostat_fbs"].shape,
    )
    data = combine_fbsh_and_fbs_datasets(fbsh_dataset, fbs_dataset)

    _assert_tb_size(data, 2000)

    # Prepare data.
    data = clean_data(
        tb=data,
        ds_population=ds_population,
        items_metadata=items_metadata,
        elements_metadata=elements_metadata,
        countries_metadata=countries_metadata,
        amendments=amendments,
    )

    # Add data for aggregate regions.
    data = add_regions(
        tb=data,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        ds_population=ds_population,
        elements_metadata=elements_metadata,
    )

    # Add per-capita variables.
    data = add_per_capita_variables(tb=data, elements_metadata=elements_metadata)

    # Handle detected anomalies in the data.
    data, anomaly_descriptions = handle_anomalies(dataset_short_name=dataset_short_name, data=data)

    # Avoid objects as they would explode memory, use categoricals instead.
    for col in data.columns:
        assert data[col].dtype != object, f"Column {col} should not have object type"

    _assert_tb_size(data, 2000)

    # Create a long table (with item code and element code as part of the index).
    log.info("faostat_fbsc.prepare_long_table", shape=data.shape)
    data_table_long = prepare_long_table(tb=data)

    _assert_tb_size(data_table_long, 2000)

    # Create a wide table (with only country and year as index).
    log.info("faostat_fbsc.prepare_wide_table", shape=data.shape)
    data_table_wide = prepare_wide_table(tb=data)

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
        dest_dir=dest_dir, tables=[data_table_long, data_table_wide], default_metadata=fbs_dataset.metadata
    )

    # Check that the title assigned here coincides with the one in custom_datasets.csv (for consistency).
    error = "Dataset title given to fbsc is different to the one in custom_datasets.csv. Update the latter file."
    assert DATASET_TITLE == dataset_metadata["owid_dataset_title"], error

    # Update dataset metadata and add description of anomalies (if any) to the dataset description.
    ds_garden.metadata.description = dataset_metadata["owid_dataset_description"] + anomaly_descriptions
    ds_garden.metadata.title = dataset_metadata["owid_dataset_title"]

    # Create garden dataset.
    ds_garden.save()
