"""Auxiliary FAOSTAT RL step that does some basic processing and creates a dataset that is used by the population dataset to calculate population density.

"""

import os
import tempfile
import zipfile
from typing import Dict

import numpy as np
import owid.catalog.processing as pr
import structlog
from owid.catalog import Table, Variable, VariablePresentationMeta
from owid.catalog.utils import underscore
from owid.datautils import dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Initialise log.
log = structlog.get_logger()

paths = PathFinder(__file__)

# Elements and items.

# Maximum number of characters for item_code.
# FAOSTAT "item_code" is usually an integer number, however sometimes it has decimals and sometimes it contains letters.
# So we will convert it into a string of this number of characters (integers will be prepended with zeros).
N_CHARACTERS_ITEM_CODE = 8
# Maximum number of characters for item_code for faostat_sdgb and faostat_fs, which have a different kind of item codes,
# e.g. '24002-F-Y_GE15', '24002-M-Y_GE15', etc.
N_CHARACTERS_ITEM_CODE_EXTENDED = 15
# Maximum number of characters for element_code (integers will be prepended with zeros).
N_CHARACTERS_ELEMENT_CODE = 6

# Shared functions.


def load_data(snapshot: Snapshot) -> Table:
    """Load snapshot data (as a table) for current dataset.

    Parameters
    ----------
    local_path : Path or str
        Path to local snapshot file.

    Returns
    -------
    data : Table
        Snapshot data.

    """
    # Unzip data into a temporary folder.
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(snapshot.path)
        z.extractall(temp_dir)
        (filename,) = list(filter(lambda x: "(Normalized)" in x, os.listdir(temp_dir)))

        # Load data from main file.
        data = pr.read_csv(
            os.path.join(temp_dir, filename),
            encoding="latin-1",
            low_memory=False,
            origin=snapshot.metadata.origin,
            metadata=snapshot.to_table_metadata(),
        )

    return data


def run_sanity_checks(tb: Table) -> None:
    """Run basic sanity checks on loaded data (raise assertion errors if any check fails).

    Parameters
    ----------
    tb : Table
        Data to be checked.

    """
    tb = tb.copy()

    # Check that column "Year Code" is identical to "Year", and can therefore be dropped.
    error = "Column 'Year Code' does not coincide with column 'Year'."
    if "Year" not in tb.columns:
        pass
        # Column 'Year' is not in data (this happens at least in faostat_wcad, which requires further processing).
    elif tb["Year"].dtype == int:
        # In most cases, columns "Year Code" and "Year" are simply the year.
        assert (tb["Year Code"] == tb["Year"]).all(), error
    else:
        # Sometimes (e.g. for dataset fs) there are year ranges (e.g. with "Year Code" 20002002 and "Year" "2000-2002").
        assert (tb["Year Code"] == tb["Year"].str.replace("-", "").astype(int)).all(), error

    # Check that there is only one element-unit for each element code.
    error = "Multiple element-unit for the same element code."
    assert (tb.groupby(["Element", "Unit"])["Element Code"].nunique() == 1).all(), error


def check_that_countries_are_well_defined(tb: Table) -> None:
    """Apply sanity checks related to the definition of countries.

    Parameters
    ----------
    tb : Table
        Data, right after harmonizing country names.

    """
    # Ensure area codes and countries are well defined, and no ambiguities were introduced when mapping country names.
    n_countries_per_area_code = tb.groupby("area_code")["country"].transform("nunique")
    ambiguous_area_codes = (
        tb.loc[n_countries_per_area_code > 1][["area_code", "country"]]
        .drop_duplicates()
        .set_index("area_code")["country"]
        .to_dict()
    )
    error = (
        f"There cannot be multiple countries for the same area code. "
        f"Redefine countries file for:\n{ambiguous_area_codes}."
    )
    assert len(ambiguous_area_codes) == 0, error
    n_area_codes_per_country = tb.groupby("country")["area_code"].transform("nunique")
    ambiguous_countries = (
        tb.loc[n_area_codes_per_country > 1][["area_code", "country"]]
        .drop_duplicates()
        .set_index("area_code")["country"]
        .to_dict()
    )
    error = (
        f"There cannot be multiple area codes for the same countries. "
        f"Redefine countries file for:\n{ambiguous_countries}."
    )
    assert len(ambiguous_countries) == 0, error


def harmonize_items(tb: Table, item_col: str = "item") -> Table:
    """Harmonize item codes (by ensuring they are strings of numbers with a fixed length, prepended with zeros), make
    amendments to faulty items, and make item codes and items of categorical dtype.

    Parameters
    ----------
    tb : Table
        Data before harmonizing item codes.
    item_col : str
        Name of items column.

    Returns
    -------
    tb : Table
        Data after harmonizing item codes.

    """
    tb = tb.copy()

    n_characters_item_code = N_CHARACTERS_ITEM_CODE

    # Note: Here list comprehension is faster than doing .astype(str).str.zfill(...).
    tb["item_code"] = [str(item_code).zfill(n_characters_item_code) for item_code in tb["item_code"]]

    # Convert both columns to category to reduce memory.
    tb = tb.astype({"item_code": "category", item_col: "category"})

    # Remove unused categories.
    tb["item_code"] = tb["item_code"].cat.remove_unused_categories()
    tb[item_col] = tb[item_col].cat.remove_unused_categories()

    return tb


def harmonize_elements(tb: Table, element_col: str = "element") -> Table:
    """Harmonize element codes (by ensuring they are strings of numbers with a fixed length, prepended with zeros), and
    make element codes and elements of categorical dtype.

    Parameters
    ----------
    tb : Table
        Data before harmonizing element codes.
    dataset_short_name : str
        Dataset short name.
    element_col : str
        Name of element column (this is only necessary to convert element column into categorical dtype).

    Returns
    -------
    tb : Table
        Data after harmonizing element codes.

    """
    tb = tb.copy()
    tb["element_code"] = [str(element_code).zfill(N_CHARACTERS_ELEMENT_CODE) for element_code in tb["element_code"]]

    # Convert both columns to category to reduce memory
    tb = tb.astype({"element_code": "category", element_col: "category"})

    return tb


def harmonize_countries(tb: Table) -> Table:
    """Harmonize country names.

    A new column 'country' will be added, with the harmonized country names.

    Parameters
    ----------
    tb : Table
        Data before harmonizing country names.

    Returns
    -------
    tb : Table
        Data after harmonizing country names.

    """
    tb["area_code"] = tb["area_code"].astype(int)

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path.parent / "faostat.countries.json",
        excluded_countries_file=paths.excluded_countries_path.parent / "faostat.excluded_countries.json",
        warn_on_missing_countries=True,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
    )

    # Further sanity checks.
    check_that_countries_are_well_defined(tb)

    # Set appropriate dtypes.
    tb = tb.astype({"country": "category"})

    return tb


def remove_rows_with_nan_value(tb: Table, verbose: bool = False) -> Table:
    """Remove rows for which column "value" is nan.

    Parameters
    ----------
    tb : Table
        Data for current dataset.
    verbose : bool
        True to display information about the number and fraction of rows removed.

    Returns
    -------
    tb : Table
        Data after removing nan values.

    """
    tb = tb.copy()
    # Number of rows with a nan in column "value".
    # We could also remove rows with any nan, however, before doing that, we would need to assign a value to nan flags.
    n_rows_with_nan_value = len(tb[tb["value"].isnull()])
    if n_rows_with_nan_value > 0:
        frac_nan_rows = n_rows_with_nan_value / len(tb)
        if verbose:
            log.info(f"Removing {n_rows_with_nan_value} rows ({frac_nan_rows: .2%}) " f"with nan in column 'value'.")
        if frac_nan_rows > 0.15:
            log.warning(f"{frac_nan_rows: .0%} rows of nan values removed.")
        tb = tb.dropna(subset="value").reset_index(drop=True)

    return tb


def remove_columns_with_only_nans(tb: Table, verbose: bool = True) -> Table:
    """Remove columns that only have nans.

    In principle, it should not be possible that columns have only nan values, but we use this function just in case.

    Parameters
    ----------
    tb : Table
        Data for current dataset.
    verbose : bool
        True to display information about the removal of columns with nan values.

    Returns
    -------
    tb : Table
        Data after removing columns of nans.

    """
    tb = tb.copy()
    # Remove columns that only have nans.
    columns_of_nans = tb.columns[tb.isnull().all(axis=0)]
    if len(columns_of_nans) > 0:
        if verbose:
            log.info(
                f"Removing {len(columns_of_nans)} columns ({len(columns_of_nans) / len(tb.columns): .2%}) "
                f"that have only nans."
            )
        tb = tb.drop(columns=columns_of_nans)

    return tb


def clean_year_column(year_column: Variable) -> Variable:
    """Clean year column.

    Year is given almost always as an integer value. But sometimes (e.g. in the faostat_fs dataset) it is a range of
    years (that differ by exactly 2 years, e.g. "2010-2012"). This function returns a series of integer years, which, in
    the cases where the original year was a range, corresponds to the mean of the range.

    Parameters
    ----------
    year_column : Variable
        Original column of year values (which may be integer, or ranges of values).

    Returns
    -------
    year_clean_series : Variable
        Clean column of years, as integer values.

    """
    year_clean = []
    for year in year_column:
        if "-" in str(year):
            year_range = year.split("-")
            year_min = int(year_range[0])
            year_max = int(year_range[1])
            assert year_max - year_min == 2
            year_clean.append(year_min + 1)
        else:
            year_clean.append(int(year))

    # Prepare series of integer year values.
    year_clean_series = Variable(year_clean, name="year")

    return year_clean_series


def clean_data_values(values: Variable) -> Variable:
    """Fix spurious data values (defined in value_amendments.csv) and make values a float column.

    Parameters
    ----------
    values : Variable
        Content of the "value" column in the original data.

    Returns
    -------
    values_clean : Variable
        Original values after fixing known issues and converting to float.

    """
    values_clean = values.copy()

    # Convert all numbers into numeric.
    # Note: If this step fails with a ValueError, it may be because other spurious values have been introduced.
    # If so, add them to value_amendments.csv and re-run faostat_metadata.
    values_clean = values_clean.astype(float)

    return values_clean


def clean_data(
    tb: Table,
) -> Table:
    """Process data (with already harmonized item codes and element codes).

    Parameters
    ----------
    tb : Table
        Unprocessed data for current dataset (with harmonized item codes and element codes).

    Returns
    -------
    tb : Table
        Processed data, ready to be made into a table for a garden dataset.

    """
    # Fix spurious data values (applying mapping in value_amendments.csv) and ensure column of values is float.
    tb["value"] = clean_data_values(tb["value"])

    tb = tb.rename(columns={"area": "country"})

    # Ensure year column is integer (sometimes it is given as a range of years, e.g. 2013-2015).
    tb["year"] = clean_year_column(tb["year"])

    # Remove rows with nan value.
    tb = remove_rows_with_nan_value(tb)

    # Harmonize country names.
    tb = harmonize_countries(tb=tb)

    # Convert back to categorical columns (maybe this should be handled automatically in `add_population_to_table`)
    tb = tb.astype({"country": "category"})

    return tb


def prepare_long_table(tb: Table) -> Table:
    """Prepare a data table in long format.

    Parameters
    ----------
    tb : Table
        Data (as a dataframe) in long format.

    Returns
    -------
    tb_long : Table
        Data (as a table) in long format.

    """
    # Create new table with long data.
    tb_long = tb.copy()

    # Set appropriate indexes.
    tb_long = tb_long.format(keys=["area_code", "year", "item_code", "element_code"], short_name=paths.short_name)

    # Sanity check.
    number_of_infinities = len(tb_long[tb_long["value"] == np.inf])
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the long table."

    return tb_long


def create_variable_short_names(variable_name: str) -> str:
    """Create lower-snake-case short names for the columns in the wide (flatten) output table, ensuring that they are
    not too long (to avoid issues when inserting variable in grapher).

    If a new name is too long, the ending of the item name will be reduced.
    If the item name is not long enough to solve the problem, this function will raise an assertion error.

    Parameters
    ----------
    variable_name : str
        Variable name.

    Returns
    -------
    new_name : str
        New variable name.

    """
    # Extract all the necessary fields from the variable name.
    item, item_code, element, element_code, unit = variable_name.replace("||", "|").split(" | ")

    # Check that the extraction was correct by constructing the variable name again and comparing with the original.
    assert variable_name == f"{item} | {item_code} || {element} | {element_code} || {unit}"

    new_name = underscore(variable_name)

    # Check that the number of characters of the short name is not too long.
    n_char = len(new_name)
    if n_char > 255:
        # This name will cause an issue when uploading to grapher (because of a limit of 255 characters in short name).
        # Remove the extra characters from the ending of the item name (if possible).
        n_char_to_be_removed = n_char - 255
        # It could happen that it is not the item name that is long, but the element name, dataset, or unit.
        # But for the moment, assume it is the item name.
        assert len(item) > n_char_to_be_removed, "Variable name is too long, but it is not due to item name."
        new_item = underscore(item)[0:-n_char_to_be_removed]
        new_name = underscore(f"{new_item} | {item_code} || {element} | {element_code} || {unit}")

    # Check that now the new name now fulfils the length requirement.
    error = "Variable short name is too long. Improve create_variable_names function to account for this case."
    assert len(new_name) <= 255, error

    return new_name


def prepare_wide_table(tb: Table) -> Table:
    """Flatten a long table to obtain a wide table with ["country", "year"] as index.

    The input table will be pivoted to have [country, year] as index, and as many columns as combinations of
    item-element-unit entities.

    Parameters
    ----------
    tb : Table
        Data for current domain.

    Returns
    -------
    tb_wide : Table
        Data table with index [country, year].

    """
    tb = tb.astype({"unit": "category"})

    # Construct a variable name that will not yield any possible duplicates.
    # This will be used as column names (which will then be formatted properly with underscores and lower case),
    # and also as the variable titles in grapher.
    # Also, for convenience, keep a similar structure as in the previous OWID dataset release.
    # Finally, ensure that the short name version of the variable is not too long
    # (which would cause issues when uploading to grapher).
    tb["variable_name"] = dataframes.apply_on_categoricals(
        [tb.item, tb.item_code, tb.element, tb.element_code, tb.unit],
        lambda item,
        item_code,
        element,
        element_code,
        unit: f"{item} | {item_code} || {element} | {element_code} || {unit}",
    )

    # Construct a human-readable variable display name (which will be shown in grapher charts).
    tb["variable_display_name"] = dataframes.apply_on_categoricals(
        [tb.item, tb.element, tb.unit],
        lambda item, element, unit: f"{item} - {element} ({unit})",
    )

    # This is the case for faostat_qv since the last update.
    tb["variable_description"] = ""

    # Pivot over long dataframe to generate a wide dataframe with country-year as index, and as many columns as
    # unique elements in "variable_name" (which should be as many as combinations of item-elements).
    # Note: We include area_code in the index for completeness, but by construction country-year should not have
    # duplicates.
    # Create a wide table with just the data values.
    tb_wide = tb.pivot(
        index=["area_code", "country", "year"],
        columns=["variable_name"],
        values="value",
    )

    # Add variable name.
    for column in tb_wide.columns:
        tb_wide[column].metadata.title = column

    # Add variable unit (long name).
    variable_name_mapping = _variable_name_map(tb, "unit")
    for column in tb_wide.columns:
        tb_wide[column].metadata.unit = variable_name_mapping[column]

    # This is the case for faostat_qv since the last update.
    for column in tb_wide.columns:
        tb_wide[column].metadata.short_unit = ""

    # Add variable description.
    variable_name_mapping = _variable_name_map(tb, "variable_description")
    for column in tb_wide.columns:
        tb_wide[column].metadata.description_from_producer = variable_name_mapping[column]

    # Add display and presentation parameters (for grapher).
    variable_name_mapping = _variable_name_map(tb, "variable_display_name")
    for column in tb_wide.columns:
        tb_wide[column].metadata.display = {"name": variable_name_mapping[column]}
        tb_wide[column].metadata.presentation = VariablePresentationMeta(title_public=variable_name_mapping[column])

    # Sort columns and rows conveniently.
    tb_wide = tb_wide.reset_index().format(short_name="faostat_rl_auxiliary_flat")
    tb_wide = tb_wide[["area_code"] + sorted([column for column in tb_wide.columns if column != "area_code"])]
    tb_wide = tb_wide.sort_index(level=["country", "year"]).sort_index()

    # Make all column names snake_case.
    variable_to_short_name = {
        column: create_variable_short_names(variable_name=tb_wide[column].metadata.title)
        for column in tb_wide.drop(columns=["area_code"]).columns
        if tb_wide[column].metadata.title is not None
    }
    tb_wide = tb_wide.rename(columns=variable_to_short_name, errors="raise")

    # Sanity check.
    number_of_infinities = np.isinf(tb_wide.select_dtypes(include=np.number).fillna(0)).values.sum()
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the wide table."

    return tb_wide


def _variable_name_map(data: Table, column: str) -> Dict[str, str]:
    """Extract map {variable name -> column} from dataframe and make sure it is unique (i.e. ensure that one variable
    does not map to two distinct values)."""
    pivot = data.dropna(subset=[column]).groupby(["variable_name"], observed=True)[column].apply(set)
    assert all(pivot.map(len) == 1)
    return pivot.map(lambda x: list(x)[0]).to_dict()  # type: ignore


def run() -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snapshot = paths.load_snapshot("faostat_rl")
    tb = load_data(snapshot=snapshot)

    #
    # Process data.
    #
    # Run sanity checks.
    run_sanity_checks(tb=tb)

    # Prepare table.
    tb = tb[["Area Code", "Area", "Year", "Item Code", "Item", "Element Code", "Element", "Unit", "Value"]].underscore()

    # TODO: This step is a mix of the old meadow and garden FAOSTAT steps. But the only required column in the end is: land_area__00006601__area__005110__hectares
    # So we can greatly simplify this step to keep only what's needed.
    # tb = tb[(tb["item_code"] == 6601) & (tb["element_code"] == 5110)].reset_index()

    # Harmonize items and elements, and clean data.
    tb = harmonize_items(tb=tb)
    tb = harmonize_elements(tb=tb)

    # Prepare data.
    tb = clean_data(tb=tb)

    # Create a wide table (with only country and year as index).
    tb_wide = prepare_wide_table(tb=tb)

    tb_wide = tb_wide.reset_index()[["country", "year", "land_area__00006601__area__005110__1000_ha"]]
    tb_wide["land_area__00006601__area__005110__1000_ha"] *= 1000
    tb_wide = tb_wide.rename(
        columns={"land_area__00006601__area__005110__1000_ha": "land_area__00006601__area__005110__hectares"}
    )

    tb_wide = tb_wide.format()

    error = "All value columns of the wide table must have one origin."
    assert all(
        [len(tb_wide[column].metadata.origins) == 1 for column in tb_wide.columns if column not in ["area_code"]]
    ), error

    #
    # Save outputs.
    #
    # Initialise new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_wide], default_metadata=snapshot.to_table_metadata(), check_variables_metadata=False
    )

    # Create garden dataset.
    ds_garden.save()
