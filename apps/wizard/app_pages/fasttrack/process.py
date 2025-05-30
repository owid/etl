"""Data/metadata processing tools.

Relies on Streamlit.
"""

import json
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore, validate_underscore

from apps.wizard.app_pages.fasttrack.fast_import import FasttrackImport
from apps.wizard.app_pages.fasttrack.load import load_data_from_csv, load_data_from_sheets
from apps.wizard.app_pages.fasttrack.utils import IMPORT_GSHEET, LOCAL_CSV, UPDATE_GSHEET, ValidationError, set_states
from etl.paths import DATA_DIR, LATEST_REGIONS_DATASET_PATH


@st.cache_data(show_spinner=False)
def processing_part_1(import_method, dataset_uris, infer_metadata, is_private, _status):
    """Load and first processing."""
    # 1/ LOCAL CSV
    if import_method == LOCAL_CSV:
        # Get filename, show notification
        data, dataset_meta, variables_meta_dict, origin = load_data_from_csv(
            dataset_uris["uploaded_file"],
        )
        dataset_uri = dataset_uris["uploaded_file"].name

    # 2/ New GOOGLE SHEET
    elif import_method == IMPORT_GSHEET:
        data, dataset_meta, variables_meta_dict, origin = load_data_from_sheets(
            dataset_uris["sheets_url"], _status=_status
        )
        dataset_uri = dataset_uris["sheets_url"]

    # 3/ UPDATE GOOGLE SHEET
    elif import_method == UPDATE_GSHEET:
        data, dataset_meta, variables_meta_dict, origin = load_data_from_sheets(
            dataset_uris["existing_sheet"]["value"], _status=_status
        )
        dataset_uri = dataset_uris["existing_sheet"]["value"]
    else:
        raise ValueError(f"Unknown import method: {import_method}")

    # PROCESS
    if infer_metadata:
        st.write("Inferring metadata...")
        data, variables_meta_dict = _infer_metadata(data, variables_meta_dict)

    data = _convert_percentages(data, variables_meta_dict)
    data = _clean_numeric_formatting(data, variables_meta_dict)

    # VALIDATION
    st.write("Validating data and metadata...")
    success = _validate_data(data, variables_meta_dict)
    if not success:
        _status.update(state="error")
        st.stop()

    # HARMONIZATION
    # NOTE: harmonization is not done in ETL, but here in fast-track for technical reasons
    # It's not yet clear what will authors prefer and how should we handle preprocessing from
    # raw data to data saved as snapshot
    st.write("Harmonizing countries...")
    data, unknown_countries = _harmonize_countries(data)
    if unknown_countries:
        st.error(f"There are {len(unknown_countries)} unknown entities!")
        _status.update(state="error")

    dims = [c for c in data.columns if c.startswith("dim_")]
    if dims:
        st.write(f"Using dimensions: {', '.join(dims)}...")
        variables_meta_dict = {k: v for k, v in variables_meta_dict.items() if k not in dims}

    # Update dataset metadata
    dataset_meta.is_public = not is_private

    return data, dataset_meta, variables_meta_dict, origin, unknown_countries, dataset_uri


####  MAIN FUNCTION (2)
def processing_part_2(data, dataset_meta, variables_meta_dict, origin, dataset_uri, status, import_method):
    """Continue processing after data has been loaded.

    This function is expected to be run after `load_data_from_sheets` has been executed.
    """
    with status:
        # Build table
        st.write("Building table and dataset objects...")
        tb = Table(data, short_name=dataset_meta.short_name)
        for short_name, var_meta in variables_meta_dict.items():
            tb[short_name].metadata = var_meta

        # Set dimensions as index
        dims = [c for c in data.columns if c.startswith("dim_")]
        if dims:
            tb = tb.set_index(dims, append=True)

        # Build dataset
        dataset_meta.channel = "grapher"
        dataset = Dataset.create_empty(DATA_DIR / dataset_meta.uri, dataset_meta)
        dataset.add(tb)
        dataset.save()

        # Prepare import
        st.write("Building submission...")
        fast_import = FasttrackImport(
            dataset=dataset,
            origin=origin,
            dataset_uri=dataset_uri,
            is_gsheet=import_method in (IMPORT_GSHEET, UPDATE_GSHEET),
        )

    # Cross-check with existing dataset
    if fast_import.snapshot_exists() and fast_import.metadata_path.exists():
        with st.form("crosscheck_form"):
            st.markdown("Do you want to continue and add the dataset to the Grapher database?")
            with st.expander("See changes in metadata", expanded=False):
                # Differences in dataset
                st.write("""Data differences from existing dataset...""")
                are_different, text = fast_import.data_diff()
                st.markdown(
                    f'<iframe srcdoc="{text}" width="100%" style="border: 1px solid black; background: white"></iframe>',
                    unsafe_allow_html=True,
                )

                # Differences in metadata files
                st.write("""Metadata differences from existing dataset...""")
                are_different, text = fast_import.metadata_diff()
                if are_different:
                    st.markdown(
                        text,
                        unsafe_allow_html=True,
                    )
                else:
                    st.success(text)
            st.form_submit_button(
                "Continue",
                type="primary",
                use_container_width=True,
                on_click=lambda: set_states({"to_be_submitted_confirmed_2": True}),
            )
    else:
        set_states({"to_be_submitted_confirmed_2": True})

    return fast_import


def _convert_percentages(data: pd.DataFrame, variables_meta_dict: Dict[str, VariableMeta]) -> pd.DataFrame:
    """Convert percentages to numbers."""
    for col in data.columns:
        if getattr(variables_meta_dict.get(col, {}), "unit", "") == "%":
            # Remove % symbol first
            cleaned_values = data[col].astype(str).str.replace("%", "")

            # Try to convert to float and check for invalid values
            try:
                numeric_values = pd.to_numeric(cleaned_values, errors="coerce")
                # Check if any values couldn't be converted (excluding NaN from original data)
                original_nulls = data[col].isna()
                conversion_failures = numeric_values.isna() & ~original_nulls

                if conversion_failures.any():
                    failed_values = cleaned_values[conversion_failures].unique()
                    raise ValidationError(
                        f"Column '{col}' contains percentage values that cannot be converted to numbers: {list(failed_values)}. "
                        "Please ensure all percentage values are numeric (e.g., '5.2%' not '<0.1%')."
                    )

                data[col] = numeric_values
            except ValidationError:
                raise
            except Exception as e:
                raise ValidationError(f"Error converting percentages in column '{col}': {str(e)}")

    return data


def _clean_numeric_formatting(data: pd.DataFrame, variables_meta_dict: Dict[str, VariableMeta]) -> pd.DataFrame:
    """Clean numeric formatting issues like commas, extra spaces, etc."""
    data = data.copy()

    for col in data.columns:
        if col.startswith("dim_") or col in ["country", "year"]:
            continue  # Skip dimension columns and standard index columns

        non_null_values = data[col].dropna()
        if len(non_null_values) == 0:
            continue

        # Try to clean and convert all values to numeric
        cleaned_values = data[col].astype(str).str.replace(r"[,\s]+", "", regex=True)
        numeric_values = pd.to_numeric(cleaned_values, errors="coerce")

        # Count successful conversions
        successful_conversions = numeric_values.notna().sum() - data[col].isna().sum()

        if successful_conversions > 0:
            # Replace the original values with cleaned numeric values where conversion succeeded
            data[col] = data[col].where(data[col].isna(), numeric_values)
            st.info(f"Cleaned numeric formatting in column '{col}' (converted {successful_conversions} values)")

    return data


def _validate_data(df: pd.DataFrame, variables_meta_dict: Dict[str, VariableMeta]) -> bool:
    errors = []

    # check column names
    for col in df.columns:
        try:
            validate_underscore(col, "Variables")
        except NameError as e:
            errors.append(ValidationError(e))

    # missing columns in metadata
    for col in set(df.columns) - set(variables_meta_dict.keys()):
        errors.append(ValidationError(f"Variable {col} is not defined in metadata"))

    # extra columns in metadata
    for col in set(variables_meta_dict.keys()) - set(df.columns):
        errors.append(ValidationError(f"Variable {col} in metadata is not in the data"))

    # missing titles
    for col in df.columns:
        if col in variables_meta_dict and not variables_meta_dict[col].title:
            errors.append(ValidationError(f"Variable {col} is missing title (you can use its short name)"))

    # check for mixed data types
    for col in df.columns:
        if col.startswith("dim_") or col in ["country", "year"]:
            continue  # Skip dimension columns and standard index columns

        non_null_values = df[col].dropna()
        if len(non_null_values) == 0:
            continue

        # Check if column has mixed numeric and non-numeric values
        numeric_mask = pd.to_numeric(non_null_values, errors="coerce").notna()
        numeric_count = numeric_mask.sum()
        total_count = len(non_null_values)

        # If some but not all values are numeric, it's mixed
        if 0 < numeric_count < total_count:
            non_numeric_examples = non_null_values[~numeric_mask].unique()[:3]
            error_msg = f"Variable {col} contains mixed data types ({numeric_count}/{total_count} values are numeric). "
            error_msg += f"Non-numeric examples: {list(non_numeric_examples)}. "
            error_msg += "Please ensure the column contains either all numeric values or all text values."
            errors.append(ValidationError(error_msg))

    # no inf values
    for col in df.select_dtypes("number").columns:
        if col in df.columns and np.isinf(df[col].abs().max()):
            errors.append(ValidationError(f"Variable {col} has inf values"))

    if errors:
        for error in errors:
            st.exception(error)
        return False
    else:
        return True


def _harmonize_countries(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Check if all countries are harmonized.

    TODO: Vectorise this function.
    """
    # Read the main table of the regions dataset.
    tb_regions = Dataset(LATEST_REGIONS_DATASET_PATH)["regions"][["name", "aliases", "iso_alpha2", "iso_alpha3"]]

    # First convert ISO2 and ISO3 country codes.
    df = df.reset_index()
    for iso_col in ["iso_alpha2", "iso_alpha3"]:
        df["country"] = df["country"].replace(tb_regions.set_index(iso_col)["name"])
        # lowercase
        df["country"] = df["country"].replace(
            tb_regions.assign(**{iso_col: tb_regions.iso_alpha2.str.lower()}).set_index(iso_col)["name"]
        )

    # Convert strings of lists of aliases into lists of aliases.
    tb_regions["aliases"] = [json.loads(alias) if pd.notnull(alias) else [] for alias in tb_regions["aliases"]]

    # Explode list of aliases to have one row per alias.
    tb_regions = tb_regions.explode("aliases").reset_index(drop=True)

    # Create a series that maps aliases to country names.
    alias_to_country = tb_regions.rename(columns={"aliases": "alias"}).set_index("alias")["name"]

    unknown_countries = []

    for country in set(df.country):
        # country is in reference dataset
        if country in alias_to_country.values:
            continue

        # there is an alias for this country
        elif country in alias_to_country.index:
            df.country = df.country.replace({country: alias_to_country[country]})
            st.warning(f"Country `{country}` harmonized to `{alias_to_country.loc[country]}`")

        # unknown country
        else:
            unknown_countries.append(country)

    df.set_index(["country", "year"], inplace=True)

    return df, unknown_countries


def _infer_metadata(
    data: pd.DataFrame, meta_variables: Dict[str, VariableMeta]
) -> Tuple[pd.DataFrame, Dict[str, VariableMeta]]:
    """Infer metadata."""
    # underscore variable names from data sheet, this doesn't raise warnings
    for col in data.columns:
        data = data.rename(columns={col: underscore(col)})

    # underscore short names from metadata, raise warning if they don't match
    for short_name in list(meta_variables.keys()):
        try:
            validate_underscore(short_name, "Variables")
        except NameError:
            new_short_name = underscore(short_name)
            st.warning(
                st.markdown(
                    f"`{short_name}` isn't in [snake_case](https://en.wikipedia.org/wiki/Snake_case) format and was renamed to `{new_short_name}`. Please update it in your sheet `variables_meta`."
                )
            )
            meta_variables[new_short_name] = meta_variables.pop(short_name)

    return data, meta_variables
