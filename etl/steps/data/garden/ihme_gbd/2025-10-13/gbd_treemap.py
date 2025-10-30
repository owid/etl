"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

cause_renaming_dict = {
    "Cardiovascular diseases": "Heart diseases",
    "Neoplasms": "Cancers",
    "Respiratory infections and tuberculosis excluding Tuberculosis": "Pneumonia",
    "Self-harm": "Suicide",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_treemap")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_treemap"].reset_index()
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Drop the measure column
    tb = tb.drop(columns="measure")

    # Reaggregate causes
    tb = reaggregate_causes(tb)
    # Rename causes
    tb = rename_causes(tb, cause_renaming_dict=cause_renaming_dict)
    # Check for duplicates
    # index_cols = ["country", "age", "cause", "metric", "year"]
    # duplicates = tb[tb.duplicated(subset=index_cols, keep=False)]
    # if len(duplicates) > 0:
    #    print(f"\nFound {len(duplicates)} duplicate rows:")
    #    print(duplicates.sort_values(index_cols))
    # Format the tables
    tb = tb.format(["country", "age", "cause", "metric", "year"], short_name="gbd_treemap")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def reaggregate_causes(tb: Table) -> Table:
    """
    We want to reaggregate some causes to pull out some important causes of death and to combine some smaller ones.

    Specifically, we want to:
    - Pull out Malaria from Neglected tropical diseases and malaria
    - Pull out HIV/AIDS from HIV/AIDS and sexually transmitted infections
    - Pull out Tuberculosis from Respiratory infections and tuberculosis
    - Pull out Diarrheal diseases from Enteric infections
    - Pull out Self-harm from Self-harm and interpersonal violence
    - Pull out Interpersonal violence from Self-harm and interpersonal violence excluding Self-harm #created in the previous line
    - Combine 'Musculoskeletal disorders', 'Mental disorders', 'Skin and subcutaneous diseases', 'Substance use disorders' and 'Other non-communicable diseases'  into 'Other non-communicable diseases'
    - Combine 'HIV/AIDS and sexually transmitted infections excluding HIV/AIDS','Respiratory infections and tuberculosis excluding Tuberculosis', 'Enteric infections excluding Diarrheal diseases' and 'Other infectious diseases' into 'Other infectious diseases'
    - Combine 'Unintentional injuries', 'Self-harm and interpersonal violence excluding Self-harm excluding Interpersonal violence' into 'Other injuries
    """
    tb = pull_out_cause(tb, pull_out_cause="Malaria", aggregate_cause="Neglected tropical diseases and malaria")
    tb = pull_out_cause(tb, pull_out_cause="HIV/AIDS", aggregate_cause="HIV/AIDS and sexually transmitted infections")
    tb = pull_out_cause(tb, pull_out_cause="Tuberculosis", aggregate_cause="Respiratory infections and tuberculosis")
    tb = pull_out_cause(tb, pull_out_cause="Diarrheal diseases", aggregate_cause="Enteric infections")
    tb = pull_out_cause(tb, pull_out_cause="Self-harm", aggregate_cause="Self-harm and interpersonal violence")
    tb = pull_out_cause(
        tb,
        pull_out_cause="Interpersonal violence",
        aggregate_cause="Self-harm and interpersonal violence excluding Self-harm",
    )
    # We have both maternal disorders and neonatal disorders in the data so we can remove their combined grouping
    tb = tb[tb["cause"] != "Maternal and neonatal disorders"]
    tb = combine_causes(
        tb=tb,
        causes_to_combine=[
            "Musculoskeletal disorders",
            "Mental disorders",
            "Skin and subcutaneous diseases",
            "Substance use disorders",
            "Other non-communicable diseases",
        ],
        new_cause_name="Other non-communicable diseases",
    )
    tb = combine_causes(
        tb=tb,
        causes_to_combine=[
            "HIV/AIDS and sexually transmitted infections excluding HIV/AIDS",
            "Enteric infections excluding Diarrheal diseases",
            "Neglected tropical diseases and malaria excluding Malaria",
            "Other infectious diseases",
        ],
        new_cause_name="Other infectious diseases",
    )
    tb = combine_causes(
        tb=tb,
        causes_to_combine=[
            "Unintentional injuries",
            "Self-harm and interpersonal violence excluding Self-harm excluding Interpersonal violence",
        ],
        new_cause_name="Other injuries",
    )
    return tb


def rename_causes(tb: Table, cause_renaming_dict: dict[str, str]) -> Table:
    """
    Rename causes based on a provided mapping dictionary.

    Args:
        tb: Input table with cause data
        cause_renaming_dict: Dictionary mapping old cause names to new cause names

    Returns:
        Table with renamed causes
    """
    tb["cause"] = tb["cause"].replace(cause_renaming_dict, regex=False)
    return tb


def combine_causes(tb: Table, causes_to_combine: list[str], new_cause_name: str) -> Table:
    """
    Combine multiple causes into a single new cause by summing their values.

    Args:
        tb: Input table with cause data
        causes_to_combine: List of causes to combine
        combined_cause_name: Name for the combined cause - should be one of the causes being combined e.g. Other non-communicable diseases

    Returns:
        Table with the combined cause
    """
    # Validate that all causes to combine exist
    unique_causes = set(tb["cause"].unique())
    missing_causes = [c for c in causes_to_combine if c not in unique_causes]
    if missing_causes:
        raise ValueError(f"Causes not found in data: {missing_causes}. Available causes: {sorted(unique_causes)}")

    # Define grouping keys explicitly (should match your index structure)
    groupby_cols = ["country", "year", "metric", "age"]

    # Filter rows for the causes to combine
    tb_to_combine = tb[tb["cause"].isin(causes_to_combine)].reset_index(drop=True)

    # Group by key columns and sum the values
    tb_combined = tb_to_combine.groupby(groupby_cols, as_index=False, observed=True)["value"].sum()

    # Assign the new cause name
    tb_combined["cause"] = new_cause_name

    # Remove the original causes from the table
    tb_remaining = tb[~tb["cause"].isin(causes_to_combine)].reset_index(drop=True)
    assert new_cause_name not in tb_remaining["cause"].values, (
        f"The new cause name '{new_cause_name}' already exists in the data. "
        "Please choose a different name to avoid duplicates."
    )

    # Concatenate the remaining table with the combined cause table
    tb_result = pr.concat([tb_remaining, tb_combined], ignore_index=True)

    return tb_result


def pull_out_cause(tb: Table, pull_out_cause: str, aggregate_cause: str) -> Table:
    """
    Pull out a specific cause from a broader cause category and create a residual category.

    Example: Pull out "Malaria" from "Neglected tropical diseases and malaria"
    to create three separate categories:
    1. "Malaria" (kept as-is)
    2. "Neglected tropical diseases and malaria excluding Malaria" (residual)
    3. All other causes (unchanged)

    Args:
        tb: Input table with cause data
        pull_out_cause: Specific cause to extract (e.g., "Malaria")
        aggregate_cause: Broader category to subtract from (e.g., "Neglected tropical diseases and malaria")

    Returns:
        Table with the pulled-out cause and residual category
    """
    # Validate that both causes exist in the data
    unique_causes = set(tb["cause"].unique())
    if pull_out_cause not in unique_causes:
        raise ValueError(f"Cause '{pull_out_cause}' not found in data. Available causes: {sorted(unique_causes)}")
    if aggregate_cause not in unique_causes:
        raise ValueError(f"Cause '{aggregate_cause}' not found in data. Available causes: {sorted(unique_causes)}")

    # Define merge keys for clarity
    merge_keys = ["country", "year", "metric", "age"]

    # Extract the two relevant subsets (copy to avoid SettingWithCopyWarning)
    tb_specific = tb[tb["cause"] == pull_out_cause].reset_index(drop=True)
    tb_aggregate = tb[tb["cause"] == aggregate_cause].reset_index(drop=True)

    # Validate we have data for both causes
    if len(tb_specific) == 0:
        raise ValueError(f"No data found for cause '{pull_out_cause}'")
    if len(tb_aggregate) == 0:
        raise ValueError(f"No data found for cause '{aggregate_cause}'")

    # Merge to calculate residual (aggregate - specific)
    tb_residual = pr.merge(
        tb_aggregate,
        tb_specific,
        how="left",  # Keep all aggregate rows
        on=merge_keys,
        suffixes=("_aggregate", "_specific"),
    )

    # Calculate residual value (handling potential missing data)
    tb_residual["value"] = tb_residual["value_aggregate"] - tb_residual["value_specific"].fillna(0)

    # Validate that residual values are non-negative
    negative_residuals = tb_residual[tb_residual["value"] < 0]
    if len(negative_residuals) > 0:
        raise ValueError(
            f"Found {len(negative_residuals)} negative residuals when subtracting '{pull_out_cause}' "
            f"from '{aggregate_cause}'. This suggests '{pull_out_cause}' is not properly nested within '{aggregate_cause}'."
        )

    # Set the new cause name for residual
    tb_residual["cause"] = f"{aggregate_cause} excluding {pull_out_cause}"

    # Keep only necessary columns
    tb_residual = tb_residual[merge_keys + ["cause", "value"]]

    # Combine: all other causes + residual cause
    tb_other = tb[~tb["cause"].isin([aggregate_cause])].reset_index(drop=True)
    tb_result = pr.concat([tb_other, tb_residual], ignore_index=True)

    return tb_result
