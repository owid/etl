"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

cause_renaming_dict = {
    "Adverse effects of medical treatment": "Medical accidents",
    "Animal contact": "Animal attacks",
    "Conflict and terrorism": "Conflict",
    "Congenital heart anomalies": "Heart abnormalities",
    "Digestive congenital anomalies": "Digestive defects",
    "Fire, heat, and hot substances": "Fire & temperature",
    "Hemolytic disease and other neonatal jaundice": "Hemolytic disorders",
    "Invasive Non-typhoidal Salmonella (iNTS)": "Invasive non-typhoidal salmonella",
    "Neonatal encephalopathy due to birth asphyxia and trauma": "Neonatal asphyxia (suffocation) and trauma",
    "Neonatal preterm birth": "Preterm birth",
    "Neonatal sepsis and other neonatal infections": "Neonatal sepsis & other infections",
    "Neoplasms": "Cancers",
    "Neural tube defects": "Neural tube disorders",
    "Nutritional deficiencies": "Malnutrition",
    "Other neonatal disorders": "Other birth disorders",
    "Pertussis": "Whooping cough",
    "Pulmonary aspiration and foreign body in airway": "Suffocation by objects",
    "Sexually transmitted infections excluding HIV": "Syphilis and other STDs",
    "Transport injuries": "Transport accidents",
    "Respiratory infections and tuberculosis excluding Tuberculosis": "Respiratory infections",
}

broad_cause_dict = {
    "Animal attacks": "Injuries",
    "Cancers": "Non-communicable diseases",
    "Conflict": "Injuries",
    "Diarrheal diseases": "Infectious diseases",
    "Digestive defects": "Birth disorders",
    "Drowning": "Injuries",
    "Falls": "Injuries",
    "Fire & temperature": "Injuries",
    "HIV/AIDS": "Infectious diseases",
    "Heart abnormalities": "Birth disorders",
    "Hemolytic disorders": "Birth disorders",
    "Invasive non-typhoidal salmonella": "Infectious diseases",
    "Malaria": "Infectious diseases",
    "Malnutrition": "Non-communicable diseases",
    "Measles": "Infectious diseases",
    "Medical accidents": "Injuries",
    "Neonatal asphyxia (suffocation) and trauma": "Birth disorders",
    "Neonatal sepsis & other infections": "Birth disorders",
    "Neural tube disorders": "Birth disorders",
    "Other birth disorders": "Birth disorders",
    "Other infectious diseases": "Infectious diseases",
    "Other injuries": "Injuries",
    "Other non-communicable diseases": "Non-communicable diseases",
    "Poisonings": "Injuries",
    "Preterm birth": "Birth disorders",
    "Pulmonary aspiration and foreign body in airway": "Injuries",
    "Respiratory infections": "Infectious diseases",
    "Sudden infant death syndrome": "Non-communicable diseases",
    "Syphilis and other STDs": "Infectious diseases",
    "Transport accidents": "Injuries",
    "Tuberculosis": "Infectious diseases",
    "Whooping cough": "Infectious diseases",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_child_treemap")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_child_treemap"].reset_index()
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Drop the measure column
    tb = tb.drop(columns="measure")
    # Drop the really high level causes
    tb = tb[
        ~tb["cause"].isin(
            [
                "All causes",
                "Communicable, maternal, neonatal, and nutritional diseases",
                "Non-communicable diseases",
                "Injuries",
            ]
        )
    ]

    # Reaggregate causes
    tb = reaggregate_causes(tb)
    # Rename causes
    tb = rename_causes(tb=tb, cause_renaming_dict=cause_renaming_dict, broad_cause_dict=broad_cause_dict)
    # Check for duplicates
    # Format the tables
    tb = tb.format(["country", "age", "broad_cause", "cause", "metric", "year"], short_name="gbd_child_treemap")

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

    """
    tb = pull_out_cause(tb, pull_out_cause="Tuberculosis", aggregate_cause="Respiratory infections and tuberculosis")
    tb = pull_out_cause(
        tb,
        pull_out_cause=["Diarrheal diseases", "Invasive Non-typhoidal Salmonella (iNTS)"],
        aggregate_cause="Enteric infections",
    )
    tb = pull_out_cause(tb, pull_out_cause="Malaria", aggregate_cause="Neglected tropical diseases and malaria")
    tb = pull_out_cause(
        tb, pull_out_cause=["Pertussis", "Measles", "Meningitis"], aggregate_cause="Other infectious diseases"
    )
    tb = pull_out_cause(
        tb,
        pull_out_cause=[
            "Conflict and terrorism"
        ],  # Self-harm is also technically in this group but it's empty for children
        aggregate_cause="Self-harm and interpersonal violence",
        residual_name="Violence",
    )
    tb = pull_out_cause(
        tb,
        pull_out_cause=[
            "Neural tube defects",
            "Digestive congenital anomalies",
            "Congenital heart anomalies",
            "Sudden infant death syndrome",
        ],
        aggregate_cause="Other non-communicable diseases",
    )
    tb = pull_out_cause(
        tb,
        pull_out_cause=[
            "Drowning",
            "Pulmonary aspiration and foreign body in airway",
            "Fire, heat, and hot substances",
            "Falls",
            "Poisonings",
            "Adverse effects of medical treatment",
            "Animal contact",
        ],
        aggregate_cause="Unintentional injuries",
        residual_name="Other injuries",
    )
    tb = combine_causes(
        tb=tb,
        causes_to_combine=[
            "Cardiovascular diseases",
            "Chronic respiratory diseases",
            "Digestive diseases",
            "Neurological disorders",
            "Substance use disorders",
            "Diabetes and kidney diseases",
            "Skin and subcutaneous diseases",
            "Musculoskeletal disorders",
            "Other non-communicable diseases excluding Neural tube defects excluding Digestive congenital anomalies excluding Congenital heart anomalies excluding Sudden infant death syndrome",
        ],
        new_cause_name="Other non-communicable diseases",
    )

    tb = combine_causes(
        tb=tb,
        causes_to_combine=[
            "Other infectious diseases excluding Pertussis excluding Measles excluding Meningitis",
            "Neglected tropical diseases and malaria excluding Malaria",
            "Enteric infections excluding Diarrheal diseases excluding Invasive Non-typhoidal Salmonella (iNTS)",
        ],
        new_cause_name="Other infectious diseases",
    )

    return tb


def rename_causes(tb: Table, cause_renaming_dict: dict[str, str], broad_cause_dict: dict[str, str]) -> Table:
    """
    Rename causes based on a provided mapping dictionary.

    Args:
        tb: Input table with cause data
        cause_renaming_dict: Dictionary mapping old cause names to new cause names

    Returns:
        Table with renamed causes
    """
    tb["cause"] = tb["cause"].replace(cause_renaming_dict, regex=False)
    tb["broad_cause"] = tb["cause"].map(broad_cause_dict)
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


def pull_out_cause(
    tb: Table, pull_out_cause: str | list[str], aggregate_cause: str, residual_name: str | None = None
) -> Table:
    """
    Pull out one or more specific causes from a broader cause category and create a residual category.

    Example 1 (single cause): Pull out "Malaria" from "Neglected tropical diseases and malaria"
    to create:
    1. "Malaria" (kept as-is)
    2. "Neglected tropical diseases and malaria excluding Malaria" (residual)
    3. All other causes (unchanged)

    Example 2 (multiple causes with custom name):
    Pull out ["HIV/AIDS", "Tuberculosis"] from "Combined infectious diseases" with residual_name="Other infectious diseases"
    to create:
    1. "HIV/AIDS" (kept as-is)
    2. "Tuberculosis" (kept as-is)
    3. "Other infectious diseases" (residual with custom name)
    4. All other causes (unchanged)

    Args:
        tb: Input table with cause data
        pull_out_cause: Specific cause(s) to extract (e.g., "Malaria" or ["HIV/AIDS", "Tuberculosis"])
        aggregate_cause: Broader category to subtract from (e.g., "Neglected tropical diseases and malaria")
        residual_name: Optional custom name for the residual category. If None, uses auto-generated
                      "aggregate_cause excluding cause1 excluding cause2..." format

    Returns:
        Table with the pulled-out cause(s) and residual category
    """
    # Convert single cause to list for uniform handling
    pull_out_causes = [pull_out_cause] if isinstance(pull_out_cause, str) else pull_out_cause

    # Validate that all causes exist in the data
    unique_causes = set(tb["cause"].unique())
    for cause in pull_out_causes:
        if cause not in unique_causes:
            raise ValueError(f"Cause '{cause}' not found in data. Available causes: {sorted(unique_causes)}")
    if aggregate_cause not in unique_causes:
        raise ValueError(f"Cause '{aggregate_cause}' not found in data. Available causes: {sorted(unique_causes)}")

    # Define merge keys for clarity
    merge_keys = ["country", "year", "metric", "age"]

    # Extract the aggregate subset
    tb_aggregate = tb[tb["cause"] == aggregate_cause].reset_index(drop=True).copy()

    # Validate we have data for aggregate cause
    if len(tb_aggregate) == 0:
        raise ValueError(f"No data found for cause '{aggregate_cause}'")

    # Initialize residual with aggregate values
    tb_residual = tb_aggregate.copy()
    tb_residual = tb_residual.rename(columns={"value": "residual_value"})

    # Subtract each specific cause from the aggregate
    for cause in pull_out_causes:
        tb_specific = tb[tb["cause"] == cause].reset_index(drop=True)

        # Validate we have data for this cause
        if len(tb_specific) == 0:
            raise ValueError(f"No data found for cause '{cause}'")

        # Merge and subtract
        tb_residual = pr.merge(
            tb_residual,
            tb_specific[merge_keys + ["value"]],
            how="left",
            on=merge_keys,
            suffixes=("", f"_{cause}"),
        )
        tb_residual["residual_value"] = tb_residual["residual_value"] - tb_residual["value"].fillna(0)
        tb_residual = tb_residual.drop(columns=["value"])

    # Validate that residual values are non-negative
    negative_residuals = tb_residual[tb_residual["residual_value"] < 0]
    if len(negative_residuals) > 0:
        causes_str = "', '".join(pull_out_causes)
        raise ValueError(
            f"Found {len(negative_residuals)} negative residuals when subtracting '{causes_str}' "
            f"from '{aggregate_cause}'. This suggests these causes are not properly nested within '{aggregate_cause}'."
        )

    # Set the new cause name for residual
    if residual_name is not None:
        tb_residual["cause"] = residual_name
    else:
        exclusions = " excluding ".join([""] + pull_out_causes)
        tb_residual["cause"] = f"{aggregate_cause}{exclusions}"
    tb_residual = tb_residual.rename(columns={"residual_value": "value"})

    # Keep only necessary columns
    tb_residual = tb_residual[merge_keys + ["cause", "value"]]

    # Combine: all other causes (excluding aggregate) + residual cause
    tb_other = tb[~tb["cause"].isin([aggregate_cause])].reset_index(drop=True)
    tb_result = pr.concat([tb_other, tb_residual], ignore_index=True)

    return tb_result
