"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "Unit of measure": "indicator",
    "Expenditure source": "expenditure_source",
    "Spending type": "spending_type",
    "PROGRAMME_TYPE": "programme_type",
    "OBS_VALUE": "value",
    "Observation status": "status",
}

# Define programme type codes and its main category
PROGRAMME_TYPE_CODES = {
    "_T": {"new_name": "Total", "category": "Total"},
    "TP01": {"new_name": "Total", "category": "Old age and survivors"},
    "TP11": {"new_name": "Total", "category": "Old age"},
    "TP111": {"new_name": "Pension", "category": "Old age"},
    "TP112": {"new_name": "Early retirement pension", "category": "Old age"},
    "TP113": {"new_name": "Other cash benefits", "category": "Old age"},
    "TP121": {"new_name": "Residential care and home-help services", "category": "Old age"},
    "TP122": {"new_name": "Other benefits in kind", "category": "Old age"},
    "TP21": {"new_name": "Total", "category": "Survivors"},
    "TP211": {"new_name": "Pension", "category": "Survivors"},
    "TP212": {"new_name": "Other cash benefits", "category": "Survivors"},
    "TP221": {"new_name": "Funeral expenses", "category": "Survivors"},
    "TP222": {"new_name": "Other benefits in kind", "category": "Survivors"},
    "TP31": {"new_name": "Total", "category": "Incapacity related"},
    "TP311": {"new_name": "Disability pensions", "category": "Incapacity related"},
    "TP312": {"new_name": "Pensions (occupational injury and disease)", "category": "Incapacity related"},
    "TP313": {"new_name": "Paid sick leave (occupational injury and disease)", "category": "Incapacity related"},
    "TP314": {"new_name": "Paid sick leave (other sickness daily allowances)", "category": "Incapacity related"},
    "TP315": {"new_name": "Other cash benefits", "category": "Incapacity related"},
    "TP321": {"new_name": "Residential care and home-help services", "category": "Incapacity related"},
    "TP322": {"new_name": "Rehabilitation services", "category": "Incapacity related"},
    "TP323": {"new_name": "Other benefits in kind", "category": "Incapacity related"},
    "TP41": {"new_name": "Total", "category": "Health"},
    "TP51": {"new_name": "Total", "category": "Family"},
    "TP511": {"new_name": "Family allowances", "category": "Family"},
    "TP512": {"new_name": "Maternity and parental leave", "category": "Family"},
    "TP513": {"new_name": "Other cash benefits", "category": "Family"},
    "TP521": {"new_name": "Early childhood education and care (ECEC)", "category": "Family"},
    "TP522": {"new_name": "Home help and accommodation", "category": "Family"},
    "TP523": {"new_name": "Other benefits in kind", "category": "Family"},
    "TP60": {"new_name": "Total", "category": "Active labor market programs"},
    "TP601": {"new_name": "PES and Administration", "category": "Active labor market programs"},
    "TP602": {"new_name": "Training", "category": "Active labor market programs"},
    "TP603": {"new_name": "Job rotation and job sharing", "category": "Active labor market programs"},
    "TP604": {"new_name": "Employment incentives", "category": "Active labor market programs"},
    "TP605": {"new_name": "Supported employment and rehabilitation", "category": "Active labor market programs"},
    "TP606": {"new_name": "Direct job creation", "category": "Active labor market programs"},
    "TP607": {"new_name": "Start-up incentives", "category": "Active labor market programs"},
    "TP71": {"new_name": "Total", "category": "Unemployment"},
    "TP711": {"new_name": "Unemployment compensation and severance pay", "category": "Unemployment"},
    "TP712": {"new_name": "Early retirement for labor market reasons", "category": "Unemployment"},
    "TP82": {"new_name": "Total", "category": "Housing"},
    "TP821": {"new_name": "Housing assistance", "category": "Housing"},
    "TP822": {"new_name": "Other benefits in kind", "category": "Housing"},
    "TP91": {"new_name": "Total", "category": "Other social policy areas"},
    "TP911": {"new_name": "Income maintenance", "category": "Other social policy areas"},
    "TP912": {"new_name": "Other cash benefits", "category": "Other social policy areas"},
    "TP921": {"new_name": "Social assistance", "category": "Other social policy areas"},
    "TP922": {"new_name": "Other benefits in kind", "category": "Other social policy areas"},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("social_expenditure.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Keep only the columns of interest.
    tb = tb[COLUMNS_TO_KEEP.keys()]

    # Rename columns.
    tb = tb.rename(columns=COLUMNS_TO_KEEP)

    # Map indicator codes to their descriptions and create a new column called programme_type_category
    for code, mapping in PROGRAMME_TYPE_CODES.items():
        tb.loc[tb["programme_type"] == code, "programme_type_category"] = mapping["category"]
        tb.loc[tb["programme_type"] == code, "programme_type"] = mapping["new_name"]

    # Drop rows with missing values.
    tb = tb.dropna(subset=["value"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb.format(
            [
                "country",
                "year",
                "indicator",
                "expenditure_source",
                "spending_type",
                "programme_type_category",
                "programme_type",
            ]
        )
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
