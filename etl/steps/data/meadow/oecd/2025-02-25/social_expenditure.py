"""Load a snapshot and create a meadow dataset."""

from owid.datautils.dataframes import map_series

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

PROGRAMME_TYPE_CODES = {
    "_T": "Total",
    "TP01": "Old age and survivors",
    "TP11": "Old age",
    "TP111": "Pension - Old age",
    "TP112": "Early retirement pension",
    "TP113": "Other cash benefits - Old age",
    "TP121": "Residential care / Home - Old age",
    "TP122": "Other benefits in kind - Old age",
    "TP21": "Survivors",
    "TP211": "Pension - Survivors",
    "TP212": "Other cash benefits - Survivors",
    "TP221": "Funeral expenses",
    "TP222": "Other benefits in kind - Survivors",
    "TP31": "Incapacity related",
    "TP311": "Disability pensions",
    "TP312": "Pensions (occupational injury and disease)",
    "TP313": "Paid sick leave (occupational injury and disease)",
    "TP314": "Paid sick leave (other sickness daily allowances)",
    "TP315": "Other cash benefits - Incapacity related",
    "TP321": "Residential care / Home - Incapacity related",
    "TP322": "Rehabilitation services",
    "TP323": "Other benefits in kind - Incapacity related",
    "TP41": "Health",
    "TP51": "Family",
    "TP511": "Family allowances",
    "TP512": "Maternity and parental leave",
    "TP513": "Other cash benefits - Family",
    "TP521": "Early childhood education and care (ECEC)",
    "TP522": "Home help / Accomodation",
    "TP523": "Other benefits in kind - Family",
    "TP60": "Active labour market programmes",
    "TP601": "PES and Administration",
    "TP602": "Training",
    "TP603": "Job Rotation and Job Sharing",
    "TP604": "Employment Incentives",
    "TP605": "Supported Employment and Rehabilitation",
    "TP606": "Direct Job Creation",
    "TP607": "Start",
    "TP71": "Unemployment",
    "TP711": "Unemployment compensation / severance pay",
    "TP712": "Early retirement for labour market reasons",
    "TP82": "Housing",
    "TP821": "Housing assistance",
    "TP822": "Other benefits in kind - Housing",
    "TP91": "Other social policy areas",
    "TP911": "Income maintenance",
    "TP912": "Other cash benefits - Other social policy areas",
    "TP921": "Social assistance",
    "TP922": "Other benefits in kind - Other social policy areas",
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

    # Map programme type codes to their descriptions.
    tb["programme_type"] = map_series(
        series=tb["programme_type"],
        mapping=PROGRAMME_TYPE_CODES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=False,
    )

    tb["programme_type"] = tb["programme_type"].copy_metadata(tb["country"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb.format(
            [
                "country",
                "year",
                "indicator",
                "expenditure_source",
                "spending_type",
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
