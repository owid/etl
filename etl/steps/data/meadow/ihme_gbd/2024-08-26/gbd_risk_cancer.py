"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gbd_risk_cancer.feather")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    # standardize column names
    tb = clean_data(tb)
    # fix percent values - they aren't consistently presented as either 0-1, or 0-100.
    tb = fix_percent(tb)
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    if all(tb["sex"] == "Both"):
        tb = tb.drop(columns="sex")
    cols = tb.columns.drop("value").to_list()
    tb = tb.format(cols)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def clean_data(tb: Table) -> Table:
    tb = tb.rename(
        columns={
            "location_name": "country",
            "location": "country",
            "val": "value",
            "measure_name": "measure",
            "sex_name": "sex",
            "age_name": "age",
            "cause_name": "cause",
            "metric_name": "metric",
            "rei_name": "rei",
        },
        errors="ignore",
    )
    tb = tb.drop(
        columns=["measure_id", "location_id", "sex_id", "age_id", "cause_id", "metric_id", "rei_id", "upper", "lower"],
        errors="ignore",
    )

    return tb


def fix_percent(tb: Table) -> Table:
    """
    IHME doesn't seem to be consistent with how it stores percentages.
    If the maximum percent value for any cause of death is less than or equal 1,
    it indicates all values are 100x too small and we need to multiply values by 100
    """
    if "Percent" in tb["metric"].unique():
        if max(tb["value"][tb["metric"] == "Percent"]) <= 1.1:
            subset_percent = tb["metric"] == "Percent"
            tb.loc[subset_percent, "value"] *= 100
            # tb["value"][(tb["metric"] == "Percent")] = tb["value"][(tb["metric"] == "Percent")] * 100
    return tb
