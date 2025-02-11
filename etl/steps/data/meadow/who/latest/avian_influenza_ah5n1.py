"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tb = paths.read_snap_table("avian_influenza_ah5n1.csv")  # safe_types=False

    #
    # Process data.
    #
    # Drop NAs
    tb = tb.dropna(subset="Range")

    # Unpivot
    tb = tb.melt(id_vars=["Range", "Month"], var_name="country", value_name="avian_cases")

    # Remove unnamed
    tb = tb[~tb["country"].str.contains("Unnamed")]

    # Dtypes
    tb["avian_cases"] = tb["avian_cases"].astype("int")

    # Create a new table and ensure all columns are snake-case.
    tb = tb.format(["range", "month", "country"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()
