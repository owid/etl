"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load the data and dictionary from the snapshot.
    snap_data = paths.load_snapshot("wittgenstein_center_data.csv")
    snap_dictionary = paths.load_snapshot("wittgenstein_center_dictionary.csv")
    tb = snap_data.read()
    tb_dict = snap_dictionary.read()

    # Create a mapping dictionary for each variable from the recode dictionary.
    mapping_dict = {}
    for var in tb_dict["var"].unique():
        var_dict = dict(zip(tb_dict[tb_dict["var"] == var]["varval"], tb_dict[tb_dict["var"] == var]["varvaldesc"]))
        mapping_dict[var] = var_dict

    # Convert age to string to be able to apply the mapping.
    tb["agest"] = tb["agest"].astype(str)

    # Apply the mapping to the data.
    for col in ["region", "edu", "agest"]:
        if col in tb.columns:
            tb[col] = tb[col].map(mapping_dict[col])

    # Select and rename columns.
    tb = tb[["region", "Time", "sex", "edu", "agest", "pop"]]
    tb = tb.rename(
        columns={
            "region": "country",
            "Time": "year",
            "edu": "educational_attainment",
            "agest": "age_group",
            "pop": "population",
        }
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = (
        tb.underscore()
        .set_index(["country", "year", "educational_attainment", "sex", "age_group"], verify_integrity=True)
        .sort_index()
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_data.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
