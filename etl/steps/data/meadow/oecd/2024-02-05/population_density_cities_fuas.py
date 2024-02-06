"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("population_density_cities_fuas.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #

    for col in tb.columns:
        print(col)
        print(tb[col].unique())
    cols_to_select = ["REF_AREA", "Reference area", "Unit of measure", "Territorial level", "TIME_PERIOD", "OBS_VALUE"]
    tb = tb[cols_to_select]

    tb = tb.rename(
        columns={
            "Reference area": "reference_area",
            "Unit of measure": "unit_of_measure",
            "Territorial level": "territorial_level",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
        }
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = (
        tb.underscore()
        .set_index(["ref_area", "reference_area", "year", "unit_of_measure", "territorial_level"])
        .sort_index()
    )
    # Python
    duplicated_index = tb.index.duplicated(keep=False)
    print(tb[duplicated_index][:20])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
