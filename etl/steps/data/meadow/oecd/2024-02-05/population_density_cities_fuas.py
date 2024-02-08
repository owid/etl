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

    tb_2020 = tb[
        (tb["year"] == 2020)
        & (tb["unit_of_measure"] == "Persons per square kilometer")
        & (tb["territorial_level"] == "City")
    ]
    top_100_cities = tb_2020.nlargest(10, "value")

    tb = tb.underscore().set_index(
        ["ref_area", "reference_area", "year", "unit_of_measure", "territorial_level"], verify_integrity=True
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
