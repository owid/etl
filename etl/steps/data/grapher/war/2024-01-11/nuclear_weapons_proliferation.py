"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Labels to be used on the status column.
# NOTE: They should be identical to the ones used in the garden step.
LABEL_DOES_NOT_CONSIDER = "Does not consider"
LABEL_CONSIDERS = "Considers"
LABEL_PURSUES = "Pursues"
LABEL_POSSESSES = "Possesses"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("nuclear_weapons_proliferation")
    tb = ds_garden["nuclear_weapons_proliferation"]

    #
    # Process data.
    #
    # Ensure that the status column contains only the labels defined above.
    error = "The status column contains values other than the ones defined above."
    assert set(tb["status"]) == {LABEL_DOES_NOT_CONSIDER, LABEL_CONSIDERS, LABEL_PURSUES, LABEL_POSSESSES}, error

    # Map labels to numbers from 0 to 3.
    tb["status"] = tb["status"].map(
        {
            LABEL_DOES_NOT_CONSIDER: 0,
            LABEL_CONSIDERS: 1,
            LABEL_PURSUES: 2,
            LABEL_POSSESSES: 3,
        }
    )

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
