"""Load a meadow dataset and create a garden dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.

    snap = paths.load_snapshot("epoch_llms.csv")

    # Load data from snapshot.
    tb = snap.read()
    tb["training_computation_petaflop"] = tb["Approx Compute (FLOP)"] / 1e15
    tb = tb.drop("Approx Compute (FLOP)", axis=1)
    tb["MMLU avg"] *= 100
    tb["Architecture"] = tb["Architecture"].str.replace("Llama", "LLaMA", regex=True)
    tb["Organisation"] = tb["Organisation"].str.replace("DeepMind", "Google DeepMind", regex=True)

    tb = tb.underscore().set_index(["architecture", "year"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
