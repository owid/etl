"""TODO: Explain this step.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Harris et al. (2015) dataset and read its main table.
    ds_harris = paths.load_dataset("harris_et_al_2015")
    tb_harris = ds_harris["harris_et_al_2015"].reset_index()

    #
    # Process data.
    #
    # TODO: Continue processing.
    tb = tb_harris.copy()

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
