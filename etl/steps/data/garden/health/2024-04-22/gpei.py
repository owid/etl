"""Load a meadow dataset and create a garden dataset."""


from etl.data_helpers.geo import harmonize_countries
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    snap = paths.load_snapshot("gpei.csv")
    tb = snap.read()
    tb = harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Set an index and sort.
    tb = tb.format()
    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
