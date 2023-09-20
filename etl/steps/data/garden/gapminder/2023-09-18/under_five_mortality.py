"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("under_five_mortality")

    # Read table from meadow dataset.
    tb = ds_meadow["under_five_mortality"].reset_index()

    # Remove rows with missing values.
    tb = tb.dropna(subset=["under_five_mortality"])

    # Remove guesstimated values.
    tb = tb[~tb["source"].str.contains("Guesstimate")]

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Create a filtered version which doesn't have 'Model based on Life Expectancy' as source

    tb_sel = tb[tb["source"] != "Model based on Life Expectancy"]

    tb = tb.set_index(["country", "year"], verify_integrity=True)
    tb_sel = tb_sel.set_index(["country", "year"], verify_integrity=True)
    tb_sel.metadata.short_name = "under_five_mortality_selected"
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_sel], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
