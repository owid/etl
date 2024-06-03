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
    ds = paths.load_dataset("wdi")
    tb = ds["wdi"].reset_index()
    # Load population dataset
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Harmonize country names (income countries)
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Add population
    tb = geo.add_population_to_table(tb, ds_population)

    # Create indicator
    tb["tourists_per_1000"] = tb["st_int_dprt"] / tb["population"] * 1000

    # Keep relevant columns
    COLUMN_INDEX = ["country", "year"]
    tb = tb[COLUMN_INDEX + ["tourists_per_1000"]]

    # Formatting
    tb = tb.format(COLUMN_INDEX, short_name="tourists_per_1000")

    # Drop NaN
    tb = tb.dropna()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
