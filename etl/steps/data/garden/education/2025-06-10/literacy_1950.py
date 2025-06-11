"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("literacy_1950")

    # Read table from meadow dataset.
    tb = ds_meadow.read("literacy_1950")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Split the illiteracy_rate into two numeric columns
    tb[["ill_low", "ill_high"]] = tb["illiteracy_rate"].str.split("to", expand=True).astype(float)
    # Calculate the middle estimate (mean) of illiteracy
    tb["illiteracy_est"] = tb[["ill_low", "ill_high"]].mean(axis=1)
    tb["illiteracy_est"] = tb["illiteracy_est"].copy_metadata(tb["illiteracy_rate"])

    # Calculate literacy as 100 - illiteracy
    tb["literacy_est"] = 100 - tb["illiteracy_est"]
    tb["literacy_est"] = tb["literacy_est"].copy_metadata(tb["illiteracy_rate"])
    tb = tb.drop(columns=["illiteracy_rate", "ill_low", "ill_high"])

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
