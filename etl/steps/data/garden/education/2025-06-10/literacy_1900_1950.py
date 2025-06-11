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
    ds_meadow = paths.load_dataset("literacy_1900_1950")

    # Read table from meadow dataset.
    tb = ds_meadow.read("literacy_1900_1950")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb[tb["sex"] == "MF"]

    # Remove "*" from illiteracy_rate
    tb["illiteracy_rate"] = tb["illiteracy_rate"].astype(str).str.replace("*", "", regex=False)
    tb["illiteracy_rate"] = tb["illiteracy_rate"].astype(float)

    # Exclude specific data points where we have age group data that better approximates 15+ for these countries and years.
    tb = tb[~((tb["country"] == "Argentina") & (tb["year"] == 1914) & (tb["age"] == "7+"))]
    tb = tb[~((tb["country"] == "Canada") & (tb["year"] == 1921) & (tb["age"] == "5+"))]
    tb = tb.drop(columns=["sex"])
    tb["literacy_rate"] = 100 - tb["illiteracy_rate"]

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
