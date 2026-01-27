"""Garden step for Meijer et al. (2021) - Process and harmonize country data."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Process meadow data and harmonize country names."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("meijer_2021")

    # Read table from meadow dataset.
    tb = ds_meadow.read("meijer_2021")

    #
    # Process data.
    #
    tb = tb[["country", "me_tons_per_year", "p_e_percent"]]

    # Add year column (data represents a snapshot from the study period)
    tb["year"] = 2019

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Add population data for per capita calculations
    tb = geo.add_population_to_table(tb, ds_population=paths.load_dataset("population"))

    # Calculate per capita plastic emissions (kg per person)
    tb["me_tons_per_year_per_capita"] = (tb["me_tons_per_year"] * 1000) / tb["population"]

    # Calculate share of global total
    global_total = tb["me_tons_per_year"].sum()
    tb["me_tons_per_year_share_of_global"] = (tb["me_tons_per_year"] / global_total) * 100

    # Drop population column (not needed in output)
    tb = tb.drop(columns=["population"])

    # Set index and sort.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()
