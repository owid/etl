"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# columns:
# conflict_new_displacement_rounded
# conflict_total_displacement
# conflict_total_displacement_rounded
# disaster_new_displacement
# disaster_new_displacement_rounded
# disaster_total_displacement
# disaster_total_displacement_rounded
# everything + _per_thousand


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("internal_displacement")
    ds_pop = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("internal_displacement")

    tb = tb.rename(columns={"country_name": "country"}, errors="raise")

    tb = tb.drop(columns=["iso3"], errors="raise")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    tb = geo.add_population_to_table(tb, ds_pop)

    # add total displacements (conflict + disaster), wait for reply before publishing
    tb["total_displacement"] = tb["conflict_total_displacement"] + tb["disaster_total_displacement"]
    tb["total_displacement_rounded"] = tb["total_displacement"].apply(round_idmc_style)
    tb["total_new_displacement"] = tb["conflict_new_displacement"] + tb["disaster_new_displacement"]
    tb["total_new_displacement_rounded"] = tb["total_new_displacement"].apply(round_idmc_style)

    columns_to_calculate = [col for col in tb.columns if col not in ["country", "year", "population"]]

    tb = calculate_shares(tb, columns_to_calculate)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_shares(tb, columns_to_calculate):
    for displacement_col in columns_to_calculate:
        share_col_name = f"{displacement_col}_per_thousand"
        tb[share_col_name] = (tb[displacement_col] / tb["population"]) * 1000

    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def round_idmc_style(x):
    """Round numbers according to IDMC style.

    - Numbers <= 100,000 are rounded to 2 significant digits.
    - Numbers > 100,000 are rounded to the nearest 1,000.
    """
    if x <= 100000:
        return round(x, -len(str(int(x))) + 2)
    else:
        return round(x, -3)
