"""Load a garden dataset and create a grapher dataset."""
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLS_WITH_DATA = [
    "manufactured_cigarettes",
    "manufactured_cigarettes_per_adult_per_day",
    "handrolled_cigarettes",
    "handrolled_cigarettes_per_adult_per_day",
    "total_cigarettes",
    "total_cigarettes_per_adult_per_day",
    "all_tobacco_products_tonnes",
    "all_tobacco_products_grams_per_adult_per_day",
]

MAN_KEY = "manufactured_cigarettes_per_adult_per_day"
HAND_KEY = "handrolled_cigarettes_per_adult_per_day"
TOTAL_KEY = "total_cigarettes_per_adult_per_day"
ALL_KEY = "all_tobacco_products_grams_per_adult_per_day"


def include_split_germany(tb, ds_population):
    """Include data for Germany 1945-1990 in the table by taking weighted average of East and West Germany data"""
    germany_tb = tb[tb["country"].isin(["West Germany", "East Germany"])]
    germany_tb = geo.add_population_to_table(germany_tb, ds_population, interpolate_missing_population=True)

    # calculate share of population for each year
    added_pop = germany_tb[["year", "population"]].groupby("year").sum().reset_index()

    for idx, row in germany_tb.iterrows():
        germany_tb.loc[idx, "share_of_population"] = (
            row["population"] / added_pop[added_pop["year"] == row["year"]]["population"].values[0]
        )
    # calculate share of cigarettes per adult for weighted average
    germany_tb[MAN_KEY] = germany_tb[MAN_KEY] * germany_tb["share_of_population"]
    germany_tb[HAND_KEY] = germany_tb[HAND_KEY] * germany_tb["share_of_population"]
    germany_tb[TOTAL_KEY] = germany_tb[TOTAL_KEY] * germany_tb["share_of_population"]
    germany_tb[ALL_KEY] = germany_tb[ALL_KEY] * germany_tb["share_of_population"]

    # sum up values for weighted average
    germany_tb = germany_tb[COLS_WITH_DATA + ["year"]].groupby("year").sum().reset_index()
    germany_tb["country"] = "Germany"

    return germany_tb


def cast_to_float(df, col):
    df[col] = df[col].astype("Float64")
    return df


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.

    ds_garden = paths.load_dataset("cigarette_sales")

    # Read table from garden dataset.
    tb = ds_garden["cigarette_sales"].reset_index()

    # Process data.

    # load population data
    ds_population = paths.load_dataset("population")

    # Calculate weighted average for Germany 1950-1990
    germany_tb = include_split_germany(tb, ds_population)

    # include for Germany 1950-1990
    tb = pr.concat([tb, germany_tb])
    # drop East and West Germany
    tb = tb[(tb["country"] != "East Germany") & (tb["country"] != "West Germany")]

    # cast columns to correct data types
    for col in COLS_WITH_DATA:
        tb = cast_to_float(tb, col)

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.

    tb = tb.format(["country", "year"])

    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
