"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("summary_reporting_system_estimates")

    # Read table from meadow dataset.
    tb = ds_meadow.read("summary_reporting_system_estimates")
    tb = tb.drop(columns=["state_abbr", "rape_legacy", "rape_revised", "caveats"])
    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Calculate crime rates per 100,000 population.
    tb = calculate_crime_rates(tb)
    tb = tb.drop(columns=["population"])
    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_crime_rates(tb):
    crime_columns = [
        "violent_crime",
        "homicide",
        "robbery",
        "aggravated_assault",
        "property_crime",
        "burglary",
        "larceny",
        "motor_vehicle_theft",
    ]
    tb["population"] = tb["population"].replace(",", "", regex=True).astype(float)
    tb[crime_columns] = tb[crime_columns].replace(",", "", regex=True).astype(float)
    for column in crime_columns:
        rate_column = f"{column}_per_100k"
        tb[rate_column] = (tb[column] / tb["population"]) * 100000
    return tb
